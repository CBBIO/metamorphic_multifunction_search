import importlib
from collections import defaultdict
from datetime import datetime
from itertools import combinations

from protein_information_system.sql.model.entities.embedding.structure_3di import Structure3Di
from protein_information_system.sql.model.entities.structure.state import State
from protein_information_system.sql.model.operational.clustering.cluster import SubclusterEntry, Subcluster
from protein_information_system.sql.model.operational.structural_alignment.group import AlignmentGroupEntry, \
    AlignmentGroup
from protein_information_system.sql.model.operational.structural_alignment.result import AlignmentResult
from protein_information_system.sql.model.operational.structural_alignment.structural_alignment_type import \
    StructuralAlignmentType
from protein_information_system.tasks.queue import QueueTaskInitializer
from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import aliased


class StructuralAlignmentManager(QueueTaskInitializer):
    """
    Manages the structural alignment process of representational subclusters using various alignment algorithms.
    """

    def __init__(self, conf):
        super().__init__(conf)
        self.logger.info("Structural Alignment Manager initialized.")
        self.fetch_tasks_info()

    def fetch_tasks_info(self):
        """
        Load alignment task modules dynamically based on configuration.
        """
        structural_alignment_types = self.session.query(StructuralAlignmentType).all()
        base_module_path = 'metamorphic_multifunction_search.operation.structural_alignment.tasks'

        self.types = {
            type_obj.id: importlib.import_module(f"{base_module_path}.{type_obj.task_name}")
            for type_obj in structural_alignment_types
            if type_obj.id in self.conf['structural_alignment']['types']
        }

    def enqueue(self):
        cluster_ids = self._get_clusters_with_pending_alignments()
        entries = self._get_entries_for_clusters(cluster_ids)

        clusters_dict = {}
        for e in entries:
            clusters_dict.setdefault(e.cluster_id, []).append({
                'subcluster_entry_id': e.subcluster_entry_id,
                'file_path': e.file_path,
                'subcluster_id': e.subcluster_id
            })

        if not any(len(v) >= 2 for v in clusters_dict.values()):
            self.logger.warning("No clusters with pending alignments found. Nothing enqueued.")

        for cluster_id, subcluster_list in clusters_dict.items():
            if len(subcluster_list) < 2:
                continue

            task_data = {
                'cluster_id': cluster_id,
                'subclusters': subcluster_list
            }

            self.publish_task(task_data)
            self.logger.info(
                f"Enqueued {len(subcluster_list)} subclusters for cluster {cluster_id}.")

    def _get_clusters_with_pending_alignments(self):
        """
        Return a list of cluster_ids that:
        - Have more than one representative subcluster entry.
        - Still have at least one missing alignment result between subcluster pairs.
        """
        # Paso 1: Obtener todos los subcluster_entries representativos agrupados por cluster_id
        rep_entries = self.session.query(
            SubclusterEntry.id.label("entry_id"),
            SubclusterEntry.subcluster_id,
            Subcluster.cluster_id
        ).join(Subcluster, Subcluster.id == SubclusterEntry.subcluster_id).filter(
            SubclusterEntry.is_representative.is_(True)
        ).all()

        cluster_to_entries = defaultdict(list)
        entry_to_cluster = {}
        for row in rep_entries:
            cluster_to_entries[row.cluster_id].append(row.entry_id)
            entry_to_cluster[row.entry_id] = row.cluster_id

        # Paso 2: Obtener pares ya alineados con resultado completo
        result_rows = self.session.query(
            AlignmentGroupEntry.alignment_group_id,
            AlignmentGroupEntry.subcluster_entry_id
        ).join(AlignmentResult, AlignmentResult.alignment_group_id == AlignmentGroupEntry.alignment_group_id).all()

        # Agrupar entries por grupo
        group_to_entries = defaultdict(set)
        for row in result_rows:
            group_to_entries[row.alignment_group_id].add(row.subcluster_entry_id)

        # Quedarse solo con grupos de alineamiento binarios válidos
        completed_pairs = set()
        for pair in group_to_entries.values():
            if len(pair) == 2:
                completed_pairs.add(frozenset(pair))

        # Paso 3: Para cada cluster, ver si le falta algún par
        clusters_with_pending = []
        for cluster_id, entries in cluster_to_entries.items():
            if len(entries) < 2:
                continue
            all_possible = [frozenset(p) for p in combinations(entries, 2)]
            if any(p not in completed_pairs for p in all_possible):
                clusters_with_pending.append(cluster_id)

        return clusters_with_pending

    def _get_entries_for_clusters(self, cluster_ids):
        """
        Dado un conjunto/lista de cluster_id, devuelve todos los subcluster entries representativos
        con su file_path, subcluster_id y cluster_id.
        """
        if not cluster_ids:
            return []

        return self.session.query(
            SubclusterEntry.id.label("subcluster_entry_id"),
            State.file_path.label("file_path"),
            Subcluster.id.label("subcluster_id"),
            Subcluster.cluster_id.label("cluster_id")
        ).join(
            Subcluster, Subcluster.id == SubclusterEntry.subcluster_id
        ).join(
            Structure3Di, Structure3Di.id == SubclusterEntry.structure_3di_id
        ).join(
            State, State.id == Structure3Di.state_id
        ).filter(
            SubclusterEntry.is_representative.is_(True),
            Subcluster.cluster_id.in_(cluster_ids)
        ).all()

    def process(self, data):
        try:
            cluster_id = data.get('cluster_id')
            subclusters = data.get('subclusters')

            if not cluster_id or not subclusters:
                raise ValueError("Missing 'cluster_id' or 'subclusters' in task data")

            self.logger.info(f"Processing cluster {cluster_id} with {len(subclusters)} entries.")

            results = []

            for s1, s2 in combinations(subclusters, 2):
                id1, id2 = s1['subcluster_entry_id'], s2['subcluster_entry_id']

                for type_id, module in self.types.items():
                    task_pair_data = {
                        'cluster_id': cluster_id,
                        'alignment_type_id': type_id,
                        'subcluster_entry_1_id': id1,
                        'subcluster_entry_2_id': id2,
                        'subcluster_1_file_path': s1['file_path'],
                        'subcluster_2_file_path': s2['file_path']
                    }

                    result = module.align_task(task_pair_data, self.conf, self.logger)
                    if result:
                        result['alignment_type_id'] = type_id
                        results.append(result)
                    else:
                        self.logger.warning(f"No result for pair ({id1}, {id2}) with type {type_id}")

            from collections import defaultdict

            pair_results = defaultdict(dict)

            for r in results:
                key = frozenset((r['subcluster_entry_1_id'], r['subcluster_entry_2_id']))
                pair_results[key]['subcluster_entry_1_id'] = r['subcluster_entry_1_id']
                pair_results[key]['subcluster_entry_2_id'] = r['subcluster_entry_2_id']
                pair_results[key]['cluster_id'] = r['cluster_id']

                if r['alignment_type_id'] == 1:
                    pair_results[key]['ce_rms'] = r.get('ce_rms')
                elif r['alignment_type_id'] == 2:
                    pair_results[key].update({
                        'tm_rms': r.get('tm_rms'),
                        'tm_seq_id': r.get('tm_seq_id'),
                        'tm_score_chain_1': r.get('tm_score_chain_1'),
                        'tm_score_chain_2': r.get('tm_score_chain_2')
                    })
                elif r['alignment_type_id'] == 3:
                    pair_results[key].update({
                        'fc_rms': r.get('fc_rms'),
                        'fc_identity': r.get('fc_identity'),
                        'fc_similarity': r.get('fc_similarity'),
                        'fc_score': r.get('fc_score'),
                        'fc_align_len': r.get('fc_align_len')
                    })

            merged_results = list(pair_results.values())

            if merged_results:
                self.store_entry({'results': merged_results})
            else:
                self.logger.warning(f"No alignments were stored for cluster {cluster_id}.")

        except Exception as e:
            self.logger.error(f"Error processing cluster {data.get('cluster_id')}: {e}")

    def store_entry(self, entry: dict) -> None:
        """Store the alignment results in the database."""
        try:
            inserted = 0
            for r in entry["results"]:
                sub1_id = r["subcluster_entry_1_id"]
                sub2_id = r["subcluster_entry_2_id"]
                cluster_id = r["cluster_id"]

                sub1 = self.session.query(SubclusterEntry).get(sub1_id)
                sub2 = self.session.query(SubclusterEntry).get(sub2_id)

                if not sub1 or not sub2:
                    self.logger.error(f"Could not find subcluster entries {sub1_id} or {sub2_id}. Skipping.")
                    continue

                # Buscar si ya existe el grupo de alineamiento con exactamente esos dos entries
                group = (
                    self.session.query(AlignmentGroup)
                    .join(AlignmentGroup.entries)
                    .filter(
                        AlignmentGroupEntry.subcluster_entry_id.in_([sub1.id, sub2.id])
                    )
                    .group_by(AlignmentGroup.id)
                    .having(func.count(AlignmentGroupEntry.id) == 2)
                    .first()
                )

                if not group:
                    group = AlignmentGroup()  # 🚫 cluster_id eliminado si no está en el modelo
                    self.session.add(group)
                    self.session.flush()

                    for s in [sub1, sub2]:
                        self.session.add(AlignmentGroupEntry(
                            alignment_group_id=group.id,
                            subcluster_entry_id=s.id
                        ))

                # Comprobación si ya hay resultado para este grupo
                existing_result = (
                    self.session.query(AlignmentResult)
                    .filter_by(alignment_group_id=group.id)
                    .first()
                )

                if existing_result:
                    self.logger.info(f"Alignment result already exists for group {group.id}. Skipping.")
                    continue

                # Insertar nuevo resultado
                result = AlignmentResult(
                    alignment_group_id=group.id,
                    ce_rms=r.get("ce_rms"),
                    tm_rms=r.get("tm_rms"),
                    tm_seq_id=r.get("tm_seq_id"),
                    tm_score_chain_1=r.get("tm_score_chain_1"),
                    tm_score_chain_2=r.get("tm_score_chain_2"),
                    fc_rms=r.get("fc_rms"),
                    fc_identity=r.get("fc_identity"),
                    fc_similarity=r.get("fc_similarity"),
                    fc_score=r.get("fc_score"),
                    fc_align_len=r.get("fc_align_len")
                )
                self.session.add(result)
                inserted += 1

            self.session.commit()
            self.logger.info(f"Stored {inserted} new alignment results.")

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error storing alignment results: {e}")



