import os
import unittest

import pytest
from sqlalchemy.dialects.mssql.information_schema import sequences

from protein_information_system.helpers.config.yaml import read_yaml_config
from metamorphic_multifunction_search.operation.clustering.sequence_clustering import SequenceClustering
from metamorphic_multifunction_search.operation.clustering.structural_subclustering import StructuralSubClustering
from protein_information_system.operation.embedding.sequence_embedding import SequenceEmbeddingManager
from protein_information_system.operation.extraction.uniprot import UniProtExtractor
from metamorphic_multifunction_search.operation.structural_alignment.structural_alignment import StructuralAlignmentManager
from protein_information_system.sql.model.entities.embedding.sequence_embedding import SequenceEmbedding, \
    SequenceEmbeddingType
from protein_information_system.sql.model.entities.embedding.structure_3di import Structure3Di
from protein_information_system.sql.model.entities.go_annotation.go_annotation import ProteinGOTermAnnotation
from protein_information_system.sql.model.entities.go_annotation.go_term import GOTerm
from protein_information_system.sql.model.entities.protein.accesion import Accession
from protein_information_system.sql.model.entities.protein.protein import Protein
from protein_information_system.sql.model.entities.sequence.sequence import Sequence
from protein_information_system.sql.model.entities.structure.structure import Structure


@pytest.mark.order(9)
class TestStructuralAlignment(unittest.TestCase):

    def setUp(self):
        config_path = self.get_config_path()
        config = read_yaml_config(config_path)
        self.structural_aligner = StructuralAlignmentManager(config)

    def get_config_path(self):
        """Devuelve la ruta al archivo de configuración."""
        return os.path.join('tests/config/', "config.yaml")  # Ruta del archivo YAML de prueba

    def test_entities_created(self):
        """Verifica que se crean instancias en todas las entidades tras ejecutar el subclustering."""
        # Ejecutar el proceso de subclustering
        self.structural_aligner.start()



    if __name__ == '__main__':
        unittest.main()