import logging
from protein_metamorphisms_is.helpers.config.yaml import read_yaml_config
import protein_metamorphisms_is.sql.model.model  # noqa: F401
from protein_metamorphisms_is.helpers.services.services import check_services
from protein_metamorphisms_is.sql.base.database_manager import DatabaseManager


def main(config_path='config/config.yaml'):
    conf = read_yaml_config(config_path)

    # ✅ Mover esto al principio
    logger = logging.getLogger("metamorphic_multifunction_search")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Step 1: Import ORM-based logic & check model coherence
    from metamorphic_multifunction_search.model import (
        SequenceClustering,
        StructuralSubClustering,
        StructuralAlignmentManager,
        GoMultifunctionalityMetrics
    )

    # Step 2: Check services running
    check_services(conf, logger)


    # Step 3: Run components
    SequenceClustering(conf).start()
    StructuralSubClustering(conf).start()
    StructuralAlignmentManager(conf).start()
    GoMultifunctionalityMetrics(conf).start()

if __name__ == '__main__':
    main()
