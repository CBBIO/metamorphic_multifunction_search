[![PyPI - Version](https://img.shields.io/pypi/v/metamorphic_multifunction_search)](https://pypi.org/project/metamorphic_multifunction_search/)
[![Documentation Status](https://readthedocs.org/metamorphic_multifunction_search/badge/?version=latest)](https://metamorphic_multifunction_search.readthedocs.io/en/latest/?badge=latest)
![Linting Status](https://github.com/CBBIO/metamorphic_multifunction_search/actions/workflows/test-lint.yml/badge.svg?branch=main)

# **Metamorphic & Multifunctional Protein Search**

## 🔬 Overview

**`metamorphic_multifunction_search`** is a systematic protocol for the **large-scale detection of structural metamorphisms and protein multifunctionality**, built on top of the Protein Information System (PIS).

The project combines structural alignments, functional GO annotations, and protein language models to uncover hidden relationships between structure and function across model and non-model organisms.

---

## 🧠 What Does This Protocol Do?

### 1. **Structural Metamorphism Detection**

* Aligns 3D protein structures with high sequence identity.
* Detects divergent conformations (i.e. metamorphisms) using metrics like RMSD or FC-score.
* Uses large-scale filtering (e.g., CD-HIT) and pairwise structural comparison.

### 2. **Functional Multifunctionality Analysis**

* Extracts Gene Ontology (GO) annotations per protein.
* Computes semantic distances between GO terms within each namespace (MF, BP, CC).
* Identifies the most divergent pair of terms per protein to quantify multifunctionality.

---

## ⚙️ Requirements

* Python 3.11.6
* RabbitMQ
* PostgreSQL with `pgvector` extension
* Docker (optional but recommended for deployment)

---

## 🚀 Quick Start

1. **Start PostgreSQL with `pgvector`:**

```bash
docker run -d --name pgvectorsql \
    -e POSTGRES_USER=user \
    -e POSTGRES_PASSWORD=password \
    -e POSTGRES_DB=BioData \
    -p 5432:5432 \
    pgvector/pgvector:pg16
```

2. **Start RabbitMQ:**

```bash
docker run -d --name rabbitmq \
    -p 15672:15672 \
    -p 5672:5672 \
    rabbitmq:management
```

3. **Run the main protocol:**

```bash
python main.py
```

This command executes the full pipeline: data extraction, structural filtering, alignment, functional analysis, and metric computation.

---

## ⚒️ Customization

You can tailor the pipeline by editing the `config.yaml` file or modifying `main.py` to:

* Switch embedding models
* Apply taxonomy-based filters
* Add new annotation types or similarity metrics

---

## 📚 Related Projects

* 🔗 [Protein Information System (PIS)](https://github.com/CBBIO/ProteinInformationSystem)
* 🔗 [FANTASIA: Functional Annotation Toolkit](https://github.com/CBBIO/FANTASIA)

---

Let me know if you'd like me to overwrite your current `README.md` with this version.
