# reality_data

End-to-end **data engineering and analytics pipeline** for the Czech real estate market.  
The project is built on the **Medallion Architecture** concept (Bronze → Silver → Gold → BI).  
Originally inspired by Databricks architecture, but implemented entirely from scratch in Python and PostgreSQL to minimize infrastructure costs.

---

## Architecture Overview

| Layer | Purpose | Technologies |
|-------|----------|--------------|
| **Bronze (ETL)** | Raw data ingestion from multiple real-estate listing sources via scraping | Python, requests, BeautifulSoup |
| **Silver** | Data cleaning, normalization, and deduplication | Pandas, SQLAlchemy, RapidFuzz |
| **Gold** | Business-ready tables and star schema for BI | PostgreSQL |
| **BI Layer** | Visualization and reporting | Apache Superset, Power BI |

---

## Core Features

- Modular design with clear separation between layers and scripts, enabling **fast horizontal scaling** to support many parallel scrapers.
- **Dirty data ingestion** from Czech real estate listing websites (e.g., Sreality, Bezrealitky).
- **Image hashing and similarity detection** for identifying duplicate listings across sources.
- **Address normalization using the OpenAI API**, transforming messy text data into standardized Czech administrative names.
- **Filtering and verification of valid Czech addresses**.
- **Fuzzy matching with RÚIAN**, the official Czech administrative registry, for correcting and validating geographic information.
- **Automated medallion pipeline orchestration** through CLI wrappers (`wrapper_silver.py`, `wrapper_gold.py`).
- **Star schema and flat table generation** for Power BI DirectQuery mode and Apache Superset.
- **Robust logging and sanity checks** at each stage of the ETL process.

---

## Output and Visualization

- **Flat fact table** for lightweight analytical queries and exports.
- **Star schema (fact + dimension tables)** for enterprise BI use cases.
- **Interactive dashboards** in Apache Superset and Power BI, built directly on top of the Gold layer.

---

## Tech Stack

- **Language:** Python 3.10  
- **Database:** PostgreSQL  
- **Libraries:** pandas, sqlalchemy, psycopg2-binary, numpy, rapidfuzz  
- **BI Tools:** Apache Superset, Power BI  
- **AI Integration:** OpenAI API (for address normalization)

---

## Deployment

The project runs in production as a scheduled ETL pipeline on a dedicated PostgreSQL instance.  
All scripts are modular and orchestrated via `main.py`, with optional Airflow-style scheduling.  
The modular structure allows easy scaling to additional scrapers or new data domains.

---

## Project Structure
reality_data/
├── ETL/                 # Raw data collectors (scrapers)
├── silver_layer/        # Cleaning, normalization, deduplication
├── gold/                # Aggregation and BI-ready schema
├── BI_reality_data/     # SQL scripts and BI definitions
├── logs/                # ETL logs (stderr/stdout)
├── config.sample.json   # Config template (DB connection)
├── main.py              # Pipeline orchestrator
└── README.md

---
## Visualization Preview

The following collage shows several Superset dashboards built on top of the Gold layer.  
They represent interactive analytics on Czech real-estate data, comparing sales vs rentals,  
average market times, and price distributions by region and apartment size.

![Superset Dashboard Collage](docs/screenshots/superset_collage.png
## Notes

This project was developed as both a **production-grade system** and a **data engineering portfolio project**.  
It represents the complete flow from unstructured scraped data to a verified and analytics-ready model.  
The modular, scalable structure allows for easy adaptation to other markets or data sources.

---

## License

MIT License © 2025 Vladimir