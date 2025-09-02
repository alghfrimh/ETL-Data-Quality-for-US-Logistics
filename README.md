# LogiSight: ETL & Data Quality for US Logistics

## Repository Outline

```
p2-ftds030-hck-m3-alghfrimh
│
├── /dags                                       # DAG scripts
│   └── P2M3_muhammad_al_ghifari_DAG.py         # ETL pipeline DAG
│
├── /data                                               # Extracted & processed datasets
│   ├── P2M3_muhammad_al_ghifari_data_raw.csv           # Raw data extracted from PostgreSQL database
│   └── P2M3_muhammad_al_ghifari_data_cleaned.csv       # Cleaned dataset for Elasticsearch
│
├── /images                                             # Visualizations & documentation
│   ├── 1 - introduction & objectives.png
│   ├── 2 - conclusion & business recommendation.png
│   ├── plot 01 - Horizontal.png
│   ├── plot 02 - Vertical.png
│   ├── plot 03 - Pie.png
│   ├── plot 04 - Table.png
│   ├── plot 05 - Area.png
│   └── plot 06 - Heatmap.png
│
├── /logs                           # Airflow DAG logs
├── /plugins                        # (empty) placeholder
├── /postgres_data                  # PostgreSQL container data
│
├── .env                                         # Database configuration
├── airflow_m3.yaml                              # Docker container configuration
├── P2M3_muhammad_al_ghifari_conceptual.txt      # Conceptual domain knowledge
├── P2M3_muhammad_al_ghifari_DAG_graph.png       # DAG workflow graphic
├── P2M3_muhammad_al_ghifari_ddl.txt             # SQL schema script
├── P2M3_muhammad_al_ghifari_GX.ipynb            # Great Expectations notebook
├── P2M3_muhammad_al_ghifari_GX_result.png       # GX validation result
├── README.md                                    # Documentation
```

## Problem Background

<div align="justify">

When people order something, they expect it to arrive **fast**, **on time**, and **without extra cost**. For US retailers and logistics partners, that’s a daily juggling act: keeping deliveries quick while still making money.

In practice, Operation and Logistics team often run into simple but painful issues:
- Deliveries arrive **late** on some routes or with certain carriers.
- It’s hard to tell **which carrier is best** for a specific lane.
- **Delivery costs keep creeping up** (fuel, surcharges, last-mile).
- Too many **exceptions**, packages marked delayed, returned, or lost.
- Managers **can’t see the full picture** quickly enough to act.

**What this project aims to provide**
- A clean, reliable view of the basics: **On-time delivery rate**, **average days in transit**, **cost per mile**, and **exception rates**.
- Easy comparisons by **carrier**, **route (origin → destination)**, **warehouse**, and **week**.
- Clear insights to help teams **allocate volume**, **renegotiate rates**, improve **route planning**, and set **realistic customer promises**.

## Project Output

<div align="justify">

* DAG Script for the ETL process
* Visualization & analysis from ETL results
* Data validation using Great Expectations

## Data

<div align="justify">

This project uses a 2,000-shipment sample from US domestic logistics. It includes realistic quirks (missing values and a few outliers), so it’s perfect for showing how raw shipments are cleaned, validated, and turned into trustworthy insights.
* Dataset URL : [Kaggle - US Logistics Performance](https://www.kaggle.com/datasets/shahriarkabir/us-logistics-performance-dataset)
* Size : 2000 Rows, 11 Columns

## Method

<div align="justify">

- Data Extraction using **PostgreSQL Database**
- ETL Pipeline with **Apache Airflow**  
- Data Validation using **Great Expectations**
- Data Loading using **ElasticSearch**
- Visualization using **Kibana**

## Stacks

<div align="justify">

### Languages
- Python  
- Pandas  
- SQL  
- YAML  

### Tools
- Visual Studio Code  
- Docker  
- PostgreSQL  
- Airflow Webserver  
- Elasticsearch  
- Kibana  

## Reference

- [**Apache Airflow – Tutorial DAG & Scheduling**](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/index.html)
- [**Great Expectations Documents**](https://greatexpectations.io/expectations/)
- [**On-Time Delivery (OTD) – definition & KPI**](https://www.shipbob.com/blog/on-time-delivery/)

---
