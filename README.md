# ETL for Business Intelligence
### *A practical guide to data provisioning, dimensional modelling, and pipeline design*

> © Patrick Dolinger, NSCC Institute of Technology
> Licensed under [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/)
> You are free to share and adapt this material for any purpose, provided appropriate credit is given.

---

## About This Book

This book is a practical, open-access textbook for students and practitioners learning to design and build ETL (Extract, Transform, Load) pipelines for business intelligence. It covers the full pipeline — from understanding why ETL exists, through dimensional modelling and SSIS implementation, to documentation, governance, and production deployment.

The book uses **Microsoft SQL Server** and **SQL Server Integration Services (SSIS)** as its implementation platform, and a fictional outdoor gear retailer called **CabotTrail Outdoor** as its consistent worked example throughout. Every concept is illustrated with real SQL and real data that readers can run in their own environments.

While SSIS is the vehicle, the concepts — gap analysis, source-to-target mapping, surrogate key management, slowly changing dimensions, data quality frameworks, and ETL administration — apply equally to any ETL platform.

---

## Who This Book Is For

This book is written for students in data management, business intelligence, and database programs who have:

- Working knowledge of SQL (SELECT, JOIN, GROUP BY, aggregation, subqueries)
- Basic familiarity with relational database design (normalization, primary and foreign keys, constraints)
- Access to SQL Server and SQL Server Management Studio (SSMS)

It is designed to be read alongside a lab environment. The companion lab book for NSCC DBAS 2103 uses this textbook as its reference. However, the book is structured to stand alone as a reference for any reader working through ETL and dimensional modelling concepts.

---

## The CabotTrail Outdoor Environment

All worked examples use the CabotTrail Outdoor database environment:

| Database | Type | Description |
|---|---|---|
| `CabotTrailOutdoor` | OLTP | Normalized operational source — Sales, Purchasing, Inventory |
| `CabotTrailOutdoorDW` | Data Warehouse | Dimensional model — 10 dimensions, 6 facts |
| `CabotTrailOutdoorsSales` | Data Mart | Sales star schema |
| `CabotTrailOutdoorsReturns` | Data Mart | Returns star schema |
| `CabotTrailOutdoorsPurchasing` | Data Mart | Purchasing star schema |
| `CabotTrailOutdoorsInventory` | Data Mart | Inventory snapshot schema |
| `CabotTrailOutdoorsTransactions` | Data Mart | Financial transactions — two fact tables |

---

## How to Use This Book

Each chapter follows a consistent structure:

| Section | Purpose |
|---|---|
| **Chapter Overview** | Learning outcomes and what to expect |
| **Concept sections** | Core content — concept-driven, with CabotTrail worked examples |
| **Chapter Summary** | Key takeaways in condensed form |
| **Review Questions** | Consolidation and self-assessment |
| **🔍 Deeper Dive** | Extended concepts, industry perspectives, and a full reference list |

The main chapter content is designed for all readers. The Deeper Dive section is for those who want to understand the *why* behind the *what* — the theoretical foundations, the industry debates, the advanced techniques, and the practitioner literature.

---

## Chapters

| # | Chapter | Topics | Course weeks |
|---|---|---|---|
| 1 | [Foundations: What ETL Is and Why It Exists](./chapter-01-foundations/README.md) | OLTP vs OLAP, BI pipeline architecture, the data warehouse, ETL stages, SSIS overview, CabotTrail environment | Weeks 1–2 |
| 2 | [Dimensional Modelling: Stars, Schemas, and the Language of Analytics](./chapter-02-dimensional-modelling/README.md) | Fact tables, dimensions, grain, measure types, star vs snowflake, SCD types, date dimension, CabotTrail model | Weeks 2–3 |
| 3 | [Gap Analysis and Source-to-Target Mapping](./chapter-03-gap-analysis-s2t-mapping/README.md) | Gap types, source data analysis, data quality assessment, S2T mapping structure, DimCustomer and FactSales worked examples | Week 2 |
| 4 | [ETL Implementation: Dimensions, Lookups, and Derivations](./chapter-04-etl-implementation/README.md) | Control Flow vs Data Flow, OLE DB Source, Lookup transformation, Derived Column, Conditional Split, OLE DB Destination, dimension and fact packages, reconciliation, master package | Week 3 |
| 5 | [Testing, Data Quality, and Governance](./chapter-05-testing-quality-governance/README.md) | ETL testing levels, formal test suite, reconciliation testing, data quality lifecycle, SSIS logging, data governance pillars, metadata management | Week 4 |
| 6 | [ETL Documentation: Data Dictionaries and Process Flows](./chapter-06-etl-documentation/README.md) | ETL design document structure, data dictionary, process flow diagrams, S2T mapping completeness, PMI standards, system catalog queries, extended properties, docs-as-code | Week 5 |
| 7 | [Advanced ETL: MERGE, Slowly Changing Dimensions, and Multiple Sources](./chapter-07-advanced-etl/README.md) | MERGE patterns, SCD Types 1/2/3/6 implementation, watermark extraction, multi-source integration, SSIS advanced patterns, late arriving facts and dimensions | Week 6 |
| 8 | [ETL Administration: Scheduling, Hierarchies, Aggregates, and Migrations](./chapter-08-etl-administration/README.md) | SQL Server Agent, monitoring and alerting, hierarchies, aggregate tables, SSIS Catalog environments, DEV-Test-QA-Prod promotion, performance tuning, versioning and rollback | Week 7 |
| 9 | [Putting It Together: The Complete ETL Pipeline](./chapter-09-complete-pipeline/README.md) | End-to-end data journey, complete pipeline architecture, design decisions, full package inventory, end-to-end reconciliation, modern data stack mapping, practitioner principles, career paths | Weeks 8–9 |

---

## Licence

This work is licensed under the [Creative Commons Attribution 4.0 International Licence (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).

You are free to:
- **Share** — copy and redistribute the material in any medium or format
- **Adapt** — remix, transform, and build upon the material for any purpose, even commercially

Under the following terms:
- **Attribution** — You must give appropriate credit, provide a link to the licence, and indicate if changes were made.

**Suggested citation:**
> Dolinger, P. (2027). *ETL for Business Intelligence: A practical guide to data provisioning, dimensional modelling, and pipeline design*. NSCC Institute of Technology. CC BY 4.0. https://github.com/[repo-url]

---

## About the Author

**Patrick Dolinger** teaches Business Intelligence and Data Science courses at NSCC Institute of Technology as part of the one-year graduate certificate programs in Data Analytics and Business Intelligence.

Contact: Patrick.Dolinger@nscc.ca

---

*NSCC Institute of Technology | Halifax, Nova Scotia, Canada*
