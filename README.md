This repository contains a subset of scripts demonstrating typical components of an automated ETL pipeline for batch-processing MongoDB data and loading it into SQL Server.

The `main.py` workflow provides an overview of the pipeline’s structure and execution flow.

`build_sql.py` demonstrates how the SQL schema is aligned with batch processing by determining the appropriate data type for each column after evaluating all batches.

Another key aspect of the pipeline is data quality validation, demonstrated in `data_validation.py`, which ensures data is accurately transferred from MongoDB to SQL Server.