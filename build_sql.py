
import os
import glob
import pandas as pd
from collections import defaultdict


def define_sql_schema(export_path, collection_name, dtype_map, SQL_RANK, cursor, conn):
    print("Defining SQL table schema")
    
    # Accumulated SQL schema (starts narrow)
    sql_schema = defaultdict(lambda: "BIGINT")
    batch_files_ls = glob.glob(os.path.join(export_path, f"{collection_name}_batch*.csv"))
    
    print(f"  - Reading dtypes across batch files")
    for file in batch_files_ls:
        # Consider optimizing with polars pl.scan_csv
        df = pd.read_csv(file, sep=";")

        for col in df.columns:
            pandas_dtype = str(df[col].dtype)
            sql_dtype = dtype_map.get(pandas_dtype, "NVARCHAR(MAX)")

            if SQL_RANK[sql_dtype] > SQL_RANK[sql_schema[col]]:
                sql_schema[col] = sql_dtype
    
    print(f"  - Structuring SQL table")
    table_structure_elements = [f"[{col}] {dtype}" for col, dtype in sql_schema.items()]
    table_structure = ", ".join(table_structure_elements)
    cursor.execute(f"CREATE TABLE {collection_name} ({table_structure})")
    conn.commit()



def update_schemas(input_path, collection_name, cursor, conn):
    print("Matching data structures across CSV batch files and existing SQL table")
    batch_files_ls = glob.glob(os.path.join(input_path, f"{collection_name}_batch*.csv"))

    for file in batch_files_ls:

        # Get column names from SQL table: placed within loop since SQL schema can change with each iteration
        cursor.execute(f"SELECT TOP 0 * FROM {collection_name}")
        columns_in_sql = [column[0] for column in cursor.description]

        df = pd.read_csv(file, sep=";")
        columns_in_python = df.columns

        # If some columns exist in Python but are missing from SQL (or vice versa), create them with dtype string.
        # This is necessary for BULK INSERT 
        unmatched_columns = list(set(columns_in_sql) ^ set(columns_in_python))   
        if missing_sql_columns := set(unmatched_columns).intersection(columns_in_python):
            print(f"  - Adjusting for missing sql columns")
            column_structure = ", ".join([f"[{col}] NVARCHAR(MAX)" for col in missing_sql_columns])
            cursor.execute(f"ALTER TABLE {collection_name} ADD {column_structure}")
            conn.commit()

        if missing_python_columns := list(set(unmatched_columns).intersection(columns_in_sql)):
            print(f"  - Adjusting for missing python columns")
            # Add all missing columns at once using pd.concat to avoid fragmentation
            new_columns = pd.DataFrame({col: None for col in missing_python_columns}, index=df.index)
            df = pd.concat([df, new_columns], axis=1)

        # Ensure columns in SQL and Python are ordered in the same way before loading: sort based on SQL
        cursor.execute(f"SELECT TOP 0 * FROM {collection_name}")
        updated_sql_columns = [column[0] for column in cursor.description]
        ordered_python_columns = updated_sql_columns
        df[ordered_python_columns].to_csv(file, index=False, sep=";", lineterminator="\r\n") 



def load_csv_to_sql(input_path, collection_name, cursor, conn):
    print(f"Loading CSV files to SQL database")
    batch_files_ls = glob.glob(os.path.join(input_path, f"{collection_name}_batch*.csv"))

    for file in batch_files_ls:
        # Get absolute file path for SQL query
        file_path = os.path.abspath(file)
        
        query = f"""
        BULK INSERT {collection_name}
        FROM '{file_path}'
        WITH (
            FORMAT = 'CSV', 
            FIRSTROW = 2,
            FIELDTERMINATOR = ';',
            ROWTERMINATOR = '\r\n', -- '0x0a',
        );
        """
        cursor.execute(query)
        conn.commit()

