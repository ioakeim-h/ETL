import pyodbc
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from config import MONGO_CONFIG, SQL_CONNECTION_STRING


class MongoSQLConfig:

    def __init__(self, mongo_config: dict, sql_connection_string: str):
        self.mongo_uri = mongo_config["mongo_uri"]
        self.mongo_db = mongo_config["mongo_db"]
        self.sql_connection_string = sql_connection_string


class MongoSQLValidator(MongoSQLConfig):
    """
    Compare the number of records in MongoDB vs SQL across the same time period (by year and month).
    """

    def __init__(self, mongo_config: dict, sql_connection_string: str, field: str):
        super().__init__(mongo_config, sql_connection_string)
        self.date_field = field["date_field"]
        self.date_type = field["date_type"]

    def _get_mongo_df(self, collection_name: str) -> pd.DataFrame:
        client = MongoClient(self.mongo_uri)
        db = client[self.mongo_db]

        if self.date_type == "int": 
                pipeline_first_step = {
                    "$addFields": {
                        # Convert each record's date_field from UNIX timestamp to a datetime
                        "created_at": {
                            "$toDate": {"$multiply": [f"${self.date_field}", 1000]}
                        }
                    }
                }
        elif self.date_type == "ISODate":
            pipeline_first_step = {
                "$addFields": {
                    # Maintain date_field as is
                    "created_at": f"${self.date_field}"
                }                
            }
        else:
            raise TypeError(f"Unsupported date type: {self.date_type}")
        
        pipeline_second_step = {
            "$group": {
                "_id": {
                    # Group records by year and month 
                    "year": {
                        "$year": {
                            "date": "$created_at" 
                        }
                    },
                    "month": {
                        "$month": {
                            "date": "$created_at" 
                        }
                    },
                },
                # Count the total number of records in each year/month group
                "total": {"$sum": 1},
            }
        }

        pipeline = [pipeline_first_step, pipeline_second_step]
        results = list(db[collection_name].aggregate(pipeline))

        if not results:
            return pd.DataFrame(columns=["year", "month", "total"])
        return pd.DataFrame(
            [
                {
                    "year": r["_id"]["year"],
                    "month": r["_id"]["month"],
                    "total": r["total"],
                }
                for r in results
            ]
        )

    def _get_sql_df(self, table_name: str) -> pd.DataFrame:
        query = f"""
            SELECT
                YEAR({self.date_field}) AS [year],
                MONTH({self.date_field}) AS [month],
                COUNT(*) AS [total]
            FROM {table_name}
            GROUP BY YEAR({self.date_field}), MONTH({self.date_field})
        """
        with pyodbc.connect(self.sql_connection_string) as conn:
            df = pd.read_sql(query, conn)

        if df.empty:
            return pd.DataFrame(columns=["year", "month", "total"])
        return df
    
    def _get_duplicates(self, table_name: str) -> pd.DataFrame:
        query = f"""
        SELECT _id, COUNT(*) AS cnt
        FROM [DW].[dbo].[{table_name}] 
        GROUP BY _id
        HAVING COUNT(*) > 1;
        """
        with pyodbc.connect(self.sql_connection_string) as conn:
            df = pd.read_sql(query, conn)
            return df

    def compare_record_counts(self, mongo_collection: str, sql_table: str):
        mongo_df = self._get_mongo_df(mongo_collection)
        sql_df = self._get_sql_df(sql_table)

        mongo_df = mongo_df.sort_values(
            ["year", "month"], ascending=False
        # Skip the current month, since it is incomplete and counts won’t match exactly
        ).iloc[1:].reset_index(drop=True)

        sql_df = sql_df.sort_values(
            ["year", "month"], ascending=False
        ).iloc[1:].reset_index(drop=True)

        # In some cases, data may match exactly; in others, SQL may have more records.
        # This may happen when one database is configured to periodically delete outdated transactions.
        feedback = []
        for idx, mongo_row in mongo_df.iterrows():
            sql_row = sql_df.iloc[idx]

            if sql_row.empty:
                feedback.append(f"SQL missing data for {mongo_row['year']}-{mongo_row['month']}")
            elif sql_row["total"] < mongo_row["total"]:
                feedback.append(f"SQL has fewer records than Mongo for {mongo_row['year']}-{mongo_row['month']}")
            elif sql_row["total"] > mongo_row["total"]:
                feedback.append(f"SQL has more records than Mongo for {mongo_row['year']}-{mongo_row['month']}")
        return feedback

    def search_for_duplicates(self, sql_table: str):
        df = self._get_duplicates(sql_table)
        return not df.empty
    


def log_data_issue(log_file_path: str, message: str, extra_messages: list = None):
    print(message)
    with open(log_file_path, "a") as f:
        f.write(f"{datetime.now()} --> {message}\n")
        if extra_messages:
            for msg in extra_messages:
                f.write(f"{msg}\n")
        f.write("\n")

