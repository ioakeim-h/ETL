import pytz
from datetime import datetime

from collection_info import field_sets
from notify_on_fail import send_data_quality_alert
from build_sql import define_sql_schema, update_schemas, load_csv_to_sql
from data_validation import MongoSQLValidator, log_data_issue

from utils import (
    error_flag, empty_export_directory, 
    connect_to_sql, exists_in_sql,
    get_sql_cutoff_date, del_data_after_cutoff_date,
    filter_and_export, process_in_batches
)

from config import (
    SQL_CONNECTION_STRING, MONGO_CONFIG, 
    EXPORT_PATH, DTYPE_MAP,
    SQL_RANK,
    DQ_LOG_PATH
)

import warnings
warnings.filterwarnings("ignore")


def main():
    error_flag(phase = "running")
    timezone = pytz.UTC

    empty_export_directory(EXPORT_PATH)
    conn, cursor = connect_to_sql(SQL_CONNECTION_STRING)


    # Process Mongo data into SQL
    for field in field_sets:
        collection_name = field["collection"]

        if not exists_in_sql(f"{collection_name}_collection", cursor):
            filter_and_export(EXPORT_PATH, field, first_run=True)
            process_in_batches(EXPORT_PATH, f"{collection_name}_collection", timezone)
            define_sql_schema(EXPORT_PATH, f"{collection_name}_collection", DTYPE_MAP, SQL_RANK, cursor, conn)
        
        else:
            date_int, date_ISO = get_sql_cutoff_date()
            del_data_after_cutoff_date(cursor, collection_name, date_ISO, timezone)
            filter_and_export(EXPORT_PATH, field, retention_start_date_unix_timestamp=date_int, retention_start_date_ISO=date_ISO)
            process_in_batches(EXPORT_PATH, f"{collection_name}_collection", timezone)
            update_schemas(EXPORT_PATH, f"{collection_name}_collection", cursor, conn)

        load_csv_to_sql(EXPORT_PATH, f"{collection_name}_collection", cursor, conn)

    conn.close()


    # Test data quality
    good_quality = True
    
    for field in field_sets:
        collection_name = field["collection"]
        
        validator = MongoSQLValidator(MONGO_CONFIG, SQL_CONNECTION_STRING, field)
        
        if feedback := validator.compare_record_counts(collection_name, f"{collection_name}_collection"):
            good_quality = False
            log_data_issue(
                log_file_path = DQ_LOG_PATH,
                message = f"Count mismatch for {collection_name}",
                extra_messages = feedback
            )

        if found := validator.search_for_duplicates(f"{collection_name}_collection"):
            good_quality = False
            log_data_issue(
                log_file_path = DQ_LOG_PATH,
                message = f"Duplicates in {collection_name}",
                extra_messages = [f"Identified using '_id'"]
            )

    if not good_quality:
        send_data_quality_alert()
    
    error_flag(phase = "finished")   
    


if __name__ == "__main__":
    try:
        main()
        with open("error_log.txt", "a") as f:
            f.write(f"{datetime.now()} Successfully executed ETL pipeline.\n")
    except Exception as e:
        with open("error_log.txt", "a") as f:
            f.write(f"\n{datetime.now()} Failed to execute ETL pipeline.\nException encountered: {e}\n\n")
            print(f"Failed to execute ETL pipeline.\nException encountered: {e}")
