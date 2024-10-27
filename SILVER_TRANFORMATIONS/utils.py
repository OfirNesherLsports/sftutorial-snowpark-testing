from typing import Tuple
from snowflake.snowpark import Session, DataFrame
import snowflake.snowpark.functions as F
from snowflake.snowpark.types import MapType, ArrayType, StructType
import traceback

# Constants
CONFIG_TABLE = "UTILS.CONFIG.DEV_CONFIG_TABLES"
TIMESTAMP_FIELD = "current_timestamp"

class ETLError(Exception):
    """Base class for ETL-specific exceptions"""
    pass

class ConfigError(ETLError):
    """Raised when there's an issue with configuration"""
    pass

class DataError(ETLError):
    """Raised when there's an issue with data processing"""
    pass

def find_last_update_date(session: Session, table_name: str) -> str:
    """
    Find the last update date for a given table.
    Raises ConfigError if the configuration cannot be found or accessed.
    """
    try:
        result = (
            session.table(CONFIG_TABLE)
            .filter(F.col("table_name") == table_name)
            .select("silver_last_update")
            .collect()
        )
        
        if not result:
            raise ConfigError(f"No configuration found for table {table_name}")
            
        return result[0][0]
        
    except Exception as e:
        raise ConfigError(
            f"Failed to retrieve last update date for {table_name}: {str(e)}"
        ) from e

def find_delta(session: Session, table_name: str, last_update: str) -> Tuple[DataFrame, int]:
    """
    Find new records since last update.
    Raises DataError if unable to query the table or process results.
    """
    try:
        df = session.table(f"{table_name}").filter(F.col(TIMESTAMP_FIELD) > last_update)
        delta_count = df.count()
        return df, delta_count
        
    except Exception as e:
        raise DataError(
            f"Failed to find delta records for {table_name}: {str(e)}"
        ) from e

def find_latest_timestamp(df: DataFrame) -> str:
    """
    Find the latest timestamp in the DataFrame.
    Raises DataError if unable to process the DataFrame.
    """
    try:
        latest = df.agg({TIMESTAMP_FIELD: "max"}).collect()[0][0]
        return latest
        
    except Exception as e:
        raise DataError(
            f"Failed to find latest timestamp: {str(e)}"
        ) from e

def update_config(session: Session, table_name: str, latest_timestamp: str) -> None:
    """
    Update the configuration table with the latest processed timestamp.
    Raises ConfigError if the update fails.
    """
    try:
        session.sql(f"""
            UPDATE {CONFIG_TABLE}
            SET silver_last_update = '{latest_timestamp}'
            WHERE table_name = '{table_name}'
        """).collect()
        
    except Exception as e:
        raise ConfigError(
            f"Failed to update configuration for {table_name}: {str(e)}"
        ) from e

def remove_null_rows(delta_df: DataFrame) -> Tuple[DataFrame, int]:
    """
    Remove rows where all columns are null.
    Returns cleaned DataFrame and count of removed rows.
    Raises DataError if the operation fails.
    """
    try:
        initial_count = delta_df.count()
        cleaned_df = delta_df.dropna(how="all")
        null_count = initial_count - cleaned_df.count()
        return cleaned_df, null_count
        
    except Exception as e:
        raise DataError(
            f"Failed to remove null rows: {str(e)}"
        ) from e

def is_simple_type(data_type) -> bool:
    """Check if a data type is a simple type (not a complex type)."""
    return not isinstance(data_type, (MapType, ArrayType, StructType))
