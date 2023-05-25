import pandas as pd
from pandas import DataFrame
from sqlalchemy.engine.base import Engine
from multiprocessing import Pool

class DataUtils():
    def __init__(self, engine, org):
        self.engine = engine
        self.org = org

    def preprocess_df(self, db: str, schema: str, table_name: str, raw_df: pd.DataFrame) -> pd.DataFrame:
        try:
            base_df = pd.read_sql(f"SELECT TOP 1 * FROM {db}.{schema}.{table_name}", con=self.engine.engine) # Retrieve an empty result set as template for table
            print(base_df)
            cleaned_df = raw_df.astype(base_df.dtypes.to_dict()) # https://stackoverflow.com/questions/48348176/convert-data-types-of-a-pandas-dataframe-to-match-another
            print(cleaned_df)
            return cleaned_df #cleaned_df
        except Exception as e:
            print(f"unable to convert df {str(e)}")

    def validate_df(self, sql_df, s3_df) -> bool:
        raise NotImplementedError

    def get_table_schema_from_db(self, db: str, schema: str, table_name: str) -> pd.DataFrame:
        raise NotImplementedError
        # Implement later when Nick Ts db mapping is complete
        # should be able to pull out data types and column names using only a table name as a reference
        # org is already available in the class object so no need to pass it in as a parameter
