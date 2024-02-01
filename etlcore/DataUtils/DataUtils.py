import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.mssql import (
    BIGINT,
    DATETIME2,
    SMALLDATETIME,
    UNIQUEIDENTIFIER,
)
from sqlalchemy.types import (
    CHAR,
    DECIMAL,
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    SmallInteger
)
bool_type={
    "Yes": True,
    "No": False,
    "YES": True,
    "NO": False,
    "1": True,
    "0": False,
    "TRUE": True,
    "FALSE": False,
    "True": True,
    "False": False,
    0: False,
    1: True
}

class DataUtils():
    def __init__(self, engine, org):
        self.engine = engine
        self.org = org

    def preprocess_df(self, db: str, schema: str, table_name: str, raw_df: pd.DataFrame, dtype_dict: dict) -> pd.DataFrame:
        try:
            sql_df = pd.read_sql(text(f"SELECT TOP 1 * FROM {db}.{schema}.{table_name}"), con=self.engine.engine) # Retrieve a 1 row result set as template for table
            raw_df = raw_df[sql_df.columns.to_list()] #drop columns from raw_df that are not in list of columns from sql_df
            cleaned_df = self.convert_dtypes(raw_df, dtype_dict) #converts all columns in raw_df to sqlalchemy dtypes
            return cleaned_df
        except Exception as e:
            return f"unable to convert df {str(e)}"

    def get_table_dtypes(self, db: str, db_schema: str, table_name: str) -> dict:
        try:
            df = pd.read_sql(text(f"exec {db}.dbo.spGet_TableSchema {db_schema}, {table_name}"), con=self.engine.engine)
            table_dtypes = {} #stores sqlalchemy dtypes with column names in dictionary
            for row in df.itertuples(index=False):
                match row.DATA_TYPE: 
                    case "int":
                        table_dtypes.update({row.COLUMN_NAME: Integer()})
                    case "bit":
                        table_dtypes.update({row.COLUMN_NAME: Boolean()})
                    case "bigint":
                        table_dtypes.update({row.COLUMN_NAME: BIGINT()})
                    case "date":
                        table_dtypes.update({row.COLUMN_NAME: Date()})
                    case "float":
                        table_dtypes.update({row.COLUMN_NAME: Float(int(row.NUMERIC_PRECISION))})
                    case "decimal":
                        table_dtypes.update({row.COLUMN_NAME: DECIMAL(precision=int(row.NUMERIC_PRECISION), scale=int(row.NUMERIC_SCALE))})
                    case "datetime":
                        table_dtypes.update({row.COLUMN_NAME: DateTime()})
                    case "datetime2":
                        table_dtypes.update({row.COLUMN_NAME: DATETIME2()})
                    case "smalldatetime":
                        table_dtypes.update({row.COLUMN_NAME: SMALLDATETIME()})
                    case "char":
                        table_dtypes.update({row.COLUMN_NAME: CHAR(int(row.CHARACTER_MAXIMUM_LENGTH))})
                    case "text":
                        table_dtypes.update({row.COLUMN_NAME: String()})
                    case "varchar":
                        if row.CHARACTER_MAXIMUM_LENGTH == 'MAX':
                            table_dtypes.update({row.COLUMN_NAME: String(None)})
                        else:
                            table_dtypes.update({row.COLUMN_NAME: String(int(row.CHARACTER_MAXIMUM_LENGTH))})
                    case "uniqueidentifier":
                        table_dtypes.update({row.COLUMN_NAME: UNIQUEIDENTIFIER()})
            del df
            return table_dtypes
        except Exception as e:
            return f"unable to get table column types {str(e)}"
    
    def convert_dtypes(self, df, dtypes):
        try:
            for c in dtypes:
                if type(dtypes[c]) in [
                    type(Integer()),
                    type(Float()),
                    type(BigInteger()),
                    type(DECIMAL()),
                    type(BIGINT()),
                    type(SmallInteger())
                ]:
                    df.loc[:, c] = pd.to_numeric(df.loc[:, c])
                elif type(dtypes[c]) in [
                    type(Date()),
                    type(DateTime()),
                    type(DATETIME2()),
                    type(SMALLDATETIME()),
                ]:
                    df.loc[:, c] = pd.to_datetime(df.loc[:, c], format='mixed')
                elif type(dtypes[c]) in [type(Boolean())]:
                    # Handle Yes/No/Null
                    df.loc[:, c] = df.loc[:, c].map(bool_type, na_action="ignore")
                    df.loc[:, c] = df.loc[:, c].astype("float64")
                elif type(dtypes[c]) in [type(UNIQUEIDENTIFIER())]:
                    # NOOP
                    pass
                elif type(dtypes[c] in [type(String())]):
                    col_length = max(df.loc[:, c].astype(str).apply(len))
                    if dtypes[c].length is None and col_length < 8000:
                        dtypes[c] = String(col_length)
                        print(
                            "Updated column {} from String() to String({})".format(c, col_length)
                        ) 
            return df          
        except Exception as e:
            return f"unable to convert column types {str(e)}"

    #at this point all column values should be the same, this only handles converting types
    def convert_df_types(self, sql_df, base_df) -> pd.DataFrame:
        #todo: this kind of works, the problem is getting a base_df that you can trust as a reference
        # additionally since this only converts pandas dtypes you will run into issues with null values in int64/float columns
        #for now this task is on the shelf, but we can revisit it later if we want
        try:
            base_copy = base_df.copy() #need copy due to reference
            for col in sql_df.columns: #https://stackoverflow.com/questions/48348176/convert-data-types-of-a-pandas-dataframe-to-match-another 
                base_copy[col]=base_copy[col].astype(sql_df[col].dtypes.name)
            return base_copy
        except Exception as e:
            return f"unable to convert df types: {str(e)}"