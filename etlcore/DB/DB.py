import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine

class DB():
    def __init__(self, con_str, org, auto_commit: bool = True):
        self.con_str = con_str
        self.org = org
        self.engine = self._create_db_connection(auto_commit)

    def _create_db_connection(self, auto_commit: bool = True) -> Engine:
        try:
            if auto_commit:
                engine = create_engine(self.con_str, fast_executemany=True, execution_options={"isolation_level": "AUTOCOMMIT"})
            else:
                engine = create_engine(self.con_str, fast_executemany=True)
            con = engine.connect()
            print("successfully connected to the database")
            return con
        except Exception as e:
            return f"unable to connect to db {str(e)}"

    def change_db_connection(self, con_str: str) -> bool:
        self.engine = create_engine(con_str, fast_executemany=True, execution_options={"isolation_level": "AUTOCOMMIT"})
        return True

    def load_to_staging(self, df: pd.DataFrame, schema: str, table_name: str, dtype_dict: dict, chunksize: int = 1000) -> bool:
        try:
            #chunksize = 2100 // len(df.columns) #calculating chunksize
            df.to_sql(name = f"RAW_{table_name}", 
                      dtype= dtype_dict, 
                      con = self.engine, 
                      schema = schema, 
                      if_exists = "replace", 
                      index=False, 
                      chunksize=chunksize)
            return True
        except Exception as e:
            return f"insert failed {str(e)}"

    #dynamic kill/fill of table
    def kill_fill(self, stage_db: str, dest_schema: str, table_name: str) -> bool:
        try:
            self.engine.execute(text(f"EXECUTE dbo.spETL_KillFillDSQL @table = '{table_name}', @org = '{self.org}', @stage_db = '{stage_db}', @dest_schema='{dest_schema}'"))
            return True
        except Exception as e:
            return f"kill/fill of table failed: {str(e)}"

    #this should be used for ETL flows that use @q logging as a result
    def run_proc(self, db: str, schema: str, stored_procedure: str) -> bool: 
        try:
            result = self.engine.execute(text(f"EXECUTE {db}.{schema}.{stored_procedure}")).fetchall()
            for q in result[0]:
                if q == 1:
                    return f"Query{list(result[0]).index(q) + 1} has failed"
                        #return False
            return True
        except Exception as e:
            return f"stored procedure run {stored_procedure} failed! error string: {str(e)}"
    #runs stored procedure and returns results
    def run_proc_with_results(self, db: str, schema: str, stored_procedure: str):
        try:
            result = self.engine.execute(text(f"EXECUTE {db}.{schema}.{stored_procedure}")).fetchall()
            return result
        except Exception as e:
            return f"stored procedure run {stored_procedure} failed! error string: {str(e)}"

    #runs stored procedure with parameter and returns results
    def run_proc_with_param(self, db: str, schema: str, stored_procedure: str, param: str):
        try:
            result = self.engine.execute(text(f"EXECUTE {db}.{schema}.{stored_procedure} '{param}'")).fetchall()
            return result #
        except Exception as e:
            return f"stored procedure run {stored_procedure} failed! error string: {str(e)}"
    def upsert(self):
        raise NotImplementedError