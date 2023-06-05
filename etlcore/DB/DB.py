import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

class DB():
    def __init__(self, con_str, org):
        self.con_str = con_str
        self.org = org
        self.engine = self._create_db_connection()

    def _create_db_connection(self) -> Engine:
        try:
            engine = create_engine(self.con_str, fast_executemany=True, execution_options={"isolation_level": "AUTOCOMMIT"})
            con = engine.connect()
            print("successfully connected to the database")
            return con
        except Exception as e:
            print(f"unable to connect to db {str(e)}")

    def change_db_connection(self, con_str: str):
        self.engine = create_engine(con_str, fast_executemany=True, execution_options={"isolation_level": "AUTOCOMMIT"})
        return True

    def load_to_staging(self, df: pd.DataFrame, schema: str, table_name: str) -> bool:
        try:
            df.to_sql(f"RAW_{table_name}", self.engine, schema, if_exists = "replace", index=False)
            return True
        except Exception as e:
            print(f"insert failed {str(e)}")        

    def kill_fill(self, stage_db: str, dest_schema: str, table_name: str) -> bool:
        try:
            with self.engine.connect() as con:
                con.execute(f"EXECUTE dbo.spETL_KillFillDSQL @table = '{table_name}', @org = '{self.org}', @stage_db = '{stage_db}', @dest_schema='{dest_schema}'")
            return True
        except Exception as e:
            print(f"kill/fill of table failed: {str(e)}")

    def run_proc(self, db: str, schema: str, stored_procedure: str) -> bool:
        try:
            with self.engine.connect() as con:
                result = con.execute(f"EXECUTE {db}.{schema}.{stored_procedure}").fetchall()
                for q in result[0]:
                    if q == 1:
                        print(f"Query{list(result[0]).index(q) + 1} has failed")
                        return False
            return True
        except Exception as e:
            print(f"stored procedure run {stored_procedure} failed! error string: {str(e)}")

    def run_proc_with_param(self, db: str, schema: str, stored_procedure: str, param: str) -> pd.DataFrame:
        try:
            with self.engine.connect() as con:
                result = con.execute(f"EXECUTE {db}.{schema}.{stored_procedure} {param}").fetchall()
                for q in result[0]:
                    if q == 1:
                        print(f"Query{list(result[0]).index(q) + 1} has failed")
                        return False
            return True
        except Exception as e:
            print(f"stored procedure run {stored_procedure} failed! error string: {str(e)}")
    
    def upsert(self):
        raise NotImplementedError
