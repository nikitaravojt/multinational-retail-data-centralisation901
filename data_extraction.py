
import pandas as pd
import database_utils as utils

class DataExtractor():
    
    def read_rds_table(self, db_connector, target_table):
        engine = db_connector.init_db_engine("db_credentials.yaml")
        conn = engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        try:
            table_dataframe = pd.read_sql_table(target_table, con=engine)
            # print(table_dataframe.shape)
            # print(table_dataframe.head(10))
        except Exception as e:
            print(f"Error: {e}")
        conn.close()

        return table_dataframe
    
db_connector1 = utils.DatabaseConnector()
extractor1 = DataExtractor()
df = extractor1.read_rds_table(db_connector1, "legacy_users")


# issues:
# takes forever to run. if it does complete, nothing is returned despite last line being: df.head(10)
# why