
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
    

    def retrieve_pdf_data(self, url):
        import tabula
        dataframes = tabula.read_pdf(url, pages="all", stream=True)
        df = pd.concat(dataframes, ignore_index=True)

        return df
    
    
# pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

# ext1 = DataExtractor()
# card_df = ext1.retrieve_pdf_data(pdf_url)
# card_df.head(40)