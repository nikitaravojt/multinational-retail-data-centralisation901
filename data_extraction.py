import pandas as pd
import database_utils as utils

class DataExtractor():
    """Class to extract data from a AWS RDS instance, S3 bucket or 
    PDF file. Provides methods to create HTTP request to pull data
    from an endpoint."""

    def read_rds_table(self, db_connector, target_table):
        """Takes a db_connector class object from database_utils
        and calls init_db_engine to initialise an sqlalchemy engine.
        Takes the db_credentials.yaml file. Connects to AWS RDS and
        reads SQL table, converting it to a pandas dataframe, unless
        an exception is raised. Return dataframe or raised exception."""

        engine = db_connector.init_db_engine("db_credentials.yaml")
        conn = engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        try:
            table_dataframe = pd.read_sql_table(target_table, con=engine)
        except Exception as e:
            print(f"Error: {e}")
        conn.close()

        return table_dataframe
    
    def retrieve_pdf_data(self, url):
        """Uses the tabula library to read and extract data from a 
        PDF file page-wise, given its web URL. Concatinates all
        data from the file and returns it as a pandas dataframe."""

        import tabula
        dataframes = tabula.read_pdf(url, pages="all")
        df = pd.concat(dataframes, ignore_index=True, axis=0)

        return df
    
    def list_number_of_stores(self, endpoint, header_dict):
        """Sends a HTTP GET request to a given endpoint using
        a given header_dict. If successful response code (200), 
        the 'number_of_stores' column is extracted from the data
        and returned, otherwise the failure code is returned."""

        import requests

        response = requests.get(endpoint, headers=header_dict)
        if response.status_code == 200:
            data = response.json()
            return data['number_stores']
        else:
            print(f"Error: {response.status_code} - {response.text}")

    def retrieve_stores_data(self, num_stores, endpoint, header_dict):
        """Iteratively extracts data from each store from a given
        endpoint using a given header_dict. Number of stores must
        be specified using num_stores. If successful response code
        (200) for a given store, its data is appended to stores_list,
        otherwise the failure response code is printed. Upon
        completion of the loop, stores_list is returned as a pandas
        dataframe."""
        
        import requests

        stores_list = []

        for i in range(0, num_stores+1):
            current_endpoint = endpoint + str(i) # store number
            response = requests.get(current_endpoint, headers=header_dict)
            if response.status_code == 200:
                store_data = response.json()
                stores_list.append(store_data)
            else:
                print(f"Error: {response.status_code} - {response.text}")

        store_df = pd.DataFrame(stores_list)

        return store_df
    
    def extract_from_s3(self, uri):
        """Takes an AWS S3 bucket URI, strips it into
        the bucket_name and obj_key and initialises 
        a boto3 S3 client to extract the data. Returns
        the CSV data as a pandas dataframe."""
        import boto3, io

        uri = uri[5:] # Removes the 's3://' prefix
        bucket_name, obj_key = uri.split('/', 1) # strip

        s3 = boto3.client('s3')
        file = s3.get_object(Bucket=bucket_name, Key=obj_key)
        file = file['Body'].read()
        bytes_io = io.BytesIO(file)
        df = pd.read_csv(bytes_io)

        return df
