
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
    
    
    def list_number_of_stores(self, endpoint, header_dict):
        import requests

        response = requests.get(endpoint, headers=header_dict)
        if response.status_code == 200:
            data = response.json()
            return data['number_stores']
        else:
            print(f"Error: {response.status_code} - {response.text}")


    def retrieve_stores_data(self, num_stores, endpoint, header_dict):
        import requests

        # store_df = pd.DataFrame()
        stores_list = []

        for i in range(0, num_stores+1):
            current_endpoint = endpoint + str(i) # store number
            response = requests.get(current_endpoint, headers=header_dict)
            if response.status_code == 200:
                store_data = response.json()
                # store_df = pd.concat([store_df, pd.DataFrame([store_data])], ignore_index=True)
                stores_list.append(store_data)
            else:
                print(f"Error: {response.status_code} - {response.text}")

        store_df = pd.DataFrame(stores_list)
        return store_df
    

    def extract_from_s3(self, uri):
        import boto3, io

        uri = uri[5:] # Remove the 's3://' prefix
        bucket_name, obj_key = uri.split('/', 1) # strip

        s3 = boto3.client('s3')
        file = s3.get_object(Bucket=bucket_name, Key=obj_key)
        file = file['Body'].read()
        bytes_io = io.BytesIO(file)
        df = pd.read_csv(bytes_io)

        return df



# ext1 = DataExtractor()
# test_df = ext1.extract_from_s3(uri='s3://data-handling-public/products.csv')
# print(type(test_df))


# num_of_stores_header = {'x-api-key': "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
# store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
# num_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"

# ext1 = DataExtractor()
# num_stores = ext1.list_number_of_stores(endpoint=num_of_stores_endpoint, header_dict=num_of_stores_header)

# import requests
# resp = requests.get(store_endpoint+'0', headers=num_of_stores_header).json()
# print(resp)
# print(type(resp))



