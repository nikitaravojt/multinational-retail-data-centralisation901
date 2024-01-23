import database_utils as utils
import data_extraction as ext
import data_cleaning as clean
import pandas as pd
import requests

# Connect to AWS RDS and pull legacy_users dataframe
db_connector1 = utils.DatabaseConnector()
extractor1 = ext.DataExtractor()
# df = extractor1.read_rds_table(db_connector1, "legacy_users")

# Clean data
cleaner = clean.DataCleaning()


def clean_upload_card_table():
    pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    card_df = extractor1.retrieve_pdf_data(pdf_url)
    card_df = cleaner.clean_card_data(card_df)
    card_df.info()
    # db_connector1.upload_to_db(card_df, "dim_card_details", "db_credentials.yaml")

def upload_clean_date_times():
    def extract_json(endpoint):
        response = requests.get(endpoint)
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            return df
        else:
            print(f"Error: {response.status_code} - {response.text}")

    events = extract_json("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")
    events_clean = cleaner.clean_date_events(events)
    events_clean.info()
    db_connector1.upload_to_db(events_clean, "dim_date_times", "db_credentials.yaml")

def upload_clean_products_table():
    product_df = extractor1.extract_from_s3(uri='s3://data-handling-public/products.csv')
    product_df = cleaner.clean_products_data(product_df)
    product_df.info()
    db_connector1.upload_to_db(product_df, "dim_products", "db_credentials.yaml")

def upload_clean_stores_table():
    header = {'x-api-key': "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
    num_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    num_stores = extractor1.list_number_of_stores(endpoint=num_of_stores_endpoint, header_dict=header)
    stores_df = extractor1.retrieve_stores_data(num_stores=num_stores, endpoint=store_endpoint, header_dict=header)
    stores_df = cleaner.clean_store_data(stores_df)
    stores_df.info()
    db_connector1.upload_to_db(stores_df, "dim_store_details", "db_credentials.yaml")

def upload_clean_users_table():
    users = extractor1.read_rds_table(db_connector1, "legacy_users")
    users_clean = cleaner.clean_user_data(users)
    users_clean.info()
    db_connector1.upload_to_db(users_clean, "dim_users", "db_credentials.yaml")

clean_upload_card_table()
# upload_clean_date_times()
# upload_clean_products_table()
# upload_clean_stores_table()
# upload_clean_users_table()




# Users
# clean_data = cleaner.clean_user_data(df)

# Products
# product_df = extractor1.extract_from_s3(uri='s3://data-handling-public/products.csv')
# product_df = cleaner.clean_products_data(product_df)
# product_df.info()

# Card Table
# pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
# card_df = extractor1.retrieve_pdf_data(pdf_url)
# card_df = cleaner.clean_card_data(card_df)
# card_df.info()
# print("sum:", card_df["card_number"].isna().sum())

# Upload to local db
# db_connector1.upload_to_db(card_df, "dim_card_details", "db_credentials.yaml")