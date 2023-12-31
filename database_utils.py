import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector():
    
    def read_db_creds(self, creds_filepath):
        with open(creds_filepath, "r") as file:
            data = yaml.safe_load(file)

        return data


    def init_db_engine(self, creds_filepath):
        creds = self.read_db_creds(creds_filepath)
        engine_url = ( 
            f'postgresql://{creds["RDS_USER"]}:'
            f'{creds["RDS_PASSWORD"]}@{creds["RDS_HOST"]}:'
            f'{creds["RDS_PORT"]}/{creds["RDS_DATABASE"]}'
        )
        engine = create_engine(engine_url)

        return engine


    def list_db_tables(self, engine):
        conn = engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        inspector = inspect(conn)
        table_names = inspector.get_table_names()
        conn.close()

        return table_names
    


# db1 = DatabaseConnector()
# engine1 = db1.init_db_engine("db_credentials.yaml")
# print(db1.list_db_tables(engine1))
