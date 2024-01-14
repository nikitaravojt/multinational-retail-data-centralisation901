import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector():
    """Class specifying methods to perform database operations
    such as reading credentials from a .yaml file, creating a
    sqlalchemy engine, listing tables found in a target 
    database and uploading to a target database.
    """

    def read_db_creds(self, creds_filepath):
        """Safe loads a .yaml file, given a creds_filepath.
        Returns the data.
        """
        with open(creds_filepath, "r") as file:
            data = yaml.safe_load(file)

        return data

    def init_db_engine(self, creds_filepath):
        """Calls the read_db_creds() method, given a 
        creds_filepath. Initialises a sqlalchemy engine
        to establish a connection to the database using 
        the provided credentials. Returns engine object."""
        creds = self.read_db_creds(creds_filepath)
        engine_url = ( 
            f'postgresql://{creds["RDS_USER"]}:'
            f'{creds["RDS_PASSWORD"]}@{creds["RDS_HOST"]}:'
            f'{creds["RDS_PORT"]}/{creds["RDS_DATABASE"]}'
        )
        engine = create_engine(engine_url)

        return engine

    def list_db_tables(self, engine):
        """Returns a list of table names, given an engine
        object which is used to connect to the remote.
        Connection is closed after the table names are 
        extracted and returned."""
        conn = engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        inspector = inspect(conn)
        table_names = inspector.get_table_names()
        conn.close()

        return table_names

    def upload_to_db(self, df, table_name, creds_filepath):
        """Calls read_db_creds() method to extract credentials
        to the local database, creating an sqlalchemy engine
        with these credentials. Takes a pandas dataframe and 
        uploads it to this target database, replacing if
        already exists."""
        creds = self.read_db_creds(creds_filepath)
        engine_url = ( 
            f'postgresql://{creds["LOCAL_USER"]}:'
            f'{creds["LOCAL_PASSWORD"]}@{creds["LOCAL_HOST"]}:'
            f'{creds["LOCAL_PORT"]}/{creds["LOCAL_DATABASE"]}'
        )
        engine = create_engine(engine_url)

        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
