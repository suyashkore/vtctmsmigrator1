from sqlalchemy import create_engine, MetaData, Table # type: ignore
from sqlalchemy.orm import sessionmaker # type: ignore
import yaml # type: ignore

def load_config():
    with open('config.yaml', 'r') as file:
        return yaml.safe_load(file)

def get_engine(db_config):
    return create_engine(
        f"{db_config['dialect']}://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )

def get_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()

config = load_config()
source_engine = get_engine(config['source_db'])
target_engine = get_engine(config['target_db'])
source_session = get_session(source_engine)
target_session = get_session(target_engine)

# Metadata object to hold table schema
metadata = MetaData()

def get_table(engine, table_name):
    return Table(table_name, metadata, autoload_with=engine)
