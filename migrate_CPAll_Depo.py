from sqlalchemy import create_engine, MetaData, Table, select, text, insert # type: ignore
from sqlalchemy.orm import sessionmaker # type: ignore
import yaml # type: ignore

# Load configuration from YAML
def load_config():
    with open('config.yaml', 'r') as file:
        return yaml.safe_load(file)

# Get SQLAlchemy engine
def get_engine(db_config):
    return create_engine(
        f"{db_config['dialect']}://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )

# Get SQLAlchemy session
def get_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()

# Load configurations
config = load_config()
source_engine = get_engine(config['source_db'])
target_engine = get_engine(config['target_db'])
source_session = get_session(source_engine)
target_session = get_session(target_engine)

# Define metadata
metadata = MetaData()

# Define tables
contract_table = Table('CPAll_Depo', metadata, autoload_with=source_engine)
cust_contracts_table = Table('offices', metadata, autoload_with=target_engine)

# Create SQL statements for merging and migrating data
def merge_and_migrate():
    # Define the query to merge data
    query = text("""
    SELECT `ID`, `CPCODE`, `DEPO_NAME`, `Gststate_Code`, `Status`, `BookDelStatus` FROM `CPAll_Depo`
    """)

    result = source_session.execute(query)

    # Iterate through the results and insert into the target table
    for row in result:
        mapped_data = {
           'code': row.CPCODE,
        'name': '.',  # Assuming the city name can be used as the office name
        'district': '.',
        'taluka': '.',
        'city': row.DEPO_NAME,
        'pincode': '.',
        'latitude': '.',
        'longitude': '.',
        'address': '.',
        'active': row.Status,
        'o_type': 'HUB',
        }

        target_session.execute(insert(cust_contracts_table).values(mapped_data))
    
    target_session.commit()
    print(" CPAll_Depo,offices Data migration completed successfully . ")

if __name__ == "__main__":
    merge_and_migrate()
