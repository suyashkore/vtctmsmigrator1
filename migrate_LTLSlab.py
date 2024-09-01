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
contract_table = Table('LTLSlab', metadata, autoload_with=source_engine)
cust_contracts_table = Table('cust_contract_slab_rates', metadata, autoload_with=target_engine)

# Create SQL statements for merging and migrating data
def merge_and_migrate():
    try:
        # Define the query to fetch data from source table
        query = text("""
        SELECT `Id`, `ContractID`, `CustCode`, `CustName`, `FromPlace`, `ToPlace`, `TransitDay`, 
               `Slab1`, `Slab2`, `Slab3`, `Slab4`, `Slab5`, `Slab6`, `Slab7`, `Slab8`, `Zone` 
        FROM `LTLSlab`
        """)
        
        result = source_session.execute(query)
        rows = result.fetchall()  # Fetch all results at once

        print(f"Total records fetched: {len(rows)}")  # Debugging line to check the number of records

        # Iterate through the results and insert into the target table
        for row in rows:
            mapped_data = {
                'ctr_num': row.CustCode,
                'zone': row.Zone,
                'from_place': row.FromPlace,
                'to_place': row.ToPlace,
                'tat': row.TransitDay,
                'slab1': row.Slab1,
                'slab2': row.Slab2,
                'slab3': row.Slab3,
                'slab4': row.Slab4,
                'slab5': row.Slab5,
                'slab6': row.Slab6,
                'slab7': row.Slab7,
                'slab8': row.Slab8,
                'tenant_id': '1',
                'cust_contract_id': '3',
                'slab_distance_type': 'km',
                'slab_contract_type': 'PER_PKG',
            }

            target_session.execute(insert(cust_contracts_table).values(mapped_data))

        target_session.commit()
        print("LTLSlab Data migration completed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
        target_session.rollback()
    
    finally:
        source_session.close()
        target_session.close()

if __name__ == "__main__":
    merge_and_migrate()
