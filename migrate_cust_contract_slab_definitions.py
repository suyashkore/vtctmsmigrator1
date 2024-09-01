from sqlalchemy import create_engine, MetaData, Table, select, insert # type: ignore
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
old_table = Table('Multislabrate', metadata, autoload_with=source_engine)
new_table = Table('cust_contract_slab_definitions', metadata, autoload_with=target_engine)  # Replace with actual new table name
def migrate_data():
    # Query to get all data from the old table
    query = select(old_table)  # Updated: Pass the table directly to select()
    result = source_session.execute(query)
    
    # Fetch all rows from the result
    rows = result.fetchall()
    
    # Process each row from the old table
    for row in rows:
        # Convert tuple to a dictionary with column names as keys
        row_dict = dict(zip(result.keys(), row))
        
        contract_id = row_dict['ContractID']
        for slab_number in range(1, 9):
            slab_from = row_dict.get(f'Slab{slab_number}from')
            slab_to = row_dict.get(f'Slab{slab_number}to')
            rate_type = row_dict.get(f'Slab{slab_number}ratetype')
            pkg_type = row_dict.get(f'slab{slab_number}pkgtype')
            
            if slab_from is not None and slab_to is not None:
                # Prepare the data to insert into the new table
                mapped_data = {
                    'tenant_id': 1,  # Set appropriate tenant_id
                    'cust_contract_id': '1',
                    'ctr_num': contract_id,
                    'slab_distance_type': pkg_type or '',
                    'slab_contract_type': rate_type or '',
                    'slab_rate_type': rate_type or 'RATED',
                    'slab_number': slab_number,
                    'slab_lower_limit': slab_from,
                    'slab_upper_limit': slab_to,
                    # 'created_by': 1,  # Set appropriate created_by
                    # 'updated_by': 1,  # Set appropriate updated_by
                    # 'created_at': None,  # Set appropriate created_at
                    # 'updated_at': None   # Set appropriate updated_at
                }

                # Check if the record already exists
                existing_query = select(new_table).where(
                    new_table.c.tenant_id == mapped_data['tenant_id'],
                    new_table.c.cust_contract_id == mapped_data['cust_contract_id'],
                    new_table.c.ctr_num == mapped_data['ctr_num'],
                    new_table.c.slab_number == mapped_data['slab_number']
                )
                existing_result = target_session.execute(existing_query)
                
                if existing_result.fetchone() is None:
                    # Insert into the new table if it does not exist
                    target_session.execute(insert(new_table).values(mapped_data))
    
    target_session.commit()
    print("Data migration completed successfully.")


if __name__ == "__main__":
    migrate_data()
