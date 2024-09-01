import datetime
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
contract_table = Table('HamaliVendor', metadata, autoload_with=source_engine)
service_selection_table = Table('DepoWiseHamali', metadata, autoload_with=source_engine)
cust_contracts_table = Table('loader_rates', metadata, autoload_with=target_engine)

# Create SQL statements for merging and migrating data
def merge_and_migrate():
    # Define the query to merge data
    query = text("""
    SELECT d.SRNO, d.DepotName, d.HamaliVendorName, d.Regular, d.Crossing, d.Regularbag, 
                 d.Crossingbag, h.id, h.VendorCode, h.Hvendor, h.DEPOT, h.HAccountNO, 
                 h.HIFSC, h.Hbank, h.Hbranch, h.Active, h.Category, h.CreateDate, 
                 h.U_Location FROM DepoWiseHamali d JOIN HamaliVendor h ON d.HamaliVendorName = 
                 h.Hvendor;
    """)

    result = source_session.execute(query)

    # Iterate through the results and insert into the target table
    for row in result:
        mapped_data = {
          'tenant_id':'1',
          'contracting_office_id':'1',
          'vendor_id' :'1',
          'vendor_name': row.Hvendor,
          'default_rate_type': 'ALL',
          'vendor_name': row.Hvendor,
          'reg_pkg_rate': row.Regular,
          'crossing_pkg_rate': row.Crossing,
          'reg_weight_rate': row.Regularbag,
          'crossing_weight_rate': row.Crossingbag,
          'active': row.Active,
          'status': 'APPROVED',
          'start_date': datetime.datetime.now(),  # Add start_date
          'end_date': datetime.datetime.now(), # Add end_date, e.g., one year later



        }

        target_session.execute(insert(cust_contracts_table).values(mapped_data))
    
    target_session.commit()
    print(" loader_rates Data migration completed successfully . ")

if __name__ == "__main__":
    merge_and_migrate()
