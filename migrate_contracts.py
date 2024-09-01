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
contract_table = Table('Contract', metadata, autoload_with=source_engine)
service_selection_table = Table('Serviceselection', metadata, autoload_with=source_engine)
cust_contracts_table = Table('cust_contracts', metadata, autoload_with=target_engine)

# Create SQL statements for merging and migrating data
def merge_and_migrate():
    # Define the query to merge data
    query = text("""
    SELECT c.ContractID, c.StartDate, c.EndDate, c.ContractType, c.Status, c.ConsignorID, c.ConsignorName, c.PkgsTYPE,
           s.id, s.ConsignorCode, s.ConsignorName AS ServiceConsignorName, s.ContractType AS ServiceContractType,
           s.Product, s.ServiceType, s.RateTypesAllowed, s.MatricesAllowed, s.PickupDelivery, s.FreightDiscountAllowed,
           s.DiscountRate, s.DiscountRateType, s.Discount, s.DeliveryReattempt, s.DeliveryReattemptRate, s.ExcessWeight,
           s.DemuBillGen, s.DemuBillGenType, s.FreeStorageDays, s.MinDemuCharge, s.DemurrageRate, s.DemurrageRateType,
           s.MaxDemuCharge, s.FuelSurcharges, s.OctroiSurcharges, s.SKUWise, s.TaxPayer, s.DocumentCharges, s.HamaliCharges,
           s.Doordeliverycharge, s.SlabRangeType, s.PkgType
    FROM Contract c
    LEFT JOIN Serviceselection s ON c.ContractID = s.ContractID
    UNION ALL
    SELECT c.ContractID, c.StartDate, c.EndDate, c.ContractType, c.Status, c.ConsignorID, c.ConsignorName, c.PkgsTYPE,
           s.id, s.ConsignorCode, s.ConsignorName AS ServiceConsignorName, s.ContractType AS ServiceContractType,
           s.Product, s.ServiceType, s.RateTypesAllowed, s.MatricesAllowed, s.PickupDelivery, s.FreightDiscountAllowed,
           s.DiscountRate, s.DiscountRateType, s.Discount, s.DeliveryReattempt, s.DeliveryReattemptRate, s.ExcessWeight,
           s.DemuBillGen, s.DemuBillGenType, s.FreeStorageDays, s.MinDemuCharge, s.DemurrageRate, s.DemurrageRateType,
           s.MaxDemuCharge, s.FuelSurcharges, s.OctroiSurcharges, s.SKUWise, s.TaxPayer, s.DocumentCharges, s.HamaliCharges,
           s.Doordeliverycharge, s.SlabRangeType, s.PkgType
    FROM Serviceselection s
    RIGHT JOIN Contract c ON c.ContractID = s.ContractID
    WHERE s.ContractID IS NULL
    """)

    result = source_session.execute(query)

    # Iterate through the results and insert into the target table
    for row in result:
        mapped_data = {
            'customer_id': 34693,
            'ctr_num': row.ConsignorID,
            'start_date': row.StartDate,
            'end_date': row.EndDate,
            'payment_type': row.ContractType,
            'load_type': row.ServiceType or 'default_value',
            'distance_type': row.MatricesAllowed or 'default_value',
            'rate_type': row.SlabRangeType or 'default_value',
            'pickup_delivery_mode': row.PickupDelivery or 'default_value',
            'excess_wt_chargeable': row.ExcessWeight or 0,  # Providing a default value
            'oda_del_chargeable': row.DeliveryReattempt or 0,
            'credit_period': row.Discount or 0,
            'docu_charges_per_invoice': row.DocumentCharges or 0,
            'loading_charges_per_pkg': row.HamaliCharges or 0,
            'fuel_surcharge': row.FuelSurcharges or 0,
            'oda_min_del_charges': row.MinDemuCharge or 0,
            'reverse_pick_up_charges': row.DeliveryReattemptRate or 0,
            'insurance_charges': row.DemurrageRate or 0,
            'minimum_chargeable_wt': row.ExcessWeight or 0,
            'active': row.Status or 0,
        }

        target_session.execute(insert(cust_contracts_table).values(mapped_data))
    
    target_session.commit()
    print(" Contract Data migration completed successfully . ")

if __name__ == "__main__":
    merge_and_migrate()
