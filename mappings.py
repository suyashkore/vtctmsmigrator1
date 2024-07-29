from db import get_table, target_engine
from datetime import datetime

def map_customers(source_row):
    erp_entry_date = source_row.CreateDate
    if erp_entry_date == '0000-00-00':
        erp_entry_date = datetime.today().date()  # Use today's date if the date is invalid

    # Truncate mobile numbers to 10 digits if they are too long
    mobile = source_row.MobileNo[:10]
    billing_mobile = source_row.MobileNo[:10]

    return {
        'code': source_row.GroupCode,
        'name': source_row.CustName,
        'industry_type': source_row.IndType,
        'city': source_row.City,
        'pincode': source_row.Pincode,
        'address': source_row.Address,
        'mobile': mobile,
        'email': source_row.EmailId,
        'billing_mobile': billing_mobile,
        'billing_email': source_row.EmailId,
        'billing_address': source_row.Address,
        'active': source_row.Status,
        'erp_entry_date': erp_entry_date,
        'payment_types': '[]',  # Provide a default value for payment_types
        'c_type': '',  # Provide a default value for c_type
        'primary_servicing_office_id': 1,  # Default value for primary_servicing_office_id
        # Add other fields as necessary
    }

TABLE_MAPPINGS = {
    'Customers': {
        'target_table': get_table(target_engine, 'customers'),
        'mapping_function': map_customers
    }
    # Add other table mappings here
}
