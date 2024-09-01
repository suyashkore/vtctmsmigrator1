from db import get_table, target_engine
from datetime import datetime
import subprocess  # For calling the PHP script

def call_php_for_hash(password):
    """Calls a PHP script to hash the password using Laravel's Hash::make"""
    result = subprocess.run(['php', 'php/hash_password.php', password], capture_output=True, text=True)
    return result.stdout.strip()

def map_customers(source_row):
    erp_entry_date = source_row.CreateDate
    if erp_entry_date == '0000-00-00':
        erp_entry_date = datetime.today().date()  # Use today's date if the date is invalid

    # Truncate mobile numbers to 10 digits if they are too long
    mobile = source_row.MobileNo[:10]
    billing_mobile = source_row.MobileNo[:10]

    return {
        'code': source_row.CustCode,
        'name': source_row.CustName,
        'payment_types': source_row.Category,
        'name_reg': source_row.CustNameMar,
        'industry_type': source_row.IndType,
        'city': source_row.City,
        'pincode': source_row.Pincode,
        'address': source_row.Address,
        'address_reg': source_row.AddressMar,
        'mobile': mobile,
        'email': source_row.EmailId,
        'billing_mobile': billing_mobile,
        'billing_email': source_row.BillingMail,
        'billing_address': source_row.Address,
        'billing_address_reg': source_row.BillAddressMar,
        'other_servicing_offices': source_row.Location,
        'active': source_row.Status,
        'erp_entry_date': erp_entry_date,
        'payment_types': '[]',  # Default value
        'c_type': '',  # Default value
        'primary_servicing_office_id': 1,  # Default value
    }

def map_driver_master(source_row):
    return {
        'vendor_name': source_row.DName,
        'contracting_office_id': 1,
        'default_rate_type': 'HOURLY',
        'status': 'CREATED',
        'start_date': '2024-07-30 00:00:00',
        'end_date': '2024-12-31 23:59:59',
    }

def map_offices(source_row):
    return {
        'code': source_row.CPCODE,
        'name': source_row.NAME,  # Assuming the city name can be used as the office name
        'district': source_row.CPDistrict,
        'taluka': source_row.cptaluka,
        'city': source_row.cpdeponame,
        'pincode': source_row.cppincode,
        'latitude': source_row.Lattitude,
        'longitude': source_row.Longitude,
        'address': source_row.cpaddress,
        'active': source_row.Active,
        'o_type': 'HUB',
    }

def map_vehicles(source_row):
    rc_num = source_row.RCBookNo if source_row.RCBookNo else "1"  # Default value for rc_num

    return {
        'rc_num': rc_num,
        'vehicle_num': source_row.Vehicle_No,
        'vehicle_ownership': source_row.VendorType,
        'make': source_row.VehicleType,
        'model': source_row.VehicleType,
        'gvw': source_row.GVW,
        'capacity': source_row.Capacity,
        'length': source_row.Length,
        'width': source_row.Width,
        'height': source_row.Height,
        'insurance_policy_num': source_row.InsuranceNo,
        'insurance_expiry': source_row.Insurance_Validity,
        'fitness_cert_num': source_row.Fitness_No or '',  # Handle potential null value
        'fitness_cert_expiry': source_row.Fitness_Validity,
        'active': source_row.ActiveFlag,
        'status': 'APPROVED',
        'base_office_id': '1',
    }

def map_vendors(source_row):
    return {
        'code': source_row.VendorCode,
        'name': source_row.VendorName,
        'mobile': source_row.Mobile_No,
        'email': source_row.Email,
        'active': source_row.ActiveFlag,
        'contracting_office_id': '1',
        'v_type': 'OTHERS'
    }

def map_station_coverage(source_row):
    return {
        'name': source_row.CityNameEng,
        'name_reg': source_row.CityNameMar,
        'pincode': source_row.Pincode,
        'taluka': source_row.Taluka,
        'district': source_row.District or '',
        'district_reg': source_row.DistrictMar,
        'state': source_row.State if source_row.State else 'Unknown',  # Default value
        'latitude': source_row.Latitude,
        'longitude': source_row.Longitude,
        'route_num': source_row.RouteNo,
        'active': source_row.Active,
        'status': 'CREATED',
        'country': 'India',
        'servicing_office_id': 1
    }

def map_users(source_row):
    max_job_title_length = 32  # Adjust based on your schema
    job_title = source_row.Designation[:max_job_title_length]  # Truncate if necessary

    # Handle blank passwords
 # Hash the password using PHP script

    return {
        'name': source_row.FullName,
        'login_id': source_row.Name,
        'mobile': source_row.UserMobile,
        'email': source_row.UserEmail,
        'password_hash': source_row.Password,  # Use hashed password or default value
        'profile_pic_url': source_row.imageurl,
        'user_type': 'TENANT',
        'job_title': job_title,  # Use truncated job title
        'active': source_row.ActiveFlag,
    }



def map_cust_contract_excess_weight_rates(source_row):
    return {
        'ctr_num': source_row.ConsignorCode,
        'lower_limit': source_row.FromWeight,
        'upper_limit': source_row.ToWeight,
        'rate': source_row.Rate,
        'tenant_id': '1',
        'cust_contract_id': '3',
    }


def map_cust_contract_oda_charges(source_row):
    return {
        'ctr_num': source_row.ConsignorCode,
        'from_place': source_row.FromPlace,
        'to_place': source_row.ToPlace,
        'rate': source_row.Rate,
        'tenant_id': '1',
        'cust_contract_id': '3',
    }


TABLE_MAPPINGS = {
    'Customers': {
        'target_table': get_table(target_engine, 'customers'),
        'mapping_function': map_customers
    },
    'DriverMaster': {
        'target_table': get_table(target_engine, 'driver_rates'),
        'mapping_function': map_driver_master
    },
    'CPDEPO': {
        'target_table': get_table(target_engine, 'offices'),
        'mapping_function': map_offices
    },
    'Vehicle': {
        'target_table': get_table(target_engine, 'vehicles'),
        'mapping_function': map_vehicles
    },
    'Vendor': {
        'target_table': get_table(target_engine, 'vendors'),
        'mapping_function': map_vendors
    },
    'CityMaster': {
        'target_table': get_table(target_engine, 'station_coverage'),
        'mapping_function': map_station_coverage
    },
    'users': {
        'target_table': get_table(target_engine, 'users'),
        'mapping_function': map_users
    },
     'ExcessWeight': {
        'target_table': get_table(target_engine, 'cust_contract_excess_weight_rates'),
        'mapping_function': map_cust_contract_excess_weight_rates
    },
      'Doordeliverycontract': {
        'target_table': get_table(target_engine, 'cust_contract_oda_charges'),
        'mapping_function': map_cust_contract_oda_charges
    },
    # Add other table mappings here
}
