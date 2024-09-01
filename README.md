# vtctmsmigrator
Python command line vtc tms data migrator

mkdir vtctmsmigrator

cd vtctmsmigrator

python3 -m venv venv

## Activate the virtual environment (use the appropriate command for your OS)

### On Windows:
venv\Scripts\activate
### Install dependencies
pip install -r requirements.txt
### How to run
python cli.py migrate --table Customers
#### How To Run All Table 
python cli.py migrate --table all
#### l Check
pip list
#### Prtiqular file 
python migrate_contracts.py


