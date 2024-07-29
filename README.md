# vtctmsmigrator
Python command line vtc tms data migrator

mkdir vtctmsmigrator

cd vtctmsmigrator

python3 -m venv venv

## Activate the virtual environment (use the appropriate command for your OS)

### On Windows:
venv\Scripts\activate
### On macOS and Linux:
source venv/bin/activate
### Install dependencies
pip install -r requirements.txt
### How to run
python3 cli.py migrate --table Customers

