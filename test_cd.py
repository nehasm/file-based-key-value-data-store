from sys import exit
from argparse import ArgumentParser
from crd.store import CRD
from foldercreate import FilePreprocess
from config import DEFAULT_DB_PATH,DEFAULT_DB_NAME

parser = ArgumentParser()
parser.add_argument('--datastore', help='Enter the datastore absolute path.')
args = parser.parse_args()
# Select user provided datastore path else, select the default path.
if args.datastore:
    db_path = args.datastore
else:
    db_path = DEFAULT_DB_PATH

# Create a datastore directory.
directory_created = FilePreprocess(db_path).create_folder()
if not directory_created:
    print(f"Permission denied: You can not create the directory `{db_path}`.\n")
    exit(0)


json_data = {
    "b": {
        "b1": "1",
        "b2": "2",
        "Time-To-Live": 50
    },
    "c": {
        "c1": "1",
        "c2": "2",
        "Time-To-Live": 202
    },
    "d": {
        "d1": "1",
        "d2": "2",
        "Time-To-Live": 234
    },
    "e": {
        "e1": "1",
        "e2": "2",
        "Time-To-Live": 450
    },
    "f": {
        "g1": "1",
        "g2": "2",
        "Time-To-Live": 789
    },
    "h": {
        "h1": "1",
        "h2": "2",
        "Time-To-Live": 600
    }
    }
    
_valid_data, message = CRD().check_CD(json_data, db_path)
print(message)