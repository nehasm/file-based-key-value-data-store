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

directory_created = FilePreprocess(db_path).create_folder()
if not directory_created:
    print(f"Permission denied: You can not create the directory `{db_path}`.\n")
    exit(0)


key = 'h'

_data_found, message = CRD().check_DD(key, db_path)
print(message)