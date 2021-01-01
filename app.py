from sys import exit
from flask import Flask
from argparse import ArgumentParser
from crd.store import CreateData, ReadData, DeleteData
from crd.store import CRD
from foldercreate import FilePreprocess
from config import DEFAULT_DB_PATH,DEFAULT_DB_NAME

DEBUG = True
HOST = 'localhost'
PORT = 5000
SECRET_KEY = '\x14b\x8cx\xcf\xa5?7\xac\xf9\x1b?\xf8\\Z\x994\xe1\r2\n\xecw\xf3'

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


app = Flask(__name__)


# Flask App Configurations
app.config['DEBUG'] = DEBUG
app.config['SECRET_KEY'] = SECRET_KEY


# API Endpoints
app.add_url_rule('/datastore/create', view_func=CreateData.as_view('create', db_path), methods=['POST'])
app.add_url_rule('/datastore/read', view_func=ReadData.as_view('read', db_path), methods=['GET'])
app.add_url_rule('/datastore/delete', view_func=DeleteData.as_view('delete', db_path), methods=['DELETE'])


# Initiates Flask Server
if __name__ == '__main__':
    app.run(host=HOST, port=PORT)