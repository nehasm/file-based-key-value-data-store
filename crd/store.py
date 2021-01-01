import json
import portalocker
import threading
from os import path
from datetime import datetime, timedelta
from dateutil.parser import parse
from flask.views import MethodView
from flask import jsonify, request
from config import DEFAULT_DB_NAME,DEFAULT_DB_PATH

class CRD:
    def check_TTL(self, value):
        create_time = parse(value['CreatedAt'])
        time_to_live = value['Time-To-Live']
        if time_to_live is not None:
            expire_time = create_time + timedelta(seconds=time_to_live)
            remain_time = (expire_time - datetime.now()).total_seconds()
            if remain_time <= 0:
                return False
        return value

    def check_CD(self, json_data, db_path):
        if not isinstance(json_data, dict):
            return False, "Incorrect request data format. Only JSON object with key-value pair is acceptable."
        if len(json.dumps(json_data)) > 1000000000:
            return False, "DataStore limit will exceed 1GB size."
        for key, value in json_data.items():
            if len(key) > 32:
                return False, "The keys must be in 32 characters length."
            if not isinstance(value, dict):
                return False, "The values must be in JSON object formats."
            if len(json.dumps(value)) > 16384:
                return False, "The values must be in 16KB size."
        datastore = path.join(db_path, DEFAULT_DB_NAME)
        data = {}
        if path.isfile(datastore):
            with open(datastore) as f:
                portalocker.lock(f, portalocker.LOCK_EX)
                data = json.load(f)
                portalocker.unlock(f)
                # file size exceeded 1GB size.
                prev_data_obj = json.dumps(data)
                if len(prev_data_obj) >= 1000000000:
                    return False, "File Size Exceeded 1GB."

        # Check any key present in previous datastore data.
        for key in json_data.keys():
            if key in data.keys():
                return False, "Key already exist in DataStore."
       
        def prepare_data_create(json_data_keys):
            # Add CreatedAt and add Time-To-Live if the data dont have in it.
            for key in json_data_keys:
                singleton_json = json_data[key]
                singleton_json["CreatedAt"] = datetime.now().isoformat()
                singleton_json["Time-To-Live"] = singleton_json["Time-To-Live"] if 'Time-To-Live' in singleton_json else None
                data[key] = singleton_json
        thread_count = 4
        items = list(json_data.keys())
        split_size = len(items) // thread_count
        threads = []
        for i in range(thread_count):
            start = i * split_size
            end = None if i+1 == thread_count else (i+1) * split_size

            threads.append(threading.Thread(target=prepare_data_create, args=(items[start:end], ), name=f"t{i+1}"))
            threads[-1].start()
        # Wait for all threads to finish.
        for t in threads:
            t.join()
        # Write the new data.
        with open(datastore, 'w+') as f:
            #process only allowed to access the file at a time
            portalocker.lock(f, portalocker.LOCK_EX)
            json.dump(data, f)
            portalocker.unlock(f)
        return True, "Data created in DataStore."

    def read_delete_preprocess(self, key, db_path):
        datastore = path.join(db_path, DEFAULT_DB_NAME)
        if not path.isfile(datastore):
            return False, "Empty DataStore. Data not found for the key."
        with open(datastore) as f:
            portalocker.lock(f, portalocker.LOCK_EX)
            data = json.load(f)
            portalocker.unlock(f)
        if key not in data.keys():
            return False, "No data found for the key provided."
        target = data[key]
        target_active = self.check_TTL(target)
        if not target_active:
            return False, "Requested data is expired for the key."
        return True, data

    def check_RD(self, key, db_path):
        status, message = self.read_delete_preprocess(key, db_path)
        if not status:
            return status, message
        data = message[key]
        del data['CreatedAt']
        return status, data

    def check_DD(self, key, db_path):
        status, message = self.read_delete_preprocess(key, db_path)
        if not status:
            return status, message
        datastore = path.join(db_path, DEFAULT_DB_NAME)
        del message[key]
        with open(datastore, 'w+') as f:
            portalocker.lock(f, portalocker.LOCK_EX)
            json.dump(message, f)
            portalocker.unlock(f)
        return True, "Data is deleted from the datastore."
    
class CreateData(MethodView):
    def __init__(self, db_path):
        self.db_path = db_path
    def post(self):
        try:
            json_data = request.get_json(force=True)
        except Exception:
            return jsonify({"status": "error", "message": "Incorrect request data format. Only JSON object is acceptable."}), 400
        valid_data, message = CRD().check_CD(json_data, self.db_path)
        if not valid_data:
            return jsonify({"status": "error", "message": message}), 400
        return jsonify({"status": "success", "message": message}), 200

class ReadData(MethodView):
    def __init__(self, db_path):
        self.db_path = db_path
    def get(self):
        key = request.args.get('key')
        if key is None:
            return jsonify({"status": "error", "message": "key is required as a query param."}), 400.
        data_found, message = CRD().check_RD(key, self.db_path)
        if not data_found:
            return jsonify({"status": "error", "message": message}), 404
        return jsonify(message), 200

class DeleteData(MethodView):
    def __init__(self, db_path):
        self.db_path = db_path
    def delete(self):
        key = request.args.get('key')

        if key is None:
            return jsonify({"status": "error", "message": "key is required as a query param."}), 400
        data_found, message = CRD().check_DD(key, self.db_path)
        if not data_found:
            return jsonify({"status": "error", "message": message}), 404
        return jsonify({"status": "success", "message": message}), 200