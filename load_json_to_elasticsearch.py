import os
import json
import requests
import argparse
from elasticsearch import Elasticsearch, helpers


def is_connected(**kwargs):
    if requests.get("http://{}:{}".format(kwargs["host"], kwargs["port"])).status_code == 200:
        return True
    return False


def is_index_exists(**kwargs):
    if requests.get("http://{}:{}/{}".format(kwargs["host"], kwargs["port"], kwargs["index"])).status_code == 200:
        return True
    return False


def delete_index(**kwargs):
    return requests.delete("http://{}:{}/{}".format(kwargs["host"], kwargs["port"], kwargs["index"]))


def create_index(**kwargs):
    return requests.put("http://{}:{}/{}".format(kwargs["host"], kwargs["port"], kwargs["index"]))


def insert_to_index(list_json_data, index_name):
    try:
        print("Inserting data into {} ...".format(index_name))
        resp = helpers.bulk(
            es,
            list_json_data,
            index=index_name
        )
        print("helpers.bulk() RESPONSE:", resp)
        print("helpers.bulk() RESPONSE:", json.dumps(resp, indent=4))
        return True
    except Exception as exc:
        print("Error: {}".format(str(exc)))
        return False


def load_json(json_file_path):
    try:
        json_list = []
        with open(json_file_path) as file_data:
            for json_obj in file_data:
                json_data = json.loads(json_obj)
                json_list.append(json_data)
        return json_list
    except Exception as exc:
        print("Error: {}".format(str(exc)))
        return []


def get_status_value(response_object):
    try:
        return "Successful" if response_object.status_code == 200 \
            else "Insert Successful" if response_object.status_code == 201 \
            else response_object
    except Exception as exc:
        return "Error: {}".format(str(exc))


if __name__ == "__main__":

    # Parse user input args
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", help="Host server of the destination elasticsearch", default="localhost")
    parser.add_argument("--port", help="Port of the destination elasticsearch", default="9200")
    parser.add_argument("--index", help="Name of the index to be created", default="base_index")
    parser.add_argument("--json_file", help="Path of the json file input", required=True)
    parser.add_argument("--json_directory", help="Directory from which to parse all json files", default="")
    args = parser.parse_args()

    # Fetch values from user input or default values
    host, port, index, json_file, json_dir = args.host, args.port, args.index, args.json_file, args.json_directory

    # Create a new connection object
    connection_object = {
        "host": host,
        "port": port,
        "index": index
    }

    es = Elasticsearch(host)

    # Check if index exists, then ask if it needs to be deleted, else add to index
    if is_connected(**connection_object):
        if is_index_exists(**connection_object):
            print("{} exists in Elasticsearch server.".format(index))
            choice = input("Would you like to delete? [y/n]")
            if choice.lower() == "y":
                print("Deleting {} ...".format(index))
                print(get_status_value(delete_index(**connection_object)))
            elif choice.lower() == "n":
                pass
            else:
                raise ValueError("Invalid choice")
        else:
            print("Creating {} ...".format(index))
            print(get_status_value(create_index(**connection_object)))

        if json_dir != "":
            for path in os.listdir(json_dir):
                # check if current path is a file
                if os.path.isfile(os.path.join(json_dir, path)):
                    file_name = os.path.join(json_dir, path)
                    data = load_json(file_name)
                    if insert_to_index(data, index):
                        print("Indexing Successful for file {}".format(file_name))
                    else:
                        print("Error Occurred for file {}".format(file_name))
        else:
            data = load_json(json_file)
            if insert_to_index(data, index):
                print("Indexing Successful")
            else:
                print("Error Occurred")
