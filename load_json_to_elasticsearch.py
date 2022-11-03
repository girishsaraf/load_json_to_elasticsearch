import os
import json
import argparse
from elasticsearch import Elasticsearch, helpers


INDEX_MAPPING_FILE = "index_mapping.json"
INDEX_SETTING_FILE = "index_settings.json"


class CustomException(Exception):
    pass


# Load Index Config from JSON file
def get_index_config_dict(filename):
    try:
        with open(filename) as config_file:
            config_detail_dict = json.load(config_file)
        return config_detail_dict
    except Exception as e:
        print("Error: {}".format(str(e)))
        raise Exception("Error: {}".format(str(e)))


# Creates an ElasticSearch Index
def create_index(es_conn, index_name):
    try:
        config_setting = get_index_config_dict(INDEX_SETTING_FILE)
        config_mapping = get_index_config_dict(INDEX_MAPPING_FILE)
        if check_if_connected(es_conn_object=es_conn):
            es_conn.indices.create(
                index=index_name,
                settings=config_setting,
                mappings=config_mapping
            )
        else:
            raise CustomException("Connection error")
    except Exception as e:
        print("Error: {}".format(str(e)))
        raise Exception("Error: {}".format(str(e)))


# Inserts data in bulk to index, retries 5 times in gaps of 5 seconds if not connected
def insert_to_index(es_connection, list_json_data, index_name):
    try:
        print("Inserting data into {} ...".format(index_name))
        helpers.bulk(
            es_connection,
            list_json_data,
            index=index_name,
            raise_on_error=False
        )
        print("Data indexed : {} rows".format(len(list_json_data)))
        return True
    except Exception as e:
        print("Error: {}".format(str(e)))
        return False


# Delete index from ElasticSearch Cluster
def delete_index(es_conn, index_name=None):
    try:
        if check_if_connected(es_conn_object=es_conn):
            es_conn.options(
                ignore_status=[400, 404]
            ).indices.delete(index=index_name)
        else:
            raise CustomException("Connection error")
        print("Index {} deleted".format(index_name))
    except Exception as e:
        raise Exception("Error: {}".format(str(e)))


# Returns Status of Conn
def get_status_value(response_object):
    try:
        return "Successful" if response_object.status_code == 200 \
            else "Insert Successful" if response_object.status_code == 201 \
            else response_object
    except Exception as e:
        return "Error: {}".format(str(e))


# Check if index exists in the ES Cluster
def check_if_index_exists(es_conn_object, index_name):
    try:
        if not es_conn_object.indices.exists(index=index_name):
            return False
        return True
    except Exception as e:
        print("Error: {}".format(str(e)))
        raise 


# Loads json file into memory
def load_json(json_file_path):
    try:
        json_list = []
        with open(json_file_path) as file_data:
            for json_obj in file_data:
                json_data = json.loads(json_obj)
                json_list.append(json_data)
        return json_list
    except Exception as e:
        print("Error: {}".format(str(e)))
        return []


# Returns whether elastic is connected
def check_if_connected(es_conn_object=None):
    try:
        if not es_conn_object.ping():
            return False
        return True
    except Exception as e:
        print("Error: {}".format(str(e)))
        return False


# Returns an ElasticSearch Connection Object
def get_elastic_connection(hostname):
    try:
        es_conn_obj = Elasticsearch(
            hostname,
            request_timeout=30,
            max_retries=10,
            retry_on_timeout=True
        )
        return es_conn_obj
    except Exception as e:
        print("Error: {}".format(str(e)))
        return None


# Starter Code
if __name__ == "__main__":

    # Parse user input args
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host", 
        help="Elastic Search Host", 
        default="http://localhost:9200"
    )
    parser.add_argument(
        "--index", 
        help="Name of the index to be created", 
        default="base_index"
    )
    parser.add_argument(
        "--json_file", 
        help="Path of the json file input", 
        default=""
    )
    parser.add_argument(
        "--json_directory", 
        help="Directory from which to parse all json files", 
        default=""
    )
    args = parser.parse_args()

    # Fetch values from user input or default values
    host = args.host
    index = args.index
    json_file = args.json_file
    json_dir = args.json_directory

    try:
        elastic_connection = get_elastic_connection(host)

        # Check if index exists, then ask if it needs to be deleted, else add to index
        if check_if_connected(elastic_connection):
            print("Connected to ES Cluster - {}".format(host))
            if check_if_index_exists(elastic_connection, index):
                print("{} already exists on the cluster".format(index))
                choice = input("Would you like to delete the index? [y/n]")
                if choice.lower() == "y":
                    print("Deleting index - {}".format(index))
                    delete_index(elastic_connection, index)
                    create_index(elastic_connection, index)
                elif choice.lower() == "n":
                    pass
                else:
                    raise ValueError("Invalid choice")
            else:
                print("Creating Index - {}".format(index))
                create_index(elastic_connection, index)

            if json_dir != "":
                for path in os.listdir(json_dir):
                    # check if current path is a json file
                    file_ext = path.split('.')[-1].lower()
                    if os.path.isfile(os.path.join(json_dir, path)) and file_ext == "json":
                        file_name = os.path.join(json_dir, path)
                        data = load_json(file_name)
                        insert_to_index(elastic_connection, data, index)
            else:
                data = load_json(json_file)
                insert_to_index(elastic_connection, data, index)
    except Exception as exc:
        print("Error: {}".format(str(exc)))
        raise Exception("Error: {}".format(str(exc)))
