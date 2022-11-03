# Load JSON to ElasticSearch

Service to Load JSON File / All JSON Files in Directory to Elasticsearch Host

## Usage

### Accepted Parameters
`host` - ElasticSearch Host with Port, default is `http://localhost:9200`

`index` - ElasticSearch Index Name, default is `base_index`

`json_file` - JSON Filename to load into ES, default is ``

`json_directory` - Directory path for parsing all JSON Files, default is ``

### Getting Started

1. Install Requirements:
`pip install -r requirements.txt`

2. Run the command to load into ElasticSearch
`python load_json_to_elasticsearch.py --json_file abcd.json`

### Configuring your Index

The 2 files for specifying index configuration are:
1. **index_settings.json** - Used to specify index settings like number of shards and number of replicas
2. **index_mapping.json** - Used to specify index config for individual fields
