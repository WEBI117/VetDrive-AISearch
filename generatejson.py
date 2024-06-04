from pymongo import MongoClient
from bson import json_util
from pprint import pprint

# Connect to MongoDB
client = MongoClient('mongodb://root:example@localhost:27017/?authSource=admin')
db = client['999-alpha']

# Function to get collection schema
def get_collection_schema(collection):
    schema = {
        'name': collection.name,
        'fields': []
    }
    sample_document = collection.find_one()
    if sample_document:
        for key, value in sample_document.items():
            schema['fields'].append({
                'name': key,
                'type': type(value).__name__,
                'sample_value': value
            })
    return schema

# Get all collections
collections = db.list_collection_names()
print(collections)

# Generate schema for each collection
Database_schema = {}
for collection_name in collections:
    try:
        if(str.isalpha(collection_name)):
            collection = db[collection_name]
            schema = get_collection_schema(collection)
            schema['indexes'] = collection.index_information()
            Database_schema[collection_name] = schema
    except:
        continue
# Print the database schema
pprint(Database_schema)

# Optionally, write the schema to a file
import json
with open('database_schema.json', 'w') as f:
    #json.dump(Database_schema, f, indent=4, default=json_util.default)
    json.dump(Database_schema, f, indent=4,  default=json_util.default)

