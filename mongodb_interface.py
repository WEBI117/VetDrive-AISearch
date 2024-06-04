from pymongo import MongoClient
from bson import json_util
from pprint import pprint
import json

class Db_interface:
    def __init__(self, connection_string, db_name):
        try:
            self.client = MongoClient(connection_string)
            self.db = self.client[f"{db_name}"]
        except Exception as e:
            print('Unable to initialize DB connection.')
            print(e)
    def get_collection_names(self):
        try:
            collections = self.db.list_collection_names()
            collections = list(filter(lambda x : x.isalpha(), collections))
            return collections
        except Exception as e:
            print(e)
        return []

    def stringify_collection_names(self):
        names = self.get_collection_names()
        return json.dumps(names)

    def _get_collection_schema(self, collection):
        schema = {
            'name': collection.name,
            'fields': []
        }
        sample_document = collection.find_one()
        if sample_document:
            for key, value in sample_document.items():
                if(len(str(value)) < 50):
                    schema['fields'].append({
                        'name': key,
                        'type': type(value).__name__,
                        'sample_value': value
                    })
                else:
                    schema['fields'].append({
                        'name': key,
                        'type': type(value).__name__,
                    })
        return schema

    def _stringify_schema(self, schema):
        return json.dumps(schema, default=json_util.default)

    def get_schema_info(self, collection_names):
        result = ""
        try:
            info_strings = []
            for name in collection_names:
                collection = self.db[name]
                schema = self._get_collection_schema(collection)
                json_string = self._stringify_schema(schema)
                info_strings.append(json_string)
            result = ','.join(info_strings)
        except Exception as e:
            print(e)

        return result

if __name__ == "__main__":
    conn_str = 'mongodb://root:example@localhost:27017/?authSource=admin'
    db_name = '999-alpha'
    test = Db_interface(conn_str,db_name)
    names = test.get_collection_names()
    print(test.get_schema_info([names[0]]))
    print('------')
    print(test.get_schema_info([names[0],names[1]]))