from openai import OpenAI
from dotenv import load_dotenv, find_dotenv
from pprint import pprint
from mongodb_interface import Db_interface
import json
_ = load_dotenv(find_dotenv()) # read local .env file
delimiter = "###"
data_delimiter = "---"




class chatgtp_interface:
    def __init__(self):
        try:
            conn_str = 'mongodb://root:example@localhost:27017/?authSource=admin'
            db_name = '999-alpha'
            self.client = OpenAI()
            self.db_interface = Db_interface(conn_str,db_name)
            # defaults to getting the key using os.environ.get("OPENAI_API_KEY")
            # if you saved the key under a different environment variable name, you can do something like:
            # client = OpenAI(
            #   api_key=os.environ.get("CUSTOM_ENV_NAME"),
            # )
        except Exception as e:
            print(e)

    def run_conversation(self, user_query):
        schema_info = self.get_schemas(user_query)
        if(schema_info):
            query = self.generate_query(user_query, schema_info)
            return query.content
        return
    
    def generate_query(self, user_query, schema_info):
        system_message = f"""
        You will be provided with a stringified JSON object delimited by {data_delimiter}: \
        The JSON object has schema information for relevant MongoDB collections \
        You will be provided with a user message that can be responed to with data that can be retrieved from the MongoDB instance. \
        Your job is to construct a MongoDB Query Language query to answer the user provided query \
        The query should be as optimised as possible. \

        Provide your response as a MongoDB Query Language query string.
        """

        system_message_data = f"""
        Here are the collection names:
        {data_delimiter}{schema_info}{data_delimiter}

        Remember to provide your response as a MongoDB Query Language query string.
        """

        messages = [
            {"role": "system", "content": system_message},
            {"role": "system", "content": system_message_data},
            {"role": "user", "content":f"{user_query}"}
        ]

        response = self.message(messages)
        return response

    
    def get_schemas(self, user_query):
        system_message_fnc = f"""
        You will be provided with a list of MongoDB collection names in a MongoDB instance delimited by {data_delimiter}: \
        You will be provided with a user message that can be responed to by with data that can be retrieved from the MongoDB instance. \

        """
        system_message_data_fnc = f"""
        Here are the collection names:
        {data_delimiter}{self.db_interface.stringify_collection_names()}{data_delimiter}
        """
        messages = [
            {"role": "system", "content": system_message_fnc},
            {"role": "system", "content": system_message_data_fnc},
            {"role": "user", "content":f"{user_query}"}
        ]
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "get_schema_info",
                    "description": "Get the schema info in JSON format for the given collection names",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "collection_names": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "The list of collection names to get schemas for e.g. [animals, events]",
                            },
                        },
                        "required": ["collection_names"],
                    },
                },
            }
        ]

        response = self.message(messages, tools, tool_choice='auto')
        tool_calls = response.tool_calls
        if(tool_calls):
            tool_call = tool_calls[0]
            function_args = json.loads(tool_call.function.arguments)
            function_response = self.db_interface.get_schema_info(
                collection_names=function_args.get("collection_names"),
            )
            return function_response
        return

    def message(self, messages, tools=[], tool_choice=''):
        completion = None
        if(tools and tool_choice):
            completion = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=messages,
                tools=tools,
                tool_choice="auto"
            )
        else:
            completion = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=messages,
            )
        if(completion):
            return completion.choices[0].message
        return completion


if __name__ == "__main__":
    user_query = "All transactions associated with events between today and 3 years ago"
    interface = chatgtp_interface()
    pprint(interface.run_conversation(user_query))
