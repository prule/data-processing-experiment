import requests
import pyjson5
import json


class JsonRepository:

    def load_url(self, path):
        content = requests.get(path).content.decode('utf8')
        print(content)
        return pyjson5.loads(content)

    def load_file(self, path):
        with open(path, "r") as file:
            return pyjson5.load(file)

    def load_str(self, content: str):
        return pyjson5.loads(content)

    def print(self, obj):
        return json.dumps(obj, indent=2)
