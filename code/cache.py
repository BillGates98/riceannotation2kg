import json
from datetime import datetime
import base64

class Cache:

    def __init__(self, file_path='../cache/cache.json'):
        self.file_path = file_path
        self.data = None

    def get_item(self, key=''):
        f = open(self.file_path,)
        self.data = json.load(f)
        # f.close()
        _key = base64.b64encode((key).encode('ascii')).decode('ascii')
        if _key in self.data:
            return self.data[_key]['value']
        else:
            return []

    def load(self):
        f = open(self.file_path,)
        self.data = json.load(f)
        return self.data

    def update(self, pair={}):
        data = self.load()
        # parent = str(hash(pair['key']))
        parent = base64.b64encode(str(pair['key']).encode('ascii')).decode('ascii')
        data[parent] = pair
        result = json.dumps(data, indent=4)
        try:
            with open(self.file_path, 'w') as outfile:
                outfile.write(result)
        except Exception as _:
            print('Cache update failed on key : ' + str(pair['key']))
        return pair

    def get_ressource(self, uri=''):
        _value = self.get_item(key=uri)
        if len(_value) == 0 :
            # self.update(pair={'key': uri, 'value':_value})
            return None
            # print('>>>>>>> ', _value)
        return _value

