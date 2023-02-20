from __future__ import annotations
from unittest import result
import pandas as pd
from compute_files import ComputeFile
from migrator import Migrator
import json
import multiprocessing
from datetime import datetime
import requests
import urllib
from cache import Cache

class Parser:

    def __init__(self, input_path='', extensions=[]):
        print('Parser')
        self.extensions = extensions
        print(input_path)
        self.cache_save = Cache()
        self.migrator = Migrator()
        self.sourceids = self.get_sourceids()
        self.config = self.migrator.load_data(input_file='../config/config.json')
        self.Global = {}
        self.files = ComputeFile(input_path=input_path, extensions=extensions).build_list_files()
        # print(self.files[1:10])
    
    def split_arrays(self, a=[], n=16):
        k, m = divmod(len(a), n)
        return [a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n)]
    
    def normalization(self, id=''):
        output = []
        config = self.config['RESOLVE_ENP']
        api = config['url']
        articleId = config['articleId'] + id
        param = config['params']
        api = api.replace(param, articleId)
        output = self.cache_save.get_item(key=articleId)
        if len(output) == 0:
            output = self.run_get(api_encoded=api)
            self.cache_save.update(pair={'key': articleId, 'value': output})
        return output
    
    def find_resolved_entity(self, data=[], value=''):
        if len(data) > 0 :
            for annotation in data[0]['annotations']:
                # print(annotation)
                exact = annotation['exact']
                tag = annotation['tags'][0]['uri']
                complete_address = annotation['id'] if 'id' in annotation else ''
                if value.lower() == exact.lower() :
                    return tag, complete_address
        return '', ''
    
    def run_get(self, api_encoded='', headers=None):
        output = None
        try:
            r = requests.get(api_encoded,  headers=headers)
            if r.status_code == requests.codes.ok :
                data_json = json.loads(r.text)
                output = data_json              
        except Exception as _:
            print('GET #Request on the ressource failed : ' + api_encoded)
        return output
    
    def get_sourceids(self):
        tmp = self.migrator.fetch(query="SELECT DISTINCT sourceid FROM articles")
        output = [v[0] for v in tmp]
        return output
    
    def push_data(self, entries=[], index=0):
        for file in entries :
            data = self.migrator.load_data(input_file=file)
            sourcedb = data['sourcedb'] if 'sourcedb' in data else ''
            sourceid = data['sourceid'] if 'sourceid' in data else ''
            get_sourceids = self.sourceids
            if str(sourceid) in get_sourceids:
                continue
            sourceurl = data['source_url'] if 'source_url' in data else 'https://www.ncbi.nlm.nih.gov/pubmed/'+sourceid
            text = data['text'] if 'text' in data else ''
            project = data['project'] if 'project' in data else ''
            self.add_article(columns=['sourcedb','sourceid','sourceurl','text','project'],
            data=[sourcedb, str(sourceid), sourceurl, text, project])
            if 'denotations' in data :
                annotations = data['denotations']
                # find annotations
                result_entnorm = self.normalization(id=sourceid)
                for denotation in annotations:
                    id = denotation['id']
                    begin = denotation['span']['begin']
                    end = denotation['span']['end']
                    token = text[begin:end]
                    type = denotation['obj']
                    normalized, full_address = self.find_resolved_entity(data=result_entnorm, value=token) # result_entnorm[token] if token in result_entnorm else ''
                    self.add_annotation(columns=['id','sourceid', 'token', 'begin', 'end', 'full_address', 'normalized_entity'],
                    data=[id, str(sourceid), urllib.parse.quote(token), str(begin), str(end), full_address, normalized])
                    self.merge_entity(columns=['name', 'type', 'normalized_entity'], data=[urllib.parse.quote(token), str(type), normalized])
        return 1
    
    def extract_block_json_files(self, file_list=[]):
        start_time = datetime.now()
        pool = multiprocessing.Pool()
        cpu = multiprocessing.cpu_count()
        _data = self.split_arrays(a=file_list, n=1)
        _output = 1
        self.push_data(entries=_data[0], index=1)
        # result_async = [pool.apply_async(self.push_data, args=(_data[index], index)) for index in range(len(_data))]
        # for result in result_async:
        #     _output = _output + result.get()
        print('Operation ended with success with ', _output, ' process runned !')
        time_elapsed = datetime.now() - start_time
        print('Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed))
        return None
        
    ###
    """
        Prepare data for loading
    """
    ###
    def add_article(self, columns=[], data=[]):
        result = self.migrator.insert_data(table='articles', columns=columns, data=['"'+d+'"' for d in data])
        return result
    
    def add_annotation(self, columns=[], data=[]):
        result = self.migrator.insert_data(table='denotations', columns=columns, data=['"'+d+'"' for d in data])
        return result
    
    def merge_entity(self, columns=[], data=[]):
        output = None
        res = self.migrator.fetchBy(table="entities", field="name", _value="type", result_value="vtype")
        if res == 0 : 
            output = self.migrator.insert_data(table='entities', columns=columns, data=['"'+d+'"' for d in data])
        return output

    def run(self):
        files = self.files
        self.extract_block_json_files(file_list=files)
        return None

# Parser(input_path='../../data/').run()
