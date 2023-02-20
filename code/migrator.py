#!/usr/bin/python3

import pymysql
import json

class Migrator:
    
    def __init__(self):
        config = self.load_data(input_file='../config/config.json')
        self.db = pymysql.connect(host=config['host'],
        user = config['user'], 
        password = config['password'],
        db = config['database'])
        self.cursor = self.db.cursor()
    
    def load_data(self, input_file=''):
        f = open(input_file)
        data = json.load(f)
        f.close()
        return data
    
    def create(self, query=""):
        output = None
        try:
            self.cursor.execute(query)
            output = self.db.commit()
        except:
            self.db.rollback()
        return output

    def truncate(self, table=""):
        return self.create(query="TRUNCATE TABLE " + table)
    
    def update(self, table="", field="", value="", id=""):
        query = """ 
        UPDATE {table} SET {col}={value} WHERE id={id}
        """
        query = query.replace('{table}', table)
        query = query.replace('{col}', field)
        query = query.replace('{value}', '"' + str(value) + '"')
        query = query.replace('{id}', '"' + str(id) + '"')
        try:
            print(query)
            self.create(query=query)
        except:
            print ("Error: unable to fetch data")        
        return 0
    
    def fetchBy(self, table="", field="", _value="", result_value=""):
        output = 0
        query = """SELECT DISTINCT {rv} FROM {table} WHERE {col}={value}"""
        query = query.replace('{rv}', result_value)
        query = query.replace('{table}', table)
        query = query.replace('{col}', field)
        query = query.replace('{value}', '"' + _value + '"')
        try:
            tmp = self.fetch(query=query)
            if len(tmp) > 0 :
                output = 1
        except:
            output = 0      
        return output
    
    def fetch(self, query=""):
        output = None
        try:
            self.cursor.execute(query)
            output = self.cursor.fetchall()
        except:
            output = []
        return output
    
    def insert_data(self, table='', columns=[], data=[]):
        query = "INSERT INTO {:table}({:columns}) VALUES ({:values})"
        query = query.replace('{:table}', table)
        query = query.replace('{:columns}', ','.join(columns))
        query = query.replace("{:values}", ','.join(data))
        # print(query)
        return self.create(query=query)
            
