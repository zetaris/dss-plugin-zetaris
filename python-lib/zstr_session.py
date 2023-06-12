#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  2 10:33:33 2023

@author: rainathomas
"""
#This module will be used to establish a session for every Zetaris API call. To start with we will be using the execute_select function only and the code will be expanded eventually to apply the remaining modules

import requests
import json
import csv
from zstr_auth import ZstrAuth
import pandas as pd


class ZstrSession(object):
    def __init__( self, server_url, username, password ):
        if not server_url:
            raise Exception("Invalid Zetaris URL or incorrect SQL query has been provided")
        self.server_url = server_url
        self.username = username
        self.password = password
        self.auth = None
        self.auth = ZstrAuth(server_url, username, password)
        
    def get_pipeline_containers(self):
        auth = self.auth.get_auth()
        response = requests.get(url=f'{self.server_url}/api/v1.0/pipeline/containers',headers=auth)
        response = json.loads(response.text)
        return response
    
    def get_pipeline_relations(self, pipeline_container_id:int):
      auth = self.auth.get_auth()
      response = requests.get(url=f'{self.server_url}/api/v1.0/pipeline/containers/{pipeline_container_id}/relations',headers=auth)
      response = json.loads(response.text)
      return response
    
    def execute_pipeline(self , pipeline_container_id:int, pipeline_relation_id:int, pageLimit:int):
      auth = self.auth.get_auth()
      body = json.dumps({
        "pageLimit": pageLimit
      })
      response = requests.post(url=f'{self.server_url}/api/v1.0/pipeline/containers/{pipeline_container_id}/relations/{pipeline_relation_id}/execute',headers=auth,data=body)
      print(response.text)
      response = json.loads(response.text)
      return response
    
    def open_sql_query(self, sql_query:str, pageLimit:int):
      auth = self.auth.get_auth()
      body = json.dumps({
        "select": sql_query,
        "pageLimit": pageLimit
      })
      response = requests.post(url=f'{self.server_url}/api/v1.0/query/sql/start',headers=auth,data=body)
      response = json.loads(response.text)
      return response
    
    def register_table(self , datasource_name:str, table_name:str):
      auth = self.auth.get_auth()
      body = json.dumps({
        "dataSourceName": datasource_name,
          "tableName": table_name
      })
      response = requests.post(url=f'{self.server_url}/api/v1.0/query/sql/register-table',headers=auth,data=body)
      response = json.loads(response.text)
      return response
    
    def create_schema_store_views(self, view_name:str, select:str):
      auth = self.auth.get_auth()
      body = json.dumps({
        "viewName": view_name,
          "select": select
      })
      response = requests.post(url=f'{self.server_url}/api/v1.0/view/schema-store-views',headers=auth,data=body)
      response = json.loads(response.text)
      return response
    
    def generic_query(self , sql_query:str):
      auth = self.auth.get_auth()
      body = json.dumps({
        "sql": sql_query
      })
      response = requests.post(url=f'{self.server_url}/api/v1.0/query/sql/generic-query',headers=auth,data=body)
      response = json.loads(response.text)
      return response
    
    def page_sql_query(self, queryToken:str, pageLimit:int, pageNumber:int):
      auth = self.auth.get_auth()
      params = {
        "queryToken": queryToken,
        "pageLimit": pageLimit,
        "pageNumber": pageNumber
      }
      response = requests.get(url=f'{self.server_url}/api/v1.0/query/sql/page',headers=auth,params=params)
      response = json.loads(response.text)
      return response
    
    def close_sql_query(self , query_token:str):
      auth = self.auth.get_auth()
      requests.delete(url=f'{self.server_url}/api/v1.0/query/sql/close/{query_token}',headers=auth)
    
    def clear_query_cache(self , username:str):
      auth = self.auth.get_auth()
      requests.delete(url=f'{self.server_url}/api/v1.0/query/sql/clear/{username}',headers=auth)
    
    def save_results(self, data, file_path, file_name):
        print(file_path)
        print(file_name)
        file_full_path = file_path + file_name
        try:
            #with open(file_full_path, 'w', newline='') as csv_file:
                #writer = csv.writer(csv_file)
                #writer.writerows(data)
            df = pd.DataFrame(data)
            df.to_csv(file_full_path,index=False)
            print(f"Data saved successfully to {file_full_path}.")
        except Exception as e:
            print(f"Error occurred while saving data: {str(e)}") 
    
    def execute_select(self,query_param:str, pageLimit:int):
        try:
            pageLimit = pagelimit
            x = self.open_sql_query(query_param,pageLimit)
            data = x['records']
            query_token = x['queryToken']
            max_pages = x['totalPages']
            for i in range(2,max_pages+1):
                page_data=self.page_sql_query(query_token,pageLimit,i)
                data += page_data['records']
            return(data)
        except Exception as e:
            return(e)
            



