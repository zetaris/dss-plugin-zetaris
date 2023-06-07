#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  2 10:35:05 2023

@author: rainathomas
"""

import requests
import logging
from base64 import b64encode
import uuid
import json
logging.basicConfig(level=logging.INFO, format='dss-plugin-zetaris %(levelname)s - %(message)s')
logger = logging.getLogger()

class ZstrAuth():
    def __init__( self, server_url, username, password ):
        self.server_url = server_url
        self.username = username
        self.password = password
        self.login( username , password, server_url )
        
    def get_auth(self,token:str=None):
        if token == None:
            global bearer_token
            token = bearer_token
        return {
                "Authorization": token,
                "X-Request-ID": str(uuid.uuid1())
                }
    
    def basic_auth(self,username:str,password:str):
        token = b64encode(f"{username}:{password}".encode('utf-8')).decode("ascii")
        return f'Basic {token}'
    
    def login(self,username:str,password:str,server_url:str):
        global bearer_token
        global refresh_token
        global api_url
        api_url = server_url
        auth=self.get_auth(self.basic_auth(username, password))
        response = requests.get(url=f'{api_url}/api/v1.0/auth/login',headers=auth)
        response = json.loads(response.text)
        bearer_token = f"Bearer {response['idToken']}"
        refresh_token = f"Bearer {response['refreshToken']}"
        return bearer_token
    
    def refresh_tokens(self):
        global bearer_token
        global refresh_token
        auth = get_auth(refresh_token)
        response = requests.get(url=f'{api_url}/api/v1.0/auth/refresh',headers=auth)
        response = json.loads(response.text)
        bearer_token = f"Bearer {response['idToken']}"
        refresh_token = f"Bearer {response['refreshToken']}"
        status_code = response.status_code
        if status_code >= 400:
                error_message = "Error {} while requesting auth token to {}".format(status_code, api_url)
                logger.error(error_message)
                logger.error("dumping: {}".format(response.content))
                raise Exception(error_message)