# This file is the actual code for the custom Python dataset dss-plugin-zetaris_create-dataset

# import the base class for the custom dataset

"""
A custom Python dataset is a subclass of Connector.

The parameters it expects and some flags to control its handling by DSS are
specified in the connector.json file.

Note: the name of the class itself is not relevant.
"""
import logging
from numpy import isnan
from zstr_session import ZstrSession

import dataiku
from dataiku.connector import Connector

import json

from collections import OrderedDict

logging.basicConfig(level=logging.INFO, format='dss-plugin-zetaris %(levelname)s - %(message)s')
logger = logging.getLogger()


class CustomExporter(Connector):
    def __init__(self, config):
        """
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
                "self.project_id, self.folder_id = self.get_ui_browse_results(config)
        """
        Connector.__init__(self, config)
        self.row_buffer = []
        self.buffer_size = 5000
        logger.info("Starting Zetaris exporter v1.1.0")
        # Plugin settings
        self.QUERY = self.config.get("query", "")
        if self.QUERY.split(' ', 1)[0] != "SELECT" :
            raise ValueError("This connector you are using only supports SELECT queries. Unfortunately , the query you have attempted is not supported by the connector.")
        self.RESULT_FORMAT = self.config.get("result_format")
        self.base_url = config["zetaris_api"].get("server_url", None)
        self.username = config["zetaris_api"].get("username", None)
        self.password = config["zetaris_api"].get("password", '')
        self.pageLimit =config["zetaris_api"].get("pageLimit", 50)
        Z = ZstrSession(self.base_url, self.username, self.password)
        self.results = Z.execute_select(self.QUERY, self.pageLimit) 

        if not (self.username and self.base_url):
            logger.error('Connection params: {}'.format(
                {
                    'username:': self.username,
                    'password:': '#' * len(self.password),
                    'base_url:': self.base_url
                })
            )
            raise ValueError("username and base_url must be filled")

    def get_read_schema(self):
        return None


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                      partition_id=None, records_limit=-1,results=None):

        if results is None:
            results = self.results
            processed_data = []
            for row in results:
                processed_row = {}
                for key, value in row.items():
                    processed_value = value
                    processed_row[key] = processed_value
                processed_data.append(processed_row)
            return processed_data




    def get_partitioning(self):
        raise Exception("Unimplemented")


    def list_partitions(self, partitioning):
        return []


    def partition_exists(self, partitioning, partition_id):
        raise Exception("unimplemented")


    def get_records_count(self, partitioning=None, partition_id=None):
        raise Exception("unimplemented")


    def write_row(self, row):

        # Example of dataset_schema: {u'userModified': False, u'columns': [{u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'condition', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'weather', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'double', u'name': u'temperature', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'bigint', u'name': u'humidity', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'date', u'name': u'date_update', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'date', u'name': u'date_add', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'ville', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'source', u'maxLength': -1}]}
        # for (col, val) in zip(self.dataset_schema["columns"], row):
        #     print (col, val)

        self.buffer.append(row)
