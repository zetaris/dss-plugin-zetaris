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


logging.basicConfig(level=logging.INFO, format='dss-plugin-microstrategy %(levelname)s - %(message)s')
logger = logging.getLogger()


class CustomExporter(Connector):
    """
    The methods will be called like this:
       __init__
       open
       write_row
       write_row
       write_row
       ...
       write_row
       close
    """

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
        self.RESULT_FORMAT = self.config.get("result_format")
        self.base_url = config["zetaris_api"].get("server_url", None)
        self.username = config["zetaris_api"].get("username", None)
        self.password = config["zetaris_api"].get("password", '')
        Z = ZstrSession(self.base_url, self.username, self.password)
        self.results = Z.execute_select(self.QUERY , 100)





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
                      partition_id=None, records_limit=-1 , results =None):
            yield self.results

    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                         partition_id=None):
        raise Exception("Unimplemented")


    def get_partitioning(self):
        raise Exception("Unimplemented")


    def list_partitions(self, partitioning):
        return []


    def partition_exists(self, partitioning, partition_id):
        raise Exception("unimplemented")


    def get_records_count(self, partitioning=None, partition_id=None):
        raise Exception("unimplemented")