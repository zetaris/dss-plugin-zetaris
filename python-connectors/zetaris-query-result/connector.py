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
from dataiku.connector import Connector ,CustomDatasetWriter

import json

from collections import OrderedDict

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
#        print(self.results) 





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



    def get_read_schema(self):
        # The Zetaris connector does not have a fixed schema, since each
        # sheet has its own (varying) schema.
        #
        # Better let DSS handle this
        return None


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        """
        The main reading method.

        Returns a generator over the rows of the dataset (or partition)
        Each yielded row must be a dictionary, indexed by column name.

        The dataset schema and partitioning are given for information purpose.
        """

        rows = self.results
        print(rows)
        try:
            columns = rows[0]
        except IndexError as e:
            columns = []


            for row in rows:
                yield OrderedDict(zip(range(1, len(columns) + 1), row))




    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                         partition_id=None):
        if self.result_format == 'json':

            raise Exception('JSON format not supported in write mode')

        return MyCustomDatasetWriter(self.config, self, dataset_schema, dataset_partitioning, partition_id)


 

class MyCustomDatasetWriter(CustomDatasetWriter):
    def __init__(self, config, parent, dataset_schema, dataset_partitioning, partition_id):
        CustomDatasetWriter.__init__(self)
        self.parent = parent
        self.config = config
        self.dataset_schema = dataset_schema
        self.dataset_partitioning = dataset_partitioning
        self.partition_id = partition_id

        self.buffer = []

#        columns = [col["name"] for col in dataset_schema["columns"]]

#        if parent.result_format == 'first-row-header':
#            self.buffer.append(columns)


    def write_row(self, row):

        # Example of dataset_schema: {u'userModified': False, u'columns': [{u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'condition', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'weather', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'double', u'name': u'temperature', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'bigint', u'name': u'humidity', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'date', u'name': u'date_update', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'date', u'name': u'date_add', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'ville', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'source', u'maxLength': -1}]}
        # for (col, val) in zip(self.dataset_schema["columns"], row):
        #     print (col, val)

        self.buffer.append(row)
        print(self.buffer)

        # if len(self.buffer) > 50:
        #     self.flush()
