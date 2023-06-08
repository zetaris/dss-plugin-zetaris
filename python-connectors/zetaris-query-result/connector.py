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
from zstr_session import ZstrSession, get_base_url

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
        self.upload_session_id = None
        print(self.QUERY)
        Z = ZstrSession(self.base_url, self.username, self.password)
        results = Z.execute_select(self.QUERY , 100)



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

        if self.RESULT_FORMAT == 'json':
            return {
                "columns": [
                    {"name": "json", "type": "object"}
                ]
            }

        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                      partition_id=None, records_limit=-1):

        results = self.results

        log("records_limit: %i" % records_limit)
        log("length initial request: %i" % len(results.get('records')))

        n = 0

        for obj in results.get('records'):
            n = n + 1
            if records_limit < 0 or n <= records_limit:
                yield self._format_row_for_dss(obj)


        while next:
            results = self.client.make_api_call(next)
            for obj in results.get('records'):
                n = n + 1
                if records_limit < 0 or n <= records_limit:
                    yield self._format_row_for_dss(obj)


    def _format_row_for_dss(self, row):
        if self.RESULT_FORMAT == 'json':
            return {"json": json.dumps(row)}
        else:
            return unnest_json(row)