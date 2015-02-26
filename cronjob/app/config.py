__author__ = 'cenk'

import json


class Config():
    def __init__(self, path=None):
        self.path = path
        self.data = {}

        self.cassandra_conf = None
        self.elastic_conf = None
        self.spark_conf = None
        self.rollups = None

        self.load_config_from_json()
        self.parse()


    def load_config_from_json(self):
        try:
            with open(self.path) as data_file:
                self.data = json.load(data_file)
            data_file.close()
        except:
            raise

    def parse(self):
        self.spark_conf = self.data.get("spark")
        self.cassandra_conf = self.data.get("cassandra")
        self.elastic = self.data.get("elasticsearch")
        self.rollups = self.data.get("rollups")

    def get_spark_master(self):
        return self.spark_conf.get("master")

    def get_cassandra_host(self):
        return self.cassandra_conf.get("host")

    def get_cassandra_keyspace(self):
        return self.cassandra_conf.get("keyspace")

    def get_cassandra_table(self):
        return self.cassandra_conf.get("table")

    def get_elastic_host(self):
        return self.elastic_conf.get("host")

    def get_elastic_index(self):
        return self.elastic_conf.get("index")

    def get_elastic_port(self):
        return self.elastic_conf.get("port")
