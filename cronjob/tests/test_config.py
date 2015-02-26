from unittest import TestCase

from app.config import Config


__author__ = 'cenk'


class ConfigTest(TestCase):
    def setUp(self):
        self.path = "sample.json"


    def test_conf_read(self):
        config = Config(self.path)
        config.load_config_from_json()
        config.parse()
        spark_master = config.get_spark_master()
        print spark_master
