import yaml
import os
import json

from libs.internal_logging import InternalLogging

class ConfigGetter:

    def __init__(self):
        self.log = InternalLogging()
        self.workdir =  os.environ['WORK_DIR']
        self.read_config()

    def read_config(self):
        """
            membuat config dengan membaca file dari ./config/config.yaml
            supaya jika ada perubahan, perubahan dilakukan melalui file tersebut
        """
        
        config_file_path = "{workdir}/config/config.yaml".format(workdir=self.workdir)
        self.log.logging_info(message=f"read data from {config_file_path}")

        with open(config_file_path, "r") as cfg_file:
            self.config = yaml.safe_load(cfg_file)

    def get_bmkg_weather_propinsi(self):
        self.log.logging_info(message=f'get whitelist propinsi {self.config["bmkg_weather"]["whitelist_propinsi"]}')
        return self.config["bmkg_weather"]["whitelist_propinsi"]

    def get_bmkg_weather_api_base_url(self):
        return self.config["bmkg_weather"]["api_base_url"]