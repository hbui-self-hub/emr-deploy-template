import os
import yaml
from dotenv import load_dotenv

class Config(object):

    _config = {}
    def __init__(self, *files):

        load_dotenv()
        self.env = os.getenv('EMR_ENVIRONMENT')
        self.app_path = os.getenv('EMR_APP_PATH')
        self.path = os.getenv('EMR_CONFIG_PATH')

        for config_file in files:
            config_path = os.path.join(self.path, config_file + '.yml')
            self.__extractConfig(config_path)

    def __extractConfig(self, path):
        with open(path) as stream:
            try:
                c = yaml.safe_load(stream)
                c = c[self.env]
            except:
                pass

        for key in c.keys():
            self._config[key] = c[key]
    
    def __generateConfigFile(self, filepath):
        with open(filepath, 'w') as f:
            f.write("conf={}".format(str(self._config)))

    
    def appendConfig(self, *files):
        for config_file in files:
            config_path = os.path.join(self.path, config_file + '.yml')
            self.__extractConfig(config_path)

    def get(self, key):
        if key in self._config.keys():
            return self._config.get(key)
        return None
    
    def generate(self):
        self.__generateConfigFile("../app/modules/commons/config.py")
        self.__generateConfigFile("../clusters/config.py")

conf = Config("app", "aws")
conf.generate()
