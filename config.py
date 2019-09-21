from configparser import SafeConfigParser
import os


class Config:
    config_parser = SafeConfigParser()
    config_path = os.path.join(os.getcwd(), 'config.ini')
    config_parser.read(config_path)

    @classmethod
    def wmata_api_key(cls, bus=True):
        """Get WMATA api key from config.ini."""
        if bus:
            key = 'bus_api_key'
        else:
            key = 'train_api_key'
        return cls.config_parser.get('wmata', key)
