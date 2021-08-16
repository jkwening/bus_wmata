from configparser import ConfigParser
import os


class Config:
    """Config.ini class."""
    config_parser = ConfigParser()
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

    @classmethod
    def postgres(cls):
        """Returns user, pass, and database name."""
        return {
            'user': cls.config_parser.get('postgres', 'user'),
            'password': cls.config_parser.get('postgres', 'password'),
            'dbname': cls.config_parser.get('postgres', 'dbname')
        }
