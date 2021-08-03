import os
from configparser import ConfigParser
import boto3

ROOT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__))
)
CONFIG_FILE = os.path.join(ROOT_DIR, 'config.ini')


def config(section: str) -> dict:
    """Returns parameters for given section of the config.in file."""
    parser = ConfigParser()
    parser.read(CONFIG_FILE)

    # get section parameters
    opts = dict()
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            opts[param[0]] = param[1]
    else:
        raise ValueError(
            f'Section {section} not found in {CONFIG_FILE}')
    return opts


def get_aws_session() -> boto3.Session:
    """Return aws session object."""
    aws_config = config(section='aws')
    return boto3.Session(**aws_config)
