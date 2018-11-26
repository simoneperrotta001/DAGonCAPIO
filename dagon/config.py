import configparser
from collections import defaultdict
from ConfigParser import SafeConfigParser


def read_config(file="dagon.ini", section=None):
    config = SafeConfigParser()
    config.read(file)
    if section is not None:
        try:
            return dict(config.items(section))
        except:
            return None
    else:
        dictionary = defaultdict(dict)
        for section in config.sections():
            dictionary[section] = {}
            for option in config.options(section):
                # print option
                dictionary[section][option] = config.get(section, option, raw=True)
        return dictionary
