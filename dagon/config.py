
import configparser

def read_config(section):

    config = configparser.ConfigParser()
    config.read('dagon.ini')
    try:
        return dict(config.items(section))
    except:
        return None
