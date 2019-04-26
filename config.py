
import configparser
import logging

from pathlib import Path


filename = 'decuma.ini'
root_dir = Path('data/')
host = '127.0.0.1'
port = 6666
max_segment_size = 1000
max_segments_in_memory = 1000
max_clients = 10
logging_level = logging.INFO
patience = 0.1


def load(filename):
    global root_dir
    global host
    global port
    global max_segment_size
    global max_segments_in_memory
    global max_clients
    global logging_level
    global patience

    config = configparser.ConfigParser()
    config.read(filename)

    root_dir = Path(config['DEFAULT']['path'])
    host = '127.0.0.1'
    port = int(config['DEFAULT']['port'])
    max_segment_size = int(config['DEFAULT']['max_segment_size'])
    max_segments_in_memory = int(config['DEFAULT']['max_segments_in_memory'])
    max_clients = int(config['DEFAULT']['max_clients'])
    logging_level = config['DEFAULT']['logging_level']
    patience = float(config['DEFAULT']['patience'])

    logging.basicConfig(format='%(asctime)s | %(levelname)s \t%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging_level)
