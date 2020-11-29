# File: mqtt_clock.py
# Date: 29-11-2020
# Author: Saruccio Culmone
#
# Save configured topics on a CouchDB database.

"""
Subscribes a set of configured topics and saves them into a CouchDB instance.
Adds timestamp to topic's data if not already present and always add localisation
information if present into the configuration file.
"""

import sys
import os
import time
import datetime
import argparse
import paho.mqtt.client as mqtt
import time2relax as relax
import configparser
import csv
from dataclasses import dataclass
import json
from loguru import logger



# Program name and version
PROGNAME = "archiver"
PROGDESCR = "IoT data archiver"
VERSION = "0.1.0"

# Directory list to search configuration file other than HOME dir
CONFIG_DIRS = [os.environ["HOME"], ".", "/etc"]
CONFIG_FNAME = PROGNAME + ".ini"
LOG_FNAME = PROGNAME + ".log"

def load_config():
    """
    Serarch and load configuration file.
    Returns an dictionary mapping the INI file of None in error case
    """
    logger.info("Searching for '{}' configuration file".format(CONFIG_FNAME))
    # INI dictionary
    ini = None

    for configdir in CONFIG_DIRS:
        configfilepath = os.path.join(configdir, CONFIG_FNAME)
        logger.debug("Searching for '{}'".format(configfilepath))
        if os.path.exists(configfilepath):
            logger.info("Using config file: '{}'".format(configfilepath))
            ini = configparser.ConfigParser()
            #try:
            ini.read(configfilepath)
            break;
    if ini is None:
        logger.error("Configuration file non found.")
    return ini


def verify_params(ini: dict, section: str, req_params: list):
    """
    Verify that a list of parameter is present in the section of the
    loaded INI file

    Parameters:
    ini : dict
        dictionary returned by 'load_config' function
    section : str
        section string
    req_params : list
        list of exepected parameters strings

    Return
    ------
    True if all expected parameters are present
    False otherwise
    """
    # Control if INI file contains [section] section
    if section not in ini.keys():
        logger.error("[{}] section not available".format(section))
        return False

    # Control that INI file contains all required parameters
    logger.info("Verifying section '{}'".format(section))
    log_config = ini[section]

    all_params_ok = True
    for param in req_params:
        if param not in log_config.keys():
            logger.error("Parameter '{}' not available".format(param))
            all_params_ok = False
    if not all_params_ok:
        return False

    return True

# Logging confguration
def config_logging(ini: dict):
    """
    Configure logging on file and on screen from parameters taken from
    configuration INI file.

    Parameters
    ----------
    ini : dict
        Log parameters in in the [config] section are:
        - logdir
        - log_rotation
        - log_rotation_size
        - log_retention
        - trace_level

    Return
    ------
    True if logging on file is ok
    False otherwise
    """
    # Control if INI file contains [config] section
    req_params = ['logdir', 'log_rotation', 'log_rotation_size', 'log_retention',
                  'trace_level']
    if not verify_params(ini, 'config', req_params):
        return False

    # All parameters are present, use them
    log_config = ini['config']
    logdir = log_config['logdir']
    if not os.path.exists(logdir):
        logger.error("Log directory '{}' does not exist".format(logdir))
        return False

    logger.info("Log directory: '{}'".format(logdir))

    # Setup new logger
    log_rotation = log_config['log_rotation']
    log_rotation_size = log_config['log_rotation_size']
    log_retention = log_config['log_retention']
    trace_level = log_config['trace_level']

    logfilepath = os.path.join(logdir, LOG_FNAME)
    logger.info("Logging on file: '{}'".format(logfilepath))
    logger.remove()

    try:
        if log_rotation == 'yes':
            logger.add(logfilepath, rotation=log_rotation_size,
                    retention=log_retention, level=trace_level)
        else:
            logger.add(logfilepath, level=trace_level)

        logger.info("Logging setup on file: '{}'".format(logfilepath))
        for param in req_params:
            logger.info("{}= {}".format(param, log_config[param]))
    except:
        logger.error("Logging setup on file '{}' failed".format(logfilepath))
        logger.error("Reason: {}".format(sys.exc_info()))
        return False

    return True




def load_iot_config(ini: dict):
    """

    """
    if not verify_params(ini, 'iot', ['file', 'filedir']):
        return {}

    # Set file directory
    iot_params = ini['iot']
    filedir = iot_params['filedir']
    filedir = filedir.strip(" ")
    if filedir == "":
        # HOME directory set
        filedir = os.environ['HOME']
    logger.info("IoT configuration dir: '{}'".format(filedir))

    iot_config = iot_params['file']
    filepath = os.path.join(filedir, iot_config)
    logger.info("IoT config filepath: '{}'".format(filepath))
    if not os.path.exists(filepath):
        logger.error("IoT config file '{}' not found".format(filepath))
        return {}

    # Load configuration data
    topics = dict()
    fieldnames = ["topic", "where", "h", "x", "y", "unit", "notes"]
    try:
        csvfd = open(filepath, "r", newline='')
        reader = csv.DictReader(csvfd, fieldnames=fieldnames,
                                delimiter=";")
        for row in reader:
            topic = row['topic'].strip(" ")
            if topic == 'topic':
                logger.debug("Skipping header")
                continue
            if topic[0] == "#":
                logger.warning("Line {} commented out".format(reader.line_num))
                continue
            if topic in topics.keys():
                logger.warning("Topic '{}' at line {} skipped beacause duplicated ".format(topic, reader.line_num))
                continue
            # Remove 'topic' key and assign remaining to topics dict
            row.pop('topic')
            topics[topic] = row
            logger.debug("Topic '{}' added: {}".format(topic, topics[topic]))
    except:
        logger.error("Loading IoT topics failed")
        logger.error("Reason: {}".format(sys.exc_info()))
        return {}

    return topics


# DataClass definition for data exchange between MQTT client and main
# loop
@dataclass
class MQTTInterface:
    queue: list
    topics: dict


def on_connect(client, userdata, flags, rc):
    """
    Callback function for MQTT Client
    """
    logger.info("Connected with result code " + str(rc))

    # Subscribing loop
    for topic in userdata.topics.keys():
        try:
            client.subscribe(topic)
            logger.info("Topic '{}' subscribed".format(topic))
        except:
            logger.error("Topic '{}' subscription failed".format(topic))


def on_message(client, userdata, msg):
    """
    On message receiving the corresponding Json will be pushed into a list
    to be processed later by archiver function
    """
    data = json.loads(msg.payload)
    if 'timestamp' not in data.keys():
        now = datetime.datetime.now().isoformat()
        dot = now.rfind(".")
        data['timestamp'] = now[:dot]

    # Add a unique '_id' to each message
    data["_id"] = msg.topic + "@" + data["timestamp"]
    data["topic"] = msg.topic
    tdata = (msg.topic, data)
    userdata.queue.insert(0, tdata)
    logger.debug("InQueue: {}".format(tdata))


def mqtt_client(ini: dict, mqtt_iface: MQTTInterface):
    """
    Establishes an MQTT connection with the brocker server, subscribes
    all configured topics and enqueue received messages into the MQTT
    interface queue.
    """
    # Validate mqtt configuration parameters
    if not verify_params(ini, 'mqtt', ['server', 'port', 'user', 'password',
                                      'keepalive']):
        return False

    mqtt_params = ini['mqtt']
    mqtt_server = mqtt_params['server']

    # Ensure that port and keepalive parameters are numbers
    try:
        mqtt_port = int(mqtt_params['port'])
    except:
        logger.error("MQTT port isn't an integer!")
        return False

    try:
        mqtt_keepalive = int(mqtt_params['keepalive'])
    except:
        logger.error("MQTT keepalive isn't an integer!")
        return False

    client = mqtt.Client(userdata=mqtt_iface)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(mqtt_server, mqtt_port, mqtt_keepalive)

    # Start MQTT internal loop
    client.loop_start()


def couchdb_client(ini: dict, mqtt_iface: MQTTInterface):
    """
    Connects the CouchDB server, dequeues data from MQTT interface and
    store it in the database.
    """
    # Validate mqtt configuration parameters
    if not verify_params(ini, 'couchdb', ['server', 'port', 'user', 'password',
                         'dbname']):
        return False

    # CouchDB connection
    couchdb_params = ini['couchdb']
    user = couchdb_params['user']
    passwd = couchdb_params['password']
    server = couchdb_params['server']
    port = couchdb_params['port']
    dbname = couchdb_params['dbname']
    couchdb_url = "http://{}:{}@{}:{}/{}".format(user, passwd,
                                                 server, port, dbname)
    couchdb = relax.CouchDB(couchdb_url, create_db=False)
    logger.debug("CouchDB url: '{}'".format(couchdb_url))

    # Insert loop
    while True:
        try:
            topic, data = mqtt_iface.queue.pop()
        except:
            time.sleep(5)
            continue

        try:
            couchdb.insert(data)
            logger.debug("Insert ok: '{}'".format(data))
        except:
            logger.error("Failed insert: '{}'".format(data))
            logger.error("Reason: '{}'".format(sys.exc_info()))



@logger.catch
def main():
    """
    Main apllication loop
    """


    # Argument parsing
    parser = argparse.ArgumentParser(description = PROGDESCR, prog = PROGNAME)
    parser.add_argument('-v', '--version', help='Print version and exit.',
                        action = 'version', version = VERSION)

    args = parser.parse_args()
    logger.debug("CLI arguments: '{}'".format(args))

    ini = load_config()
    if ini is None:
        return

    # Configure logging on file
    config_logging(ini)
    logger.info("---------------------------------------------------------")
    logger.info("| '{}'  START                   ".format(PROGNAME))
    logger.info("---------------------------------------------------------")

    # Load IoT configuration
    topics = load_iot_config(ini)
    if topics == {}:
        logger.error("No topics to subscribe")
        return

    # MQTT connection start
    mqtt = MQTTInterface([], topics)
    mqtt_client(ini, mqtt)

    # CouchDB
    couchdb_client(ini, mqtt)


if __name__ == "__main__":
    main()
