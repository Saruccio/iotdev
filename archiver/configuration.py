# File: configuration.py
# Date: 06-12-2020
# Author: Saruccio Culmone
#
# Common functions for configuration reading
"""
Common functions for configuration reading
"""

import sys
import os
import time
import argparse
import configparser
import csv
from loguru import logger


# Directory list to search configuration file other than HOME dir
CONFIG_DIRS = [os.environ["HOME"], ".", "/etc"]


def load_config(progname: str):
    """
    Serarch and load configuration file.
    Returns an dictionary mapping the INI file of None in error case
    """
    config_fname = progname + ".ini"
    logger.info("Searching for '{}' configuration file".format(config_fname))
    # INI dictionary
    ini = None

    for configdir in CONFIG_DIRS:
        configfilepath = os.path.join(configdir, config_fname)
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
def config_logging(ini: dict, progname: str):
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

    progname : str
        name of the program calling this function

    Return
    ------
    True if logging on file is ok
    False otherwise
    """
    # Control if INI file contains [config] section
    req_params = ['logdir', 'log_rotation', 'log_rotation_size',
                  'log_retention', 'trace_level', 'console_log']
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
    console_log = log_config['console_log']

    log_fname = progname + ".log"
    logfilepath = os.path.join(logdir, log_fname)
    logger.info("Logging on file: '{}'".format(logfilepath))

    if console_log == 'no':
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
