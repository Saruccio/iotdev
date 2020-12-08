# File: historicizer.py
# Date: 06-12-2020
# Author: Saruccio Culmone
#
# Save topics data into database of historical data

"""
The historicizer process read data from realtime database and according
with some policy saves post-processed data into a database of historical
sequences.
"""

import sys
import os
import time
import datetime
import argparse
import time2relax as relax
import configparser
import csv
from dataclasses import dataclass
import json
from loguru import logger
import configuration as config
import uncertainties as uncert
import statistics as stats
import copy


# Program name and version
PROGNAME = "historicizer"
PROGDESCR = "Measurement data archiver"
VERSION = "0.1.0"


# DataClass definition storing reading from and writing to database
@dataclass
class Databases:
    db_realtime: relax.CouchDB
    db_datastore: relax.CouchDB
    db_devices: relax.CouchDB


def connect_db(ini: dict, db_name):
    """
    Connects to a DB returning its instance or None in error case
    """
    # Validate mqtt configuration parameters
    if not config.verify_params(ini, 'couchdb',
                         ['server', 'port', 'user', 'password',
                         'readfom_dbname', 'writeto_dbname',
                         'devices_dbname']):
        return None

    # CouchDB connection
    couchdb_params = ini['couchdb']
    user = couchdb_params['user']
    passwd = couchdb_params['password']
    server = couchdb_params['server']
    port = couchdb_params['port']
    dbname = couchdb_params[db_name]

    try:
        couchdburl = "http://{}:{}@{}:{}/{}".format(user,
                                                    passwd,
                                                    server,
                                                    port,
                                                    dbname)
        db = relax.CouchDB(couchdburl, create_db=False)
        logger.debug("CouchDB url: '{}'".format(couchdburl))
    except:
        logger.error("Failed CouchDB connection to: '{}'".format(couchdburl))
        logger.error("Reason: '{}'".format(sys.exc_info()))
        return None

    logger.info("Connected CouchDB url: '{}'".format(couchdburl))
    return db


def couchdb_client(ini: dict):
    """
    Connects the CouchDB server and returns a tuple of two database object
    one for reading from and a second to reading to.
    None in error case
    """
    # Init return struct
    dbs = Databases(None, None, None)

    # Read from DB connection
    dbs.db_realtime = connect_db(ini, "realtime_dbname")

    # Write to DB connection
    dbs.db_datastore = connect_db(ini, "datastore_dbname")

    # Open devices DB
    dbs.db_devices = connect_db(ini, "devices_dbname")

    if ((dbs.db_realtime is None) or
       (dbs.db_datastore is None) or
       (dbs.db_devices is None)):
        logger.error("Incomplete connection to databases")
        return None

    logger.info("Connetcted to dbs: '{}', '{}', '{}'".format(dbs.db_realtime,
                                                             dbs.db_datastore,
                                                             dbs.db_devices))
    return dbs


def get_topic_list(dbs: Databases):
    """
    Curl command:
    curl $STUARTDB/_design/counters/_view/topic_list -G -d 'group=true'

    Return
    ------
    A list of topics or an empty list in case of error of any kind.
    """
    raw_query = "curl $STUARTDB/_design/counters/_view/topic_list -G -d 'group=true'"

    params = {'group': True}
    try:
        result = dbs.db_realtime.ddoc_view('counters', 'topic_list',
                                       params=params)
        data = result.json()
    except:
        logger.error("Failed query for topic list")
        logger.error("Reason: {}".format(sys.exc_info()))
        return []

    topics = [row['key'] for row in data['rows']]
    return topics



def get_first_doc(dbs: Databases, topic: str):
    params = {'group': False,
              'reduce': False,
              'startkey': [topic],
              'limit': 1}
    result = None
    try:
        result = dbs.db_realtime.ddoc_view('sequences', 'by_topic_no_reduce', params=params)
    except:
        logger.error("Failed query for {} doc".format(topic))
        logger.error("Reason: {}".format(sys.exc_info()))
        return None

    data = result.json()
    logger.debug("data= {}".format(data))
    rows = []
    try:
        rows = data['rows']
    except:
        logger.error("Result has no rows")
        return None

    if rows == []:
        logger.error("Result has no rows")
        return None

    doc = rows[0]
    key = doc['key']
    timestamp = key[1]
    return datetime.datetime.fromisoformat(timestamp)


def get_measures_slot(dbs: Databases, topic: str, timespan: int):
    """

    Return
    ------
    a list of measures found in the timespan
    None if upper limit of the time window is reached
    """
    # Fetch first doc inserted
    start_timestamp = get_first_doc(dbs, topic)

    # End timestamp
    end_timestamp = start_timestamp + datetime.timedelta(minutes=timespan)

    now = datetime.datetime.now()
    if end_timestamp >= now:
        logger.warning("Less than {} min of measures available".format(timespan))
        logger.debug("end= '{}', now= '{}'".format(end_timestamp, now))
        return None

    # Query for measures
    params = {'group': False,
              'include_docs': True,
              'reduce': False,
              'startkey': [topic, start_timestamp.isoformat()],
              'endkey': [topic, end_timestamp.isoformat()]}
    result = None
    try:
        result = dbs.db_realtime.ddoc_view('sequences', 'by_topic_no_reduce', params=params)
    except:
        logger.error("Failed query for {} doc".format(topic))
        logger.error("Reason: {}".format(sys.exc_info()))
        return None

    data = result.json()
    #logger.debug("data= {}".format(data))
    rows = []
    try:
        rows = data['rows']
    except:
        logger.error("Result has no rows")
        return None

    logger.info("Query returned '{}' rows".format(len(rows)))
    return rows


def get_device(dbs: Databases, device: str):
    """
    Returns record information for a device or None in error case
    """
    try:
        result = dbs.db_devices.get(device)
    except:
        logger.error("Device GET '{}'".format(device))
        logger.error("Reason: '{}'".format(sys.exc_info()))
        return None

    dev_data = result.json()
    return dev_data


def accuracy(device: dict, value_type: str, reading: float):
    """
    Returns the accuracy of the reading depending on device type accuracy
    ranges
    """
    if device is None:
        return 0.0

    # Get accuracy ranges
    try:
        accuracies = device[value_type]['accuracy']
    except:
        logger.error("Device has no accuracy data for '{}' values".format(value_type))
        return 0.0

    accuracy_value = 0.0
    for accuracy in accuracies:
        range_inf = accuracy['range_inf']
        range_sup = accuracy['range_sup']
        if (reading >= range_inf) and (reading < range_sup):
            accuracy_value = accuracy['value']
            break
    return accuracy_value



def process_series(dbs: Databases, topic: str, data: list):
    """
    Process document series and returns documento to be stored
    """
    # Devices
    devices = dict()

    # Min and Max values in the series
    min_value = None
    min_timestamp = ""
    max_value = None
    max_timestamp = ""

    data_values = []

    # It is supposed all values are of the same type
    measure_type = None

    first_timestamp = ""
    last_timestamp = ""
    timestamps = []

    for dt in data:
        value = dt['value']
        doc = dt['doc']
        device = doc['dev']
        if device not in devices.keys():
            devices[device] = get_device(dbs, device)

        if measure_type is None:
            measure_type = doc['type']

        data_val = (value, device)
        data_values.append(data_val)

        timestamp = doc['timestamp']
        timestamps.append(timestamp)

        # Min and Max values evaluation
        if min_value is None:
            min_value = value
            min_timestamp = timestamp
        else:
            if value < min_value:
                min_value = value
                min_timestamp = timestamp
        if max_value is None:
            max_value = value
            max_timestamp = timestamp
        else:
            if value > max_value:
                max_value = value
                max_timestamp = timestamp

    # Extract time boundaries
    first_timestamp = min(timestamps)
    last_timestamp = max(timestamps)

    logger.debug("Slot boundaries: {} -- {}".format(first_timestamp, last_timestamp))
    logger.debug("Min value= {} at {}".format(min_value, min_timestamp))
    logger.debug("Max value= {} at {}".format(max_value, max_timestamp))

    # Calculate mean value and standard deviation using device accuracy
    values = [dval[0] for dval in data_values]
    #logger.debug("Values= {}".format(values))
    mean_value = stats.mean(values)
    stddev_value = stats.stdev(values)
    logger.debug("Mean value= {}+/-{}".format(mean_value, stddev_value))

    # Calculate mean value using device accuracy
    uvalues = []
    for value, device in data_values:
        dev = devices[device]
        acc = accuracy(dev, measure_type, value)
        uvalues.append(uncert.ufloat(value, acc))

    uaverage = sum(uvalues)/len(uvalues)
    logger.debug("Mean value with accuracy: {}".format(uaverage))

    # Compose measure json struct ready to be inserted
    meas = dict()
    meas['topic'] = topic
    meas['measure_type'] = measure_type
    meas['value_type'] = "average"

    avg_timestamp = ((datetime.datetime.fromisoformat(last_timestamp) -
                     datetime.datetime.fromisoformat(first_timestamp) )/2.0 +
                     datetime.datetime.fromisoformat(first_timestamp))
    measure_timestamp = avg_timestamp.isoformat(timespec='seconds')
    meas['timestamp'] = measure_timestamp
    meas['value'] = uaverage.nominal_value
    meas['accuracy'] = uaverage.std_dev
    meas['min_value'] = {'value': min_value, 'timestamp': min_timestamp}
    meas['max_value'] = {'value': max_value, 'timestamp': max_timestamp}
    meas['time_slot'] = {'start': first_timestamp, 'end': last_timestamp}

    # Add '_id' composed as '<topic>@<timestamp>'
    meas['_id'] = topic + "@" + measure_timestamp
    logger.debug("Calculated measure: {}".format(meas))
    return meas



def archive_series(dbs: Databases, topic: str, timespan: int):
    """
    """
    rows = get_measures_slot(dbs, topic, timespan)
    #logger.debug(rows)
    if rows is None:
        logger.warning("No data to move")
        return False

    # Calculate value
    calc_meas = process_series(dbs, topic, rows)
    logger.info("Moving {} timeslot {}".format(calc_meas['_id'], calc_meas['time_slot']))

    # Insert value into the DB
    try:
        dbs.db_datastore.insert(calc_meas)
    except:
        logger.error("Failed inserting measure: '{}'".format(calc_meas))
        logger.error("Reason: {}".format(sys.exc_info()))
    logger.debug("Inserted measure: '{}'".format(calc_meas))

    # Delete measures fron reltime database
    for row in rows:
        id = row['id']
        result = dbs.db_realtime.get(id).json()
        dbs.db_realtime.remove(result['_id'], result['_rev'])
    return True

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

    ini = config.load_config(PROGNAME)
    if ini is None:
        return

    # Configure logging on file
    config.config_logging(ini, PROGNAME)
    logger.info("---------------------------------------------------------")
    logger.info("| '{}'  START                   ".format(PROGNAME))
    logger.info("---------------------------------------------------------")

    # Load IoT configuration
    topics = config.load_iot_config(ini)
    if topics == {}:
        logger.error("No topics to subscribe")
        return

    # Connects to databases
    dbs = couchdb_client(ini)
    if dbs == None:
        logger.error("No DB available")
        return

    # Get the list of topic availables
    topics = get_topic_list(dbs)
    logger.info("Available topics: '{}'".format(topics))

    for topic in topics:
        # Archives topic's dataset 5 minutes at time
        data_available = True
        while data_available:
            data_available = archive_series(dbs, topic, 10)


if __name__ == "__main__":
    main()
