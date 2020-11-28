# File: mqtt_clock.py
# Date: 24-11-2020
# Author: Saruccio Culmone
#
# Simple MQTT clock implementation

import sys
import os
import time
import datetime
import argparse
import paho.mqtt.client as mqtt


# Name and program version
PROGNAME = "MQTT Clock"
VERSION = "1.0.0"

# Parameters
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60

# Date and time topic
DATETIME_TOPIC = "datetime"

# Environment variable enabling main loop tracing
MQTT_CLOCK_TRACE = "MQTT_CLOCK_TRACE"

def datetime_formatted():
    """
    Put now() string in italian format
    """
    data_str = str(datetime.datetime.now())
    dotpos = data_str.rfind(".")
    sdate, stime = data_str[:dotpos].split(" ")
    Y, M, D = sdate.split("-")
    return D+"-"+M+"-"+Y+"\n"+stime

def main():
    """
    Main function
    """
    # Argument parsing
    parser = argparse.ArgumentParser(description = "Simple MQTT published clock",
                                     prog = PROGNAME)
    parser.add_argument('-v', '--version', help='Print version and exit.',
                        action = 'version', version = VERSION)
    parser.add_argument('server', help = "MQTT broker address")
    parser.add_argument('-p', '--port',
                        help = "MQTT broker port (default {})".format(MQTT_PORT),
                        type = int, default = MQTT_PORT)
    parser.add_argument('-k', '--keepalive',
                        help = "MQTT keepalive (default {})".format(MQTT_KEEPALIVE),
                        type = int,
                        default = MQTT_KEEPALIVE)

    args = parser.parse_args()

    # Control whether tracing is enabled or not
    tracing_enabled = False
    if  MQTT_CLOCK_TRACE in os.environ.keys():
        # Don't care about the actual value
        tracing_enabled = True
        print("Tracing enabled variable '{}' is defined".format(MQTT_CLOCK_TRACE))

    # Setup MQTT connection
    client = mqtt.Client()
    try:
        client.connect(args.server, args.port, args.keepalive)
        print("MQTT client connected: '{}:{}' keepalive= {}".format(args.server, args.port, args.keepalive))
    except:
        print("ERROR - Cannot connect '{}:{}' keepalive= {}".format(args.server, args.port, args.keepalive))
        print("Reason: {}".format(sys.exc_info()))
        sys.exit(1)

    client.loop_start()

    # data topic
    data_topic = DATETIME_TOPIC
    while True:
        data_str = datetime_formatted()
        if tracing_enabled:
            print("{}".format(data_str))
        client.publish(data_topic, payload=data_str, qos=0, retain=False)
        time.sleep(1)

if __name__ == "__main__":
    main()
