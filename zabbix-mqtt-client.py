#!/usr/bin/env python3

import argparse
import json
import locale
import logging
import sys
import time

import paho.mqtt.client as mqtt
from pyzabbix import ZabbixMetric, ZabbixSender

locale.setlocale(locale.LC_ALL, "es_MX.UTF-8")
topics_keys = {}
zabbix_host = None
zbx = None

logFormatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(name)s %(message)s")

rootLogger = logging.getLogger()
rootLogger.setLevel(logging.INFO)

fileHandler = logging.FileHandler("zabbix-mqtt-client.log")
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)


def config_get(config, item):
    """
    Get config item(s) from configuration dict.

    Specific item: "item/from/config".
    Set of items: "item/from" (will return all items therein).
    """
    parts = item.split("/")

    if len(parts) == 1:
        if parts[0] in config:
            return config[parts[0]]
    elif len(parts) > 1:
        if parts[0] in config:
            if parts[1] in config[parts[0]]:
                return config[parts[0]][parts[1]]
    return None


def mqtt_on_connect(client, userdata, flags, rc):
    global topics_keys
    logging.debug("Connected with result code %s " % str(rc))

    for key in list(topics_keys):
        print("Subscribing to %s" % key)
        client.subscribe(key)


def mqtt_on_disconnect(client, userdata, rc):
    logging.debug("Disconnected with result code %s" % str(rc))


def mqtt_on_message(client, userdata, msg):
    global topics_keys, zabbix_host, zbx

    key = topics_keys[msg.topic]
    value = msg.payload.decode()
    logging.debug(msg.topic + ": " + value)
    metrics = []

    # If JSON key/value, decode and send for each key
    if type(key) == dict:
        subvalues = json.loads(value)
        for item in key:
            subkey = key[item]
            subvalue = subvalues[item]
            m = ZabbixMetric(zabbix_host, subkey, subvalue)
            metrics.append(m)
    else:
        m = ZabbixMetric(zabbix_host, key, value)
        metrics.append(m)

    # Send to Zabbix
    try:
        if zbx:
            zbx.send(metrics)
        else:
            logging.warning("Not reporting to Zabbix: %s" % key)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MQTT client for Zabbix")
    parser.add_argument("-c", "--config", default="config.json", help="config file")
    parser.add_argument("-v", dest="verbose", action="store_true", help="verbose")
    args = parser.parse_args()

    if args.verbose:
        rootLogger.setLevel(logging.DEBUG)

    try:
        config_file = open(args.config)
        config = json.load(config_file)
    except FileNotFoundError as e:
        print("Can't find %s: %s" % (args.config, str(e)))
        sys.exit(1)

    topics_keys = config_get(config, "topics")

    # Zabbix init
    if "zabbix" in config:
        _zabbix_server = config_get(config, "zabbix/server")
        _zabbix_port = config_get(config, "zabbix/port") or 10051
        _zabbix_agent_config = config_get(config, "zabbix/agent_config")
        zabbix_host = config_get(config, "zabbix/host")
        zbx = ZabbixSender(_zabbix_server, _zabbix_port, _zabbix_agent_config)

    # MQTT init
    if "mqtt" in config:
        _mqtt_username = config_get(config, "mqtt/username")
        _mqtt_password = config_get(config, "mqtt/password")
        _mqtt_server = config_get(config, "mqtt/server")
        _mqtt_port = config_get(config, "mqtt/port")
        _mqtt_tls = config_get(config, "mqtt/tls")
        client = mqtt.Client()
        if _mqtt_tls:
            print("tls")
            client.tls_set()
        client.username_pw_set(_mqtt_username, password=_mqtt_password)
        client.on_connect = mqtt_on_connect
        client.on_disconnect = mqtt_on_disconnect
        client.on_message = mqtt_on_message
        client.connect(_mqtt_server, port=_mqtt_port, keepalive=30)
        client.loop_start()

    while True:
        time.sleep(2)
