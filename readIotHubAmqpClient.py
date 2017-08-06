#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from proton.reactor import Container, Selector
from proton.handlers import MessagingHandler
from influxdb import InfluxDBClient

import configparser
import json
import logging
import time

# Config file must be bound to docker image at runtime
CONFIG_FILE_PATH = 'config.properties'

FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)

configp = configparser.ConfigParser()
configp.read(CONFIG_FILE_PATH)

influx_config = configp['influxdb']
DB_NAME = influx_config['DATABASE']
logging.info("Using database: '{0}'".format(DB_NAME))

influxdb_client = InfluxDBClient(influx_config['HOSTNAME'],
                                 influx_config['PORT'],
                                 influx_config['USER'],
                                 influx_config['PASSWORD'],
                                 DB_NAME)


def connect_influxdb():
    while True:
        try:
            dbs = influxdb_client.get_list_database()
            if DB_NAME not in dbs:
                influxdb_client.create_database(DB_NAME)
        except Exception:
            logger.exception("Error connecting to InfluxDB. Retrying in 30sec")
            time.sleep(30)
            continue
        else:
            logging.info("connected to influxdb")
            break


def connect_iothub(event):
    # -1 = beginning
    #  @latest = only new messages
    offset = "-1"
    selector = Selector(u"amqp.annotation.x-opt-offset > '" + offset + "'")

    azure_config = configp['azure']
    amqp_url = azure_config['IOTHUB_AMQP_URL']
    partition_name = azure_config['IOTHUB_PARTITION_NAME']

    while True:
        try:
            conn = event.container.connect(amqp_url, allowed_mechs="PLAIN")
            event.container.create_receiver(conn, partition_name + "/ConsumerGroups/$default/Partitions/0",
                                            options=selector)
            event.container.create_receiver(conn, partition_name + "/ConsumerGroups/$default/Partitions/1",
                                            options=selector)
        except Exception:
            logger.exception("Error connecting to IotHub. Retrying in 30sec")
            time.sleep(30)
            continue
        else:
            break


def write_influxdb(payload):
    while True:
        try:
            influxdb_client.write_points(payload)
        except Exception:
            logger.exception("Error writing to InfluxDB. Retrying in 30sec")
            time.sleep(30)
            continue
        else:
            break


def convert_to_influx_format(message):
    name = message.annotations["iothub-connection-device-id"]
    try:
        json_input = json.loads(message.body)
    except json.decoder.JSONDecodeError:
        return

    if 'temperature_C' not in json_input or 'humidity' not in json_input or 'battery' not in json_input:
        logging.info('Ignoring event in unknown format')
        return

    time = json_input["time"]
    temperature = json_input["temperature_C"]
    humidity = json_input["humidity"]
    battery_status = json_input["battery"]

    json_body = [
        {'measurement': name, 'time': time, 'fields': {
            "temperature": temperature,
            "humidity": humidity,
            "battery": battery_status
        }}
    ]

    return json_body


class Receiver(MessagingHandler):
    def __init__(self):
        super(Receiver, self).__init__()

    def on_start(self, event):
        connect_influxdb()
        connect_iothub(event)
        logging.info("Setup complete")

    def on_message(self, event_received):
        logging.info("Event received: '{0}'".format(event_received.message))

        payload = convert_to_influx_format(event_received.message)

        if payload is not None:
            logging.info("Write points: {0}".format(payload))
            write_influxdb(payload)

    def on_connection_closing(self, event):
        logging.error("Connection closing - trying to reestablish connection")
        connect_iothub(event)

    def on_connection_closed(self, event):
        logging.error("Connection closed")
        connect_iothub(event)

    def on_connection_error(self, event):
        logging.error("Connection error")

    def on_disconnected(self, event):
        logging.error("Disconnected")

    def on_session_closing(self, event):
        logging.error("Session closing")

    def on_session_closed(self, event):
        logging.error("Session closed")

    def on_session_error(self, event):
        logging.error("Session error")



def main():
    try:
        Container(Receiver()).run()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
