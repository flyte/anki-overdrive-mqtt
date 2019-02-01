import argparse
import logging
import yaml
import sys
import socket
import json
import re
from time import sleep
from functools import partial

import paho.mqtt.client as mqtt
import cerberus
from overdrive import Overdrive


CONFIG_SCHEMA = yaml.load(open('config.schema.yml'))

LOG_LEVEL_MAP = {
    mqtt.MQTT_LOG_INFO: logging.INFO,
    mqtt.MQTT_LOG_NOTICE: logging.INFO,
    mqtt.MQTT_LOG_WARNING: logging.WARNING,
    mqtt.MQTT_LOG_ERR: logging.ERROR,
    mqtt.MQTT_LOG_DEBUG: logging.DEBUG
}
RECONNECT_DELAY_SECS = 5

_LOG = logging.getLogger(__name__)
_LOG.addHandler(logging.StreamHandler())
_LOG.setLevel(logging.DEBUG)


class ConfigValidator(cerberus.Validator):
    """
    Cerberus Validator containing function(s) for use with validating or
    coercing values relevant to the md_mqtt project.
    """

    @staticmethod
    def _normalize_coerce_rstrip_slash(value):
        """
        Strip forward slashes from the end of the string.
        :param value: String to strip forward slashes from
        :type value: str
        :return: String without forward slashes on the end
        :rtype: str
        """
        return value.rstrip("/")

    @staticmethod
    def _normalize_coerce_tostring(value):
        """
        Convert value to string.
        :param value: Value to convert
        :return: Value represented as a string.
        :rtype: str
        """
        return str(value)


def on_log(client, userdata, level, buf):
    """
    Called when MQTT client wishes to log something.
    :param client: MQTT client instance
    :param userdata: Any user data set in the client
    :param level: MQTT log level
    :param buf: The log message buffer
    :return: None
    :rtype: NoneType
    """
    _LOG.log(LOG_LEVEL_MAP[level], "MQTT client: %s" % buf)


def init_mqtt(config):
    """
    Configure MQTT client.
    :param config: Validated config dict containing MQTT connection details
    :type config: dict
    :return: Connected and initialised MQTT client
    :rtype: paho.mqtt.client.Client
    """
    topic_prefix = config["topic_prefix"]
    protocol = mqtt.MQTTv311
    if config["protocol"] == "3.1":
        protocol = mqtt.MQTTv31
    client = mqtt.Client(protocol=protocol, clean_session=True)

    if config["user"] and config["password"]:
        client.username_pw_set(config["user"], config["password"])

    # Set last will and testament (LWT)
    status_topic = "%s/%s" % (topic_prefix, config["status_topic"])
    client.will_set(
        status_topic,
        payload=config["status_payload_dead"],
        qos=1,
        retain=True)
    _LOG.debug(
        "Last will set on %r as %r.",
        status_topic,
        config["status_payload_dead"])

    client.on_log = on_log

    return client


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("config")
    args = p.parse_args()

    with open(args.config) as f:
        config = yaml.load(f)
    validator = ConfigValidator(CONFIG_SCHEMA)
    if not validator.validate(config):
        _LOG.error(
            "Config did not validate:\n%s",
            yaml.dump(validator.errors))
        sys.exit(1)
    config = validator.normalized(config)

    client = init_mqtt(config["mqtt"])

    topic_prefix = config["mqtt"]["topic_prefix"]
    vehicle_config_by_mac = {x['mac'].lower(): x for x in config['anki']['vehicles']}

    def publish_params(topic, addr, params=None):
        veh = vehicle_config_by_mac[addr.lower()]
        client.publish(
            '%s/%s/%s' % (topic_prefix, veh['name'], topic),
            json.dumps(params) if params is not None else None,
            qos=2)

    vehicles = {}
    vehicles_by_name = {}
    loc_change_callback = partial(publish_params, 'location')
    transition_callback = partial(publish_params, 'transition')
    pong_callback = partial(publish_params, 'pong')

    def on_conn(client, userdata, flags, rc):
        """
        On connection to MQTT, subscribe to the relevant topics.
        :param client: Connected MQTT client instance
        :type client: paho.mqtt.client.Client
        :param userdata: User data
        :param flags: Response flags from the broker
        :type flags: dict
        :param rc: Response code from the broker
        :type rc: int
        :return: None
        :rtype: NoneType
        """
        if rc == 0:
            _LOG.info(
                "Connected to the MQTT broker with protocol v%s.",
                config['mqtt']['protocol'])
            for veh_name in list(vehicles.keys()) + ['*']:
                for suffix in (
                        'speed', 'lane', 'ping', 'connect', 'disconnect'):
                    topic = '%s/%s/%s' % (
                        topic_prefix,
                        veh_name,
                        suffix
                    )
                    client.subscribe(topic, qos=1)
                    _LOG.info('Subscribed to topic: %r', topic)
            client.publish(
                config["mqtt"]["status_topic"],
                config['mqtt']['status_payload_running'],
                qos=1,
                retain=True)
        elif rc == 1:
            _LOG.fatal(
                "Incorrect protocol version used to connect to MQTT broker.")
            sys.exit(1)
        elif rc == 2:
            _LOG.fatal(
                "Invalid client identifier used to connect to MQTT broker.")
            sys.exit(1)
        elif rc == 3:
            _LOG.warning("MQTT broker unavailable. Retrying in %s secs...")
            sleep(RECONNECT_DELAY_SECS)
            client.reconnect()
        elif rc == 4:
            _LOG.fatal(
                "Bad username or password used to connect to MQTT broker.")
            sys.exit(1)
        elif rc == 5:
            _LOG.fatal(
                "Not authorised to connect to MQTT broker.")
            sys.exit(1)


    def on_msg(client, userdata, msg):
        """
        On reception of MQTT message, perform the relevant action to a vehicle
        :param client: Connected MQTT client instance
        :type client: paho.mqtt.client.Client
        :param userdata: User data (any data type)
        :param msg: Received message instance
        :type msg: paho.mqtt.client.MQTTMessage
        :return: None
        :rtype: NoneType
        """
        try:
            match = re.match(r'^%s/(.+?)/(.+?)$' % topic_prefix, msg.topic)
            if match is None:
                _LOG.warning('Could not parse topic: %s', msg.topic)
                return
            veh_name, action = match.groups()
            selected_vehicles = []
            if veh_name == '*':
                selected_vehicles = vehicles.values()
            else:
                selected_vehicles = [vehicles[veh_name]]
            if action == 'speed':
                data = json.loads(msg.payload.decode('utf8'))
                for veh in selected_vehicles:
                    veh.changeSpeed(data['speed'], data['accel'])
            elif action == 'lane':
                data = json.loads(msg.payload.decode('utf8'))
                try:
                    direction = data['direction']
                except KeyError:
                    direction = None
                if direction == 'left':
                    for veh in selected_vehicles:
                        veh.changeLaneLeft(data['speed'], data['accel'])
                elif direction == 'right':
                    for veh in selected_vehicles:
                        veh.changeLaneRight(data['speed'], data['accel'])
                else:
                    for veh in selected_vehicles:
                        veh.changeLane(data['speed'], data['accel'], data['offset'])
            elif action == 'ping':
                for veh in selected_vehicles:
                    veh.ping()
            elif action == 'connect':
                for veh in selected_vehicles:
                    veh.connect()
            elif action == 'disconnect':
                for veh in selected_vehicles:
                    veh.disconnect()

        except Exception:
            _LOG.exception('Exception while handling received MQTT message:')

    client.on_connect = on_conn
    client.on_message = on_msg

    for vehicle in config['anki']['vehicles']:
        veh = Overdrive(vehicle['mac'])
        client.publish(
            '%s/%s/connected' % (topic_prefix, vehicle['name']),
            True,
            qos=2
        )
        veh.setLocationChangeCallback(loc_change_callback)
        veh.setTransitionCallback(transition_callback)
        veh.setPongCallback(pong_callback)
        vehicles[vehicle['name']] = veh
        veh.ping()

    try:
        client.connect(config["mqtt"]["host"], config["mqtt"]["port"], 60)
    except socket.error as err:
        _LOG.fatal("Unable to connect to MQTT server: %s" % err)
        sys.exit(1)

    client.loop_start()

    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        print("")
        msg_infos = []
        for name, veh in vehicles.items():
            veh.disconnect()
            msg_infos.append(client.publish(
                '%s/%s/connected' % (topic_prefix, name),
                False,
                qos=2
            ))
        for msg_info in msg_infos:
            msg_info.wait_for_publish()
    finally:
        msg_info = client.publish(
            "%s/%s" % (topic_prefix, config["mqtt"]["status_topic"]),
            config["mqtt"]["status_payload_stopped"], qos=1, retain=True)
        # This should also quit the mqtt loop thread.
        msg_info.wait_for_publish()
        client.disconnect()
        client.loop_forever()
