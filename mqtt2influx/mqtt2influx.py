
from __future__ import print_function

import argparse
import datetime
import logging
import random
import requests
import six
import time
import ujson as json

from six.moves import configparser
from requests.exceptions import ConnectionError, HTTPError, RequestException

import paho.mqtt.client as mqtt

parser = argparse.ArgumentParser()
parser.add_argument('-v', '--verbosity', help='increase output verbosity', action='count')
parser.add_argument('command', choices=['print', 'bridge'])
args = parser.parse_args()

config = configparser.ConfigParser()
config.add_section('mqtt')
config.set('mqtt', 'port', 1883)
config.add_section('influx')
config.set('influx', 'url', '')
config.set('influx', 'username', '')
config.set('influx', 'password', '')
config.set('influx', 'batchsize', '5000')
config.read('mqtt2influx.ini')

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.WARN,)

msgs = []

def mqtt_connect(client, userdata, flags, rc):
    if rc != 0:
        logging.error('mqtt_connect: Connection error rc={}'.format(rc))
        return
    logging.info('Connected to MQTT broker')

    for topic in config.get('mqtt', 'topics').split(','):
        topic = topic.strip()
        logging.debug('Subscribing to topic "{}"'.format(topic))
        client.subscribe(topic)

def msg2influxlines(topic, msg):
    sums = {}
    counts = {}
    jmsg = json.loads(msg)
    feed = topic[5:] if topic.startswith('feed/') else ''
    t = jmsg['t']
    a = jmsg['a']
    ch = a['ch']
    d = a['d']
    if jmsg['f'] != 1:
        raise ValueError('Expecting f:1')

    ret = []
    for stream in jmsg['s']:
        if 's' in stream:
            #raise ValueError('multi-level strems unsupported')
            return []
        sa = stream['a']
        m = sa['m']
        if m == 10:
            continue        # ignore non-numeric m10 for now
        v = stream['v']
        if 'i' in sa:
            i = sa['i']
            key = (ch, d, m)
            sums.setdefault(key, 0)
            counts.setdefault(key, 0)
            sums[key] += v
            counts[key] += 1
        else:
            i = 1
        line = 'm{},ch={},d={},feed={},i={},m={} v={} {}'.format(m, ch, d, feed, i, m, v, t)
        logging.debug(line)
        ret.append(line)

    for key, v in sums.items():
        (ch, d, m) = key
        i = 'sumi'
        line = 'm{},ch={},d={},feed={},i={},m={} v={} {}'.format(m, ch, d, feed, i, m, v, t)
        logging.debug(line)
        ret.append(line)
        count = counts[key]
        i = 'avgi'
        line = 'm{},ch={},d={},feed={},i={},m={} v={:.4f} {}'.format(m, ch, d, feed, i, m, float(v)/count, t)
        logging.debug(line)
        ret.append(line)

    return ret

def mqtt_message(client, userdata, msg):
    logging.debug('topic:{} payload:{}'.format(msg.topic, msg.payload))
    try:
        lines = msg2influxlines(msg.topic, msg.payload)
        msgs.extend(lines)
    except (KeyError, ValueError) as e:
        logging.error('Invalid message: {}: {}'.format(e, msg.payload))

def load_to_influx(lines):
    url = config.get('influx', 'url')
    username = config.get('influx', 'username')
    password = config.get('influx', 'password')
    batchsize = config.getint('influx', 'batchsize')
    logging.info('Sending {} lines to Influx'.format(len(lines)))
    for idx in range(0, len(lines), batchsize):
        try:
            post_data = '\n'.join(lines[idx: idx+batchsize])
            logging.info('POST begin')
            if username != '':
                r = requests.post(url, auth=(args.username, args.password), data=post_data)
            else:
                r = requests.post(url, data=post_data)
            r.raise_for_status()
            logging.info('POST end')
        except ConnectionError:
            raise
        except HTTPError:
            logging.error('{:d} {}'.format(r.status_code, r.text))
        except RequestException as e:
            logging.error(str(e))

def main_loop():
    global msgs

    client = mqtt.Client()
    client.on_connect = mqtt_connect
    client.on_message = mqtt_message

    hostname = config.get('mqtt', 'hostname')
    port = config.get('mqtt', 'port')
    username = config.get('mqtt', 'username')
    password = config.get('mqtt', 'password')

    logging.info('Connecting to "{}:{}" as "{}" ..'.format(hostname, port, username))
    client.username_pw_set(username, password)
    client.connect(hostname, port, 60)

    last_flush = int(time.time())
    while True:
        client.loop(timeout=5.0)
        now = int(time.time())
        if now > last_flush + 5:
            if args.command == 'print':
                print('\n'.join(msgs))
            elif args.command == 'bridge':
                load_to_influx(msgs)
            last_flush = now
            msgs = []

def main():
    if args.verbosity >= 2:
        logging.getLogger().setLevel(logging.DEBUG)
    elif args.verbosity >= 1:
        logging.getLogger().setLevel(logging.INFO)

    main_loop()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
