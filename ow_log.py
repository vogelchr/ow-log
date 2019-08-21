#!/usr/bin/python

from influxdb import InfluxDBClient
import os
import os.path
import time
import datetime
import argparse

def read_sensor_list(fn):
    ret = list()
    lineno = 0

    try:
        with open(fn) as f:
            for line in f:
                lineno += 1
                line = line.strip()
                ix = line.find('#')
                if ix != -1:
                    line = line[:ix]
                if not line:
                    continue

                arr = line.split()
                if len(arr) < 2:
                    raise RuntimeError('Not enough fields in line.')

                w1_id = arr[0]
                sensor_name = arr[1]
                ret.append((w1_id, sensor_name))

    except Exception as e:
        raise RuntimeError(f'Exception raised reading {fn}:{lineno}.') from e

    return ret


parser = argparse.ArgumentParser()
parser.add_argument('sensors',
                    help='Sensor list.')
parser.add_argument('-H', '--host', help='Influxdb host. [127.0.0.1]',
        metavar='host', type=str, default='127.0.0.1')
parser.add_argument('-P', '--port', help='Influxdb port. [8086]',
        metavar='port', type=int, default=8086)
parser.add_argument('-D', '--db', help='Influxdb database. [heating]',
        metavar='db', type=str, default='heating')
parser.add_argument('-M', '--measurement', help='Measurement. [onewire]',
        metavar='txt', type=str, default='onewire')
parser.add_argument('-b', '--batchsize', help='Batch insert every N measurements. [10]',
        metavar='N', type=int, default=10)
parser.add_argument('-s', '--sleep', help='Sleep N seconds between polls [15]',
        metavar='SEC', type=int, default=15)

args = parser.parse_args()

sensors = read_sensor_list(args.sensors)
influx_client = InfluxDBClient(args.host, args.port)

datapoints = list()
poll_ctr = 0

while True:
    poll_ctr += 1
    influx_fields = dict()
    now = datetime.datetime.now(datetime.timezone.utc)
    for ow_id, sensorname in sensors:
        try :
            hwmondir = os.path.join('/sys/bus/w1/devices', ow_id, 'hwmon')
            if not os.path.isdir(hwmondir) :
                continue

            hwmon_entries = [fn for fn in os.listdir(hwmondir) if fn.startswith('hwmon')]
            if not hwmon_entries :
                continue

            hwmonXdir = os.path.join(hwmondir, hwmon_entries[0])
            if not os.path.isdir(hwmonXdir) :
                continue

            temp_input_fn = os.path.join(hwmonXdir, 'temp1_input')
            if not os.path.isfile(temp_input_fn) :
                continue

            temp_mil_degC = int(open(temp_input_fn).read())
            if temp_mil_degC == 85000 :
                print(f'{ow_id} {sensorname} returned T=85000 mdegC: ignoring')
                continue

            influx_fields[sensorname] = 0.001 * temp_mil_degC

        except Exception as e :
            e_str = str(e)
            print(f'{ow_id} {sensorname}: Exception {e_str} caught.')

    js_body = {
        'measurement': args.measurement,
        'time': now.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'fields': influx_fields
    }
    datapoints.append(js_body)

    if poll_ctr >= args.batchsize :
        try :
#            print('Writing datapoints...')
#            print(datapoints)
            influx_client.write_points(datapoints, database=args.db)
            datapoints.clear()
        except Exception as e :
            print(f'Exception occured writing points to influxdb.')
            print(e)
        poll_ctr = 0

    time.sleep(args.sleep)
