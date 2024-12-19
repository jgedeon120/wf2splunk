"""
Weatherflow/Tempest data into Splunk
Listen over the wire for Tempest data
Parse data then send to Splunk to be indexed
"""

import configparser
import os
import sys
import requests
import time
from datetime import datetime
import json
from socket import socket, AF_INET, SOCK_DGRAM, IPPROTO_UDP, SOL_SOCKET, SO_BROADCAST, SO_REUSEADDR
from queue import Queue
import threading
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import signal

# Weather flow variables:
wf_Port = 50222
wf_Address = ""
wf_Listener = os.uname()[1]

config = configparser.ConfigParser()
config.read('weatherflow.config')

# Splunk Variables:
splunkReceiver = config['Splunk']['hec_receiver']
splunkPort = config["Splunk"]["hec_port"]
splunkSourcetype = config["Splunk"]["splunk_sourcetype"]
splunkIndex = config["Splunk"]["splunk_index"]
splunkToken = config["Splunk"]["splunk_token"]
wfListener = config["Splunk"]["host_name"]
userAgent = "Weatherflow 2 Splunk v1.0"

def wf_listener_task(q):
    thread_name = threading.current_thread().name
    thread_pid = format(os.getpid())

    while 1:
        try:
            msg, host_info = s.recvfrom(1024)
            msg = msg.decode()
            data = json.loads(msg)
            q.put(data)

        except:
            pass
        time.sleep(0.01)

def wf_reporter_task(q):
    thread_name = threading.current_thread().name
    thread_pid = format(os.getpid())

    while 1:
        try:
            while not q.empty():
                data = q.get()
                record_data(data)

        except:
            pass
        time.sleep(0.01)

def requests_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(408, 500, 502, 503, 504), session=None):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def send_to_splunk(sourcetype: str, index: str, time: int, log_data: str):
    uri = f'https://{splunkReceiver}:{splunkPort}/services/collector/event'
    headers = {'Authorization': f'Splunk {splunkToken}', 'user-agent': '{userAgent}', 'Content-Type': 'application/json'}

    post_data = {
        "time": time,
        "sourcetype": sourcetype,
        "host": f'{wfListener}-{wf_Listener}',
        "index": index,
        "event": log_data
    }

    post_data = json.dumps(post_data).encode('utf-8')
    
    try:
        response = requests_retry_session().post(uri, post_data, headers=headers, verify=False, timeout=10)
        response.raise_for_status()
        return response
    
    except requests.exceptions.RequestException as e:
        print(f'Error during Post request: {e}')
    
def format_evt_strike(data: str):
    # process lightening strike data
    # {"serial_number":"ST-00113978","type":"evt_strike","hub_sn":"HB-00016720","evt":[1734164012,8,16894258]}
    evt_strike = {}
    evt_strike["serial_number"] = data["serial_number"]
    evt_strike["hub_sn"] = data["hub_sn"]
    evt_strike["distance_km"] = data["evt"][1]
    evt_strike["distance_mi"] = round(data["evt"][1] * 0.62137, 2)
    evt_strike["energy"] = data["evt"][2]

    sourcetype = f'{splunkSourcetype}:{data["type"]}'
    time = data["evt"][0]
    index = splunkIndex
    send_to_splunk(sourcetype, index, time, evt_strike)

def format_evt_precip(data: str):
    # process precipitation data
    # {"serial_number":"ST-00113978","type":"evt_precip","hub_sn":"HB-00016720","evt":[1734169487]}
    evt_precip = {}
    evt_precip["serial_number"] = data["serial_number"]
    evt_precip["hub_sn"] = data["hub_sn"]

    sourcetype = f'{splunkSourcetype}:{data["type"]}'
    time = data["evt"][0]
    index = splunkIndex
    send_to_splunk(sourcetype, index, time, evt_precip)

def format_device_status(data: str):
    # process device status data
    # {"serial_number":"ST-00113978","type":"device_status","hub_sn":"HB-00016720","timestamp":173424746,"uptime":9239109,"voltage":2.648,"firmware_revision":181,"rssi":-76,"hub_rssi":-70,"sensor_status":672255,"debug":0}
    
    if "ST-" in data["serial_number"]:
        device_type = "tempest"
    elif "AR-" in data["serial_number"]:
        device_type = "air"
    elif "SK-" in data["serial_number"]:
        device_type = "sky"
    else:
        device_type = "unknown"
    
    device_status = {}
    device_status["device_type"] = device_type
    device_status["serial_number"] = data["serial_number"]
    device_status["hub_sn"] = data["hub_sn"]
    device_status["uptime"] = data["uptime"]
    device_status["voltage"] = data["voltage"]
    device_status["firmware_revision"] = data["firmware_revision"]
    device_status["rssi"] = data["rssi"]
    device_status["hub_rssi"] = data["hub_rssi"]
    device_status["sensor_status"] = data["sensor_status"]
    device_status["debug"] = data["debug"]


    sourcetype = f'{splunkSourcetype}:{data["type"]}'
    time = data["timestamp"]
    index = splunkIndex
    send_to_splunk(sourcetype, index, time, device_status)

def format_obs_st(data: str):
    # process obs_st data
    # {"serial_number":"ST-00113978","type":"obs_st","hub_sn":"HB-00016720","obs":[[1734246921,0.00,0.26,0.75,104,3,998.60,8.30,81.18,1,0.00,0,0.000000,0,0,0,2.646,1]],"firmware_revision":181}
    if data["obs"][0][12] == 0:
        percipitation_type_h = "none"
    elif data["obs"][0][12] == 1:
        percipitation_type_h = "rain"
    elif data["obs"][0][12] == 2:
        percipitation_type_h = "hail"
    elif data["obs"][0][12] == 3:
        percipitation_type_h = "rain + hail"
    else:
        percipitation_type_h = "unknown"

    obs_st = {}
    obs_st["serial_number"] = data["serial_number"]
    obs_st["hub_sn"] = data["hub_sn"]
    obs_st["wind_lull_mps"] = data["obs"][0][1]
    obs_st["wind_avg_mps"] = data["obs"][0][2]
    obs_st["wind_gust_mps"] = data["obs"][0][3]
    obs_st["wind_lull_mips"] = round(data["obs"][0][1] * 2.236936, 2)
    obs_st["wind_avg_mips"] = round(data["obs"][0][2] * 2.236936, 2)
    obs_st["wind_gust_mips"] = round(data["obs"][0][3] * 2.236936, 2)
    obs_st["wind_direction"] = data["obs"][0][4]
    obs_st["wind_sample_interval"] = data["obs"][0][5]
    obs_st["station_pressure"] = data["obs"][0][6]
    obs_st["temperature_c"] = data["obs"][0][7]
    obs_st["temperature_f"] = round(data["obs"][0][7] * 9/5 + 32, 1)
    obs_st["relative_humidity"] = data["obs"][0][8]
    obs_st["illuminance"] = data["obs"][0][9]
    obs_st["uv"] = data["obs"][0][10]
    obs_st["solar_radiation"] = data["obs"][0][11]
    obs_st["rain_accumulated_mm"] = data["obs"][0][12]
    obs_st["rain_accumulated_in"] = round(data["obs"][0][12] * 0.0393700787, 3)
    obs_st["percipitation_type"] = data["obs"][0][13]
    obs_st["percipitation_type_h"] = percipitation_type_h
    obs_st["lightning_strike_avg_distance_km"] = data["obs"][0][14]
    obs_st["lightning_strike_avg_distance_mi"] = round(data["obs"][0][14] * 0.62137, 2)
    obs_st["lightning_strike_count"] = data["obs"][0][15]
    obs_st["battery"] = data["obs"][0][16]
    obs_st["report_interval"] = data["obs"][0][17]
    obs_st["firmware_revision"] = data["firmware_revision"]

    sourcetype = f'{splunkSourcetype}:{data["type"]}'
    time = data["obs"][0][0]
    index = splunkIndex
    send_to_splunk(sourcetype, index, time, obs_st)

def format_rapid_wind(data: str):
    # process wind data
    # {"serial_number":"ST-00113978","type":"rapid_wind","hub_sn":"HB-00016720","ob":[1734247292,0.02,86]}
    rapid_wind = {}
    rapid_wind["serial_number"] = data["serial_number"]
    rapid_wind["speed_mps"] = data["ob"][1]
    rapid_wind["speed_mips"] = round(data["ob"][1] * 2.236936, 2)
    rapid_wind["direction"] = data["ob"][2]

    sourcetype = f'{splunkSourcetype}:{data["type"]}'
    time = data["ob"][0]
    index = splunkIndex
    send_to_splunk(sourcetype, index, time, rapid_wind)

def format_hub_status(data: str):
    # process hub_status data
    # {"serial_number":"HB-00016720","type":"hub_status","firmware_revision":"194","uptime":7736540,"rssi":-32,"timestamp":1734247366,"reset_flags":"PIN,SFT,HRDFLT","seq":770642,"radio_stats":[22,1,0,3,601],"mqtt_stats":[23,1]}
    hub_status = {}
    hub_status["device_type"] = "hub"
    hub_status["serial_number"] = data["serial_number"]
    hub_status["firmware_revision"] = data["firmware_revision"]
    hub_status["uptime"] = data["uptime"]
    hub_status["rssi"] = data["rssi"]
    hub_status["reset_flags"] = data["reset_flags"]
    hub_status["seq"] = data["seq"]
    hub_status["radio_stats_version"] = data["radio_stats"][0]
    hub_status["reboot_count"] = data["radio_stats"][1]
    hub_status["i2c_bus_error_count"] = data["radio_stats"][2]
    hub_status["radio_status"] = data["radio_stats"][3]
    hub_status["radio_network_id"] = data["radio_stats"][4]

    sourcetype = f'{splunkSourcetype}:{data["type"]}'
    time = data["timestamp"]
    index = splunkIndex
    send_to_splunk(sourcetype, index, time, hub_status)

def record_data(data: str):
    if data["type"] == "hub_status":
        format_hub_status(data)
    elif data["type"] == "rapid_wind":
        format_rapid_wind(data)
    elif data["type"] == "obs_st":
        format_obs_st(data)
    elif data["type"] == "device_status":
        format_device_status(data)
    elif data["type"] == "evt_precip":
        format_evt_precip(data)
    elif data["type"] == "evt_strike":
        format_evt_strike(data)

def signal_handler(sig, frame):
    print("\nExiting, shutting down script.")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    s = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)
    s.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
    s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    s.setblocking(False)
    s.bind((wf_Address, wf_Port))

    thread_name = threading.current_thread().name
    thread_pid = format(os.getpid())

    q = Queue(maxsize=0)
    q.join()

    threads = [
        threading.Thread(target=wf_listener_task, name='wf_listener', args=(q,)),
        threading.Thread(target=wf_reporter_task, name='wf_reporter', args=(q,))
    ]
    for t in threads:
        t.setDaemon(True)
        t.start()
    
    for t in threads:
        t.join()

sys.exit(0)
