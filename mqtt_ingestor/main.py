import json
import logging
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

import paho.mqtt.client as mqtt
from influxdb_client_3 import (
    InfluxDBClient3,
    InfluxDBError,
    Point,
    WriteOptions,
    write_client_options, WritePrecision,
)

MQTT_HOST = "mqtt.gipanis.pp.ua"
MQTT_PORT = 31883
MQTT_USERNAME = "userSservisEva1238"
MQTT_PASSWORD = "!90jkaihqnq23499257#$"
MQTT_TOPIC = "#"
DATE_TIME_PATTERN = "%d.%m.%Y %H:%M:%S"
INFLUX_HOST = "http://localhost:8181"
INFLUX_DB = "gipanis"
INFLUX_TABLE = "metrics"
INFLUX_TOKEN = "apiv3_Mz80VTdch5mOElazvMm-xr4q2AAMsbki6eLKPUYKNVcJVAI76IQz_7-TzbJBSgow8Qj2QHpWRYAV135X31FYrA"


class Machine:
    def __init__(self, name=None):
        self.name = name
        self.datasets = set()

        self.ltp = None  # dataset1
        self.rtp = None
        self.lcp = None
        self.rcp = None
        self.lpl = None
        self.rpl = None
        self.sct = None
        self.ctr = None

        self.lisv = None  # dataset2
        self.liav = None
        self.liit = None
        self.lis = None
        self.risv = None
        self.riav = None
        self.riit = None
        self.ris = None

        self.i1s1p = None  # dataset5
        self.i1s2p = None
        self.i1s3p = None
        self.i1s4p = None
        self.i1s5p = None
        self.i1s6p = None
        self.i2s1p = None
        self.i2s2p = None
        self.i2s3p = None
        self.i2s4p = None
        self.i2s5p = None
        self.i2s6p = None
        self.i1s1sf = None
        self.i1s2sf = None
        self.i1s3sf = None
        self.i1s4sf = None
        self.i1s5sf = None
        self.i1s6sf = None

        self.i2s1sf = None  # dataset6
        self.i2s2sf = None
        self.i2s3sf = None
        self.i2s4sf = None
        self.i2s5sf = None
        self.i2s6sf = None


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        logging.info(f"Successfully connected to MQTT broker {MQTT_HOST}:{MQTT_PORT}")
        client.subscribe(MQTT_TOPIC)
    else:
        logging.error(
            f"Connection to MQTT broker {MQTT_HOST}:{MQTT_PORT} failed with result code {reason_code}"
        )


def on_subscribe(client, userdata, mid, reason_code_list, properties):
    if reason_code_list[0].is_failure:
        logging.error(
            f"MQTT broker {MQTT_HOST}:{MQTT_PORT} rejected you subscription {MQTT_TOPIC}: {reason_code_list[0]}"
        )
    else:
        logging.info(
            f"MQTT broker {MQTT_HOST}:{MQTT_PORT} accepted you subscription {MQTT_TOPIC}"
        )


def add_point_to_database(
        machine_name,
        ltp, rtp, lcp, rcp, lpl, rpl, sct, ctr,
        lisv, liav, liit, lis, risv, riav, riit, ris,
        i1s1p, i1s2p, i1s3p, i1s4p, i1s5p, i1s6p, i2s1p, i2s2p, i2s3p, i2s4p, i2s5p, i2s6p, i1s1sf, i1s2sf, i1s3sf,
        i1s4sf, i1s5sf, i1s6sf,
        i2s1sf, i2s2sf, i2s3sf, i2s4sf, i2s5sf, i2s6sf,
):
    point = [Point(INFLUX_TABLE)
             .tag("machine_name", machine_name)
             # dataset1
             .field("lTP_1", ltp[0]).field("lTP_2", ltp[1]).field("lTP_3", ltp[2]).field("lTP_4", ltp[3])
             .field("lTP_5", ltp[4]).field("lTP_6", ltp[5])
             .field("rTP_1", rtp[0]).field("rTP_2", rtp[1]).field("rTP_3", rtp[2]).field("rTP_4", rtp[3])
             .field("rTP_5", rtp[4]).field("rTP_6", rtp[5])
             .field("lCP_1", lcp[0]).field("lCP_2", lcp[1]).field("lCP_3", lcp[2]).field("lCP_4", lcp[3])
             .field("lCP_5", lcp[4]).field("lCP_6", lcp[5])
             .field("rCP_1", rcp[0]).field("rCP_2", rcp[1]).field("rCP_3", rcp[2]).field("rCP_4", rcp[3])
             .field("rCP_5", rcp[4]).field("rCP_6", rcp[5])
             .field("lPL_1", lpl[0]).field("lPL_2", lpl[1]).field("lPL_3", lpl[2]).field("lPL_4", lpl[3])
             .field("lPL_5", lpl[4]).field("lPL_6", lpl[5])
             .field("rPL_1", rpl[0]).field("rPL_2", rpl[1]).field("rPL_3", rpl[2]).field("rPL_4", rpl[3])
             .field("rPL_5", rpl[4]).field("rPL_6", rpl[5])
             .field("sCT_1", sct[0]).field("sCT_2", sct[1]).field("sCT_3", sct[2]).field("sCT_4", sct[3])
             .field("sCT_5", sct[4]).field("sCT_6", sct[5])
             .field("cTR_1", ctr[0]).field("cTR_2", ctr[1]).field("cTR_3", ctr[2]).field("cTR_4", ctr[3])
             .field("cTR_5", ctr[4]).field("cTR_6", ctr[5])
             # dataset2
             .field("lISV_1", lisv[0]).field("lISV_2", lisv[1]).field("lISV_3", lisv[2]).field("lISV_4", lisv[3])
             .field("lISV_5", lisv[4]).field("lISV_6", lisv[5])
             .field("lIAV_1", liav[0]).field("lIAV_2", liav[1]).field("lIAV_3", liav[2]).field("lIAV_4", liav[3])
             .field("lIAV_5", liav[4]).field("lIAV_6", liav[5])
             .field("lIIT_1", liit[0]).field("lIIT_2", liit[1]).field("lIIT_3", liit[2]).field("lIIT_4", liit[3])
             .field("lIIT_5", liit[4]).field("lIIT_6", liit[5])
             .field("lIS_1", lis[0]).field("lIS_2", lis[1]).field("lIS_3", lis[2]).field("lIS_4", lis[3])
             .field("lIS_5", lis[4]).field("lIS_6", lis[5])
             .field("rISV_1", risv[0]).field("rISV_2", risv[1]).field("rISV_3", risv[2]).field("rISV_4", risv[3])
             .field("rISV_5", risv[4]).field("rISV_6", risv[5])
             .field("rIAV_1", riav[0]).field("rIAV_2", riav[1]).field("rIAV_3", riav[2]).field("rIAV_4", riav[3])
             .field("rIAV_5", riav[4]).field("rIAV_6", riav[5])
             .field("rIIT_1", riit[0]).field("rIIT_2", riit[1]).field("rIIT_3", riit[2]).field("rIIT_4", riit[3])
             .field("rIIT_5", riit[4]).field("rIIT_6", riit[5])
             .field("rIS_1", ris[0]).field("rIS_2", ris[1]).field("rIS_3", ris[2]).field("rIS_4", ris[3])
             .field("rIS_5", ris[4]).field("rIS_6", ris[5])
             # dataset5
             .field("i1S1P_1", i1s1p[0]).field("i1S1P_2", i1s1p[1]).field("i1S1P_3", i1s1p[2])
             .field("i1S1P_4", i1s1p[3]).field("i1S1P_5", i1s1p[4]).field("i1S1P_6", i1s1p[5])
             .field("i1S1P_7", i1s1p[6]).field("i1S1P_8", i1s1p[7]).field("i1S1P_9", i1s1p[8])
             .field("i1S1P_10", i1s1p[9])
             .field("i1S2P_1", i1s2p[0]).field("i1S2P_2", i1s2p[1]).field("i1S2P_3", i1s2p[2])
             .field("i1S2P_4", i1s2p[3]).field("i1S2P_5", i1s2p[4]).field("i1S2P_6", i1s2p[5])
             .field("i1S2P_7", i1s2p[6]).field("i1S2P_8", i1s2p[7]).field("i1S2P_9", i1s2p[8])
             .field("i1S2P_10", i1s2p[9])
             .field("i1S3P_1", i1s3p[0]).field("i1S3P_2", i1s3p[1]).field("i1S3P_3", i1s3p[2])
             .field("i1S3P_4", i1s3p[3]).field("i1S3P_5", i1s3p[4]).field("i1S3P_6", i1s3p[5])
             .field("i1S3P_7", i1s3p[6]).field("i1S3P_8", i1s3p[7]).field("i1S3P_9", i1s3p[8])
             .field("i1S3P_10", i1s3p[9])
             .field("i1S4P_1", i1s4p[0]).field("i1S4P_2", i1s4p[1]).field("i1S4P_3", i1s4p[2])
             .field("i1S4P_4", i1s4p[3]).field("i1S4P_5", i1s4p[4]).field("i1S4P_6", i1s4p[5])
             .field("i1S4P_7", i1s4p[6]).field("i1S4P_8", i1s4p[7]).field("i1S4P_9", i1s4p[8])
             .field("i1S4P_10", i1s4p[9])
             .field("i1S5P_1", i1s5p[0]).field("i1S5P_2", i1s5p[1]).field("i1S5P_3", i1s5p[2])
             .field("i1S5P_4", i1s5p[3]).field("i1S5P_5", i1s5p[4]).field("i1S5P_6", i1s5p[5])
             .field("i1S5P_7", i1s5p[6]).field("i1S5P_8", i1s5p[7]).field("i1S5P_9", i1s5p[8])
             .field("i1S5P_10", i1s5p[9])
             .field("i1S6P_1", i1s6p[0]).field("i1S6P_2", i1s6p[1]).field("i1S6P_3", i1s6p[2])
             .field("i1S6P_4", i1s6p[3]).field("i1S6P_5", i1s6p[4]).field("i1S6P_6", i1s6p[5])
             .field("i1S6P_7", i1s6p[6]).field("i1S6P_8", i1s6p[7]).field("i1S6P_9", i1s6p[8])
             .field("i1S6P_10", i1s6p[9])
             .field("i2S1P_1", i2s1p[0]).field("i2S1P_2", i2s1p[1]).field("i2S1P_3", i2s1p[2])
             .field("i2S1P_4", i2s1p[3]).field("i2S1P_5", i2s1p[4]).field("i2S1P_6", i2s1p[5])
             .field("i2S1P_7", i2s1p[6]).field("i2S1P_8", i2s1p[7]).field("i2S1P_9", i2s1p[8])
             .field("i2S1P_10", i2s1p[9])
             .field("i2S2P_1", i2s2p[0]).field("i2S2P_2", i2s2p[1]).field("i2S2P_3", i2s2p[2])
             .field("i2S2P_4", i2s2p[3]).field("i2S2P_5", i2s2p[4]).field("i2S2P_6", i2s2p[5])
             .field("i2S2P_7", i2s2p[6]).field("i2S2P_8", i2s2p[7]).field("i2S2P_9", i2s2p[8])
             .field("i2S2P_10", i2s2p[9])
             .field("i2S3P_1", i2s3p[0]).field("i2S3P_2", i2s3p[1]).field("i2S3P_3", i2s3p[2])
             .field("i2S3P_4", i2s3p[3]).field("i2S3P_5", i2s3p[4]).field("i2S3P_6", i2s3p[5])
             .field("i2S3P_7", i2s3p[6]).field("i2S3P_8", i2s3p[7]).field("i2S3P_9", i2s3p[8])
             .field("i2S3P_10", i2s3p[9])
             .field("i2S4P_1", i2s4p[0]).field("i2S4P_2", i2s4p[1]).field("i2S4P_3", i2s4p[2])
             .field("i2S4P_4", i2s4p[3]).field("i2S4P_5", i2s4p[4]).field("i2S4P_6", i2s4p[5])
             .field("i2S4P_7", i2s4p[6]).field("i2S4P_8", i2s4p[7]).field("i2S4P_9", i2s4p[8])
             .field("i2S4P_10", i2s4p[9])
             .field("i2S5P_1", i2s5p[0]).field("i2S5P_2", i2s5p[1]).field("i2S5P_3", i2s5p[2])
             .field("i2S5P_4", i2s5p[3]).field("i2S5P_5", i2s5p[4]).field("i2S5P_6", i2s5p[5])
             .field("i2S5P_7", i2s5p[6]).field("i2S5P_8", i2s5p[7]).field("i2S5P_9", i2s5p[8])
             .field("i2S5P_10", i2s5p[9])
             .field("i2S6P_1", i2s6p[0]).field("i2S6P_2", i2s6p[1]).field("i2S6P_3", i2s6p[2])
             .field("i2S6P_4", i2s6p[3]).field("i2S6P_5", i2s6p[4]).field("i2S6P_6", i2s6p[5])
             .field("i2S6P_7", i2s6p[6]).field("i2S6P_8", i2s6p[7]).field("i2S6P_9", i2s6p[8])
             .field("i2S6P_10", i2s6p[9])
             .field("i1S1SF_1", i1s1sf[0]).field("i1S1SF_2", i1s1sf[1]).field("i1S1SF_3", i1s1sf[2])
             .field("i1S1SF_4", i1s1sf[3]).field("i1S1SF_5", i1s1sf[4]).field("i1S1SF_6", i1s1sf[5])
             .field("i1S1SF_7", i1s1sf[6]).field("i1S1SF_8", i1s1sf[7]).field("i1S1SF_9", i1s1sf[8])
             .field("i1S1SF_10", i1s1sf[9])
             .field("i1S2SF_1", i1s2sf[0]).field("i1S2SF_2", i1s2sf[1]).field("i1S2SF_3", i1s2sf[2])
             .field("i1S2SF_4", i1s2sf[3]).field("i1S2SF_5", i1s2sf[4]).field("i1S2SF_6", i1s2sf[5])
             .field("i1S2SF_7", i1s2sf[6]).field("i1S2SF_8", i1s2sf[7]).field("i1S2SF_9", i1s2sf[8])
             .field("i1S2SF_10", i1s2sf[9])
             .field("i1S3SF_1", i1s3sf[0]).field("i1S3SF_2", i1s3sf[1]).field("i1S3SF_3", i1s3sf[2])
             .field("i1S3SF_4", i1s3sf[3]).field("i1S3SF_5", i1s3sf[4]).field("i1S3SF_6", i1s3sf[5])
             .field("i1S3SF_7", i1s3sf[6]).field("i1S3SF_8", i1s3sf[7]).field("i1S3SF_9", i1s3sf[8])
             .field("i1S3SF_10", i1s3sf[9])
             .field("i1S4SF_1", i1s4sf[0]).field("i1S4SF_2", i1s4sf[1]).field("i1S4SF_3", i1s4sf[2])
             .field("i1S4SF_4", i1s4sf[3]).field("i1S4SF_5", i1s4sf[4]).field("i1S4SF_6", i1s4sf[5])
             .field("i1S4SF_7", i1s4sf[6]).field("i1S4SF_8", i1s4sf[7]).field("i1S4SF_9", i1s4sf[8])
             .field("i1S4SF_10", i1s4sf[9])
             .field("i1S5SF_1", i1s5sf[0]).field("i1S5SF_2", i1s5sf[1]).field("i1S5SF_3", i1s5sf[2])
             .field("i1S5SF_4", i1s5sf[3]).field("i1S5SF_5", i1s5sf[4]).field("i1S5SF_6", i1s5sf[5])
             .field("i1S5SF_7", i1s5sf[6]).field("i1S5SF_8", i1s5sf[7]).field("i1S5SF_9", i1s5sf[8])
             .field("i1S5SF_10", i1s5sf[9])
             .field("i1S6SF_1", i1s6sf[0]).field("i1S6SF_2", i1s6sf[1]).field("i1S6SF_3", i1s6sf[2])
             .field("i1S6SF_4", i1s6sf[3]).field("i1S6SF_5", i1s6sf[4]).field("i1S6SF_6", i1s6sf[5])
             .field("i1S6SF_7", i1s6sf[6]).field("i1S6SF_8", i1s6sf[7]).field("i1S6SF_9", i1s6sf[8])
             .field("i1S6SF_10", i1s6sf[9])
             # dataset6
             .field("i2S1SF_1", i2s1sf[0]).field("i2S1SF_2", i2s1sf[1]).field("i2S1SF_3", i2s1sf[2])
             .field("i2S1SF_4", i2s1sf[3]).field("i2S1SF_5", i2s1sf[4]).field("i2S1SF_6", i2s1sf[5])
             .field("i2S1SF_7", i2s1sf[6]).field("i2S1SF_8", i2s1sf[7]).field("i2S1SF_9", i2s1sf[8])
             .field("i2S1SF_10", i2s1sf[9])
             .field("i2S2SF_1", i2s2sf[0]).field("i2S2SF_2", i2s2sf[1]).field("i2S2SF_3", i2s2sf[2])
             .field("i2S2SF_4", i2s2sf[3]).field("i2S2SF_5", i2s2sf[4]).field("i2S2SF_6", i2s2sf[5])
             .field("i2S2SF_7", i2s2sf[6]).field("i2S2SF_8", i2s2sf[7]).field("i2S2SF_9", i2s2sf[8])
             .field("i2S2SF_10", i2s2sf[9])
             .field("i2S3SF_1", i2s3sf[0]).field("i2S3SF_2", i2s3sf[1]).field("i2S3SF_3", i2s3sf[2])
             .field("i2S3SF_4", i2s3sf[3]).field("i2S3SF_5", i2s3sf[4]).field("i2S3SF_6", i2s3sf[5])
             .field("i2S3SF_7", i2s3sf[6]).field("i2S3SF_8", i2s3sf[7]).field("i2S3SF_9", i2s3sf[8])
             .field("i2S3SF_10", i2s3sf[9])
             .field("i2S4SF_1", i2s4sf[0]).field("i2S4SF_2", i2s4sf[1]).field("i2S4SF_3", i2s4sf[2])
             .field("i2S4SF_4", i2s4sf[3]).field("i2S4SF_5", i2s4sf[4]).field("i2S4SF_6", i2s4sf[5])
             .field("i2S4SF_7", i2s4sf[6]).field("i2S4SF_8", i2s4sf[7]).field("i2S4SF_9", i2s4sf[8])
             .field("i2S4SF_10", i2s4sf[9])
             .field("i2S5SF_1", i2s5sf[0]).field("i2S5SF_2", i2s5sf[1]).field("i2S5SF_3", i2s5sf[2])
             .field("i2S5SF_4", i2s5sf[3]).field("i2S5SF_5", i2s5sf[4]).field("i2S5SF_6", i2s5sf[5])
             .field("i2S5SF_7", i2s5sf[6]).field("i2S5SF_8", i2s5sf[7]).field("i2S5SF_9", i2s5sf[8])
             .field("i2S5SF_10", i2s5sf[9])
             .field("i2S6SF_1", i2s6sf[0]).field("i2S6SF_2", i2s6sf[1]).field("i2S6SF_3", i2s6sf[2])
             .field("i2S6SF_4", i2s6sf[3]).field("i2S6SF_5", i2s6sf[4]).field("i2S6SF_6", i2s6sf[5])
             .field("i2S6SF_7", i2s6sf[6]).field("i2S6SF_8", i2s6sf[7]).field("i2S6SF_9", i2s6sf[8])
             .field("i2S6SF_10", i2s6sf[9])

             .time(int(time.time()), WritePrecision.S)
             ]
    with InfluxDBClient3(host=INFLUX_HOST,
                         token=INFLUX_TOKEN,
                         database=INFLUX_DB,
                         write_client_options=wco) as client:
        client.write(point, write_precision='s')


def parse_mqtt_payload(payload_str):
    payload_list = json.loads(payload_str)
    result = {}

    for item in payload_list:
        result.update(item)

    return result


def on_message(client, userdata, msg):
    try:
        machine_name, dataset = msg.topic.split("/")
        dataset = dataset.lower()

        if dataset == "dataset3" or dataset == "dataset4":
            return

        machine_name = machine_name.lower()

        if machine_name == "eva1":
            machine = eva1
        elif machine_name == "eva2":
            machine = eva2
        elif machine_name == "eva3":
            machine = eva3
        else:
            machine = eva4

        payload_str = msg.payload.decode("utf-8")
        data = parse_mqtt_payload(payload_str)

        if dataset == "dataset1":
            machine.ltp = data["lTP"]
            machine.rtp = data["rTP"]
            machine.lcp = data["lCP"]
            machine.rcp = data["rCP"]
            machine.lpl = data["lPL"]
            machine.rpl = data["rPL"]
            machine.sct = data["sCT"]
            machine.ctr = data["cTR"]
        elif dataset == "dataset2":
            machine.lisv = data["lISV"]
            machine.liav = data["lIAV"]
            machine.liit = data["lIIT"]
            machine.lis = data["lIS"]
            machine.risv = data["rISV"]
            machine.riav = data["rIAV"]
            machine.riit = data["rIIT"]
            machine.ris = data["rIS"]
        elif dataset == "dataset5":
            machine.i1s1p = data["i1S1P"]
            machine.i1s2p = data["i1S2P"]
            machine.i1s3p = data["i1S3P"]
            machine.i1s4p = data["i1S4P"]
            machine.i1s5p = data["i1S5P"]
            machine.i1s6p = data["i1S6P"]
            machine.i2s1p = data["i2S1P"]
            machine.i2s2p = data["i2S2P"]
            machine.i2s3p = data["i2S3P"]
            machine.i2s4p = data["i2S4P"]
            machine.i2s5p = data["i2S5P"]
            machine.i2s6p = data["i2S6P"]
            machine.i1s1sf = data["i1S1SF"]
            machine.i1s2sf = data["i1S2SF"]
            machine.i1s3sf = data["i1S3SF"]
            machine.i1s4sf = data["i1S4SF"]
            machine.i1s5sf = data["i1S5SF"]
            machine.i1s6sf = data["i1S6SF"]
        else:  # dataset6
            machine.i2s1sf = data["i2S1SF"]
            machine.i2s2sf = data["i2S2SF"]
            machine.i2s3sf = data["i2S3SF"]
            machine.i2s4sf = data["i2S4SF"]
            machine.i2s5sf = data["i2S5SF"]
            machine.i2s6sf = data["i2S6SF"]

        machine.datasets.add(dataset)

        if len(machine.datasets) == 4:
            add_point_to_database(machine.name,
                                  machine.ltp, machine.rtp, machine.lcp, machine.rcp, machine.lpl, machine.rpl,
                                  machine.sct,
                                  machine.ctr,
                                  machine.lisv, machine.liav, machine.liit, machine.lis, machine.risv, machine.riav,
                                  machine.riit, machine.ris,
                                  machine.i1s1p, machine.i1s2p, machine.i1s3p, machine.i1s4p, machine.i1s5p,
                                  machine.i1s6p,
                                  machine.i2s1p, machine.i2s2p, machine.i2s3p, machine.i2s4p, machine.i2s5p,
                                  machine.i2s6p,
                                  machine.i1s1sf, machine.i1s2sf, machine.i1s3sf, machine.i1s4sf, machine.i1s5sf,
                                  machine.i1s6sf,
                                  machine.i2s1sf, machine.i2s2sf, machine.i2s3sf, machine.i2s4sf, machine.i2s5sf,
                                  machine.i2s6sf
                                  )
            machine.datasets.clear()
    except Exception as ex:
        machine.datasets.clear()
        logging.error(f"on_message error: {ex}")


def setup_logging():
    log_path = Path(__file__).resolve().parent / "mqtt_ingestor.log"

    handler = RotatingFileHandler(
        log_path,
        maxBytes=1_000_000,  # 1 МB
        backupCount=1,
        encoding="utf-8",
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%d.%m.%Y %H:%M:%S",
        handlers=[
            handler,
            logging.StreamHandler(),
        ],
    )


def db_write_error(self, data: str, exception: InfluxDBError):
    logging.error(f"Failed writing batch: config: {self}, data: {data} due: {exception}")


influxdb_write_options = WriteOptions(batch_size=500,
                                      flush_interval=10_000,
                                      jitter_interval=2_000,
                                      retry_interval=5_000,
                                      max_retries=5,
                                      max_retry_delay=30_000,
                                      exponential_base=2)

wco = write_client_options(error_callback=db_write_error, write_options=influxdb_write_options)

eva1 = Machine("eva1")
eva2 = Machine("eva2")
eva3 = Machine("eva3")
eva4 = Machine("eva4")

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.username_pw_set(username=MQTT_USERNAME, password=MQTT_PASSWORD)
mqtt_client.on_connect = on_connect
mqtt_client.on_subscribe = on_subscribe
mqtt_client.on_message = on_message
setup_logging()

try:
    mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
    mqtt_client.loop_forever()
except KeyboardInterrupt:
    logging.info("MQTT client stopped manually")
    mqtt_client.disconnect()
except Exception as e:
    logging.error(f"Error connecting to MQTT broker {MQTT_HOST}:{MQTT_PORT}: {e}")
