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
             .field("ltp_1", ltp[0]).field("ltp_2", ltp[1]).field("ltp_3", ltp[2]).field("ltp_4", ltp[3])
             .field("ltp_5", ltp[4]).field("ltp_6", ltp[5])
             .field("rtp_1", rtp[0]).field("rtp_2", rtp[1]).field("rtp_3", rtp[2]).field("rtp_4", rtp[3])
             .field("rtp_5", rtp[4]).field("rtp_6", rtp[5])
             .field("lcp_1", lcp[0]).field("lcp_2", lcp[1]).field("lcp_3", lcp[2]).field("lcp_4", lcp[3])
             .field("lcp_5", lcp[4]).field("lcp_6", lcp[5])
             .field("rcp_1", rcp[0]).field("rcp_2", rcp[1]).field("rcp_3", rcp[2]).field("rcp_4", rcp[3])
             .field("rcp_5", rcp[4]).field("rcp_6", rcp[5])
             .field("lpl_1", lpl[0]).field("lpl_2", lpl[1]).field("lpl_3", lpl[2]).field("lpl_4", lpl[3])
             .field("lpl_5", lpl[4]).field("lpl_6", lpl[5])
             .field("rpl_1", rpl[0]).field("rpl_2", rpl[1]).field("rpl_3", rpl[2]).field("rpl_4", rpl[3])
             .field("rpl_5", rpl[4]).field("rpl_6", rpl[5])
             .field("sct_1", sct[0]).field("sct_2", sct[1]).field("sct_3", sct[2]).field("sct_4", sct[3])
             .field("sct_5", sct[4]).field("sct_6", sct[5])
             .field("ctr_1", ctr[0]).field("ctr_2", ctr[1]).field("ctr_3", ctr[2]).field("ctr_4", ctr[3])
             .field("ctr_5", ctr[4]).field("ctr_6", ctr[5])
             # dataset2
             .field("lisv_1", lisv[0]).field("lisv_2", lisv[1]).field("lisv_3", lisv[2]).field("lisv_4", lisv[3])
             .field("lisv_5", lisv[4]).field("lisv_6", lisv[5])
             .field("liav_1", liav[0]).field("liav_2", liav[1]).field("liav_3", liav[2]).field("liav_4", liav[3])
             .field("liav_5", liav[4]).field("liav_6", liav[5])
             .field("liit_1", liit[0]).field("liit_2", liit[1]).field("liit_3", liit[2]).field("liit_4", liit[3])
             .field("liit_5", liit[4]).field("liit_6", liit[5])
             .field("lis_1", lis[0]).field("lis_2", lis[1]).field("lis_3", lis[2]).field("lis_4", lis[3])
             .field("lis_5", lis[4]).field("lis_6", lis[5])
             .field("risv_1", risv[0]).field("risv_2", risv[1]).field("risv_3", risv[2]).field("risv_4", risv[3])
             .field("risv_5", risv[4]).field("risv_6", risv[5])
             .field("riav_1", riav[0]).field("riav_2", riav[1]).field("riav_3", riav[2]).field("riav_4", riav[3])
             .field("riav_5", riav[4]).field("riav_6", riav[5])
             .field("riit_1", riit[0]).field("riit_2", riit[1]).field("riit_3", riit[2]).field("riit_4", riit[3])
             .field("riit_5", riit[4]).field("riit_6", riit[5])
             .field("ris_1", ris[0]).field("ris_2", ris[1]).field("ris_3", ris[2]).field("ris_4", ris[3])
             .field("ris_5", ris[4]).field("ris_6", ris[5])
             # dataset5
             .field("i1s1p_1", i1s1p[0]).field("i1s1p_2", i1s1p[1]).field("i1s1p_3", i1s1p[2])
             .field("i1s1p_4", i1s1p[3]).field("i1s1p_5", i1s1p[4]).field("i1s1p_6", i1s1p[5])
             .field("i1s1p_7", i1s1p[6]).field("i1s1p_8", i1s1p[7]).field("i1s1p_9", i1s1p[8])
             .field("i1s1p_10", i1s1p[9])
             .field("i1s2p_1", i1s2p[0]).field("i1s2p_2", i1s2p[1]).field("i1s2p_3", i1s2p[2])
             .field("i1s2p_4", i1s2p[3]).field("i1s2p_5", i1s2p[4]).field("i1s2p_6", i1s2p[5])
             .field("i1s2p_7", i1s2p[6]).field("i1s2p_8", i1s2p[7]).field("i1s2p_9", i1s2p[8])
             .field("i1s2p_10", i1s2p[9])
             .field("i1s3p_1", i1s3p[0]).field("i1s3p_2", i1s3p[1]).field("i1s3p_3", i1s3p[2])
             .field("i1s3p_4", i1s3p[3]).field("i1s3p_5", i1s3p[4]).field("i1s3p_6", i1s3p[5])
             .field("i1s3p_7", i1s3p[6]).field("i1s3p_8", i1s3p[7]).field("i1s3p_9", i1s3p[8])
             .field("i1s3p_10", i1s3p[9])
             .field("i1s4p_1", i1s4p[0]).field("i1s4p_2", i1s4p[1]).field("i1s4p_3", i1s4p[2])
             .field("i1s4p_4", i1s4p[3]).field("i1s4p_5", i1s4p[4]).field("i1s4p_6", i1s4p[5])
             .field("i1s4p_7", i1s4p[6]).field("i1s4p_8", i1s4p[7]).field("i1s4p_9", i1s4p[8])
             .field("i1s4p_10", i1s4p[9])
             .field("i1s5p_1", i1s5p[0]).field("i1s5p_2", i1s5p[1]).field("i1s5p_3", i1s5p[2])
             .field("i1s5p_4", i1s5p[3]).field("i1s5p_5", i1s5p[4]).field("i1s5p_6", i1s5p[5])
             .field("i1s5p_7", i1s5p[6]).field("i1s5p_8", i1s5p[7]).field("i1s5p_9", i1s5p[8])
             .field("i1s5p_10", i1s5p[9])
             .field("i1s6p_1", i1s6p[0]).field("i1s6p_2", i1s6p[1]).field("i1s6p_3", i1s6p[2])
             .field("i1s6p_4", i1s6p[3]).field("i1s6p_5", i1s6p[4]).field("i1s6p_6", i1s6p[5])
             .field("i1s6p_7", i1s6p[6]).field("i1s6p_8", i1s6p[7]).field("i1s6p_9", i1s6p[8])
             .field("i1s6p_10", i1s6p[9])
             .field("i2s1p_1", i2s1p[0]).field("i2s1p_2", i2s1p[1]).field("i2s1p_3", i2s1p[2])
             .field("i2s1p_4", i2s1p[3]).field("i2s1p_5", i2s1p[4]).field("i2s1p_6", i2s1p[5])
             .field("i2s1p_7", i2s1p[6]).field("i2s1p_8", i2s1p[7]).field("i2s1p_9", i2s1p[8])
             .field("i2s1p_10", i2s1p[9])
             .field("i2s2p_1", i2s2p[0]).field("i2s2p_2", i2s2p[1]).field("i2s2p_3", i2s2p[2])
             .field("i2s2p_4", i2s2p[3]).field("i2s2p_5", i2s2p[4]).field("i2s2p_6", i2s2p[5])
             .field("i2s2p_7", i2s2p[6]).field("i2s2p_8", i2s2p[7]).field("i2s2p_9", i2s2p[8])
             .field("i2s2p_10", i2s2p[9])
             .field("i2s3p_1", i2s3p[0]).field("i2s3p_2", i2s3p[1]).field("i2s3p_3", i2s3p[2])
             .field("i2s3p_4", i2s3p[3]).field("i2s3p_5", i2s3p[4]).field("i2s3p_6", i2s3p[5])
             .field("i2s3p_7", i2s3p[6]).field("i2s3p_8", i2s3p[7]).field("i2s3p_9", i2s3p[8])
             .field("i2s3p_10", i2s3p[9])
             .field("i2s4p_1", i2s4p[0]).field("i2s4p_2", i2s4p[1]).field("i2s4p_3", i2s4p[2])
             .field("i2s4p_4", i2s4p[3]).field("i2s4p_5", i2s4p[4]).field("i2s4p_6", i2s4p[5])
             .field("i2s4p_7", i2s4p[6]).field("i2s4p_8", i2s4p[7]).field("i2s4p_9", i2s4p[8])
             .field("i2s4p_10", i2s4p[9])
             .field("i2s5p_1", i2s5p[0]).field("i2s5p_2", i2s5p[1]).field("i2s5p_3", i2s5p[2])
             .field("i2s5p_4", i2s5p[3]).field("i2s5p_5", i2s5p[4]).field("i2s5p_6", i2s5p[5])
             .field("i2s5p_7", i2s5p[6]).field("i2s5p_8", i2s5p[7]).field("i2s5p_9", i2s5p[8])
             .field("i2s5p_10", i2s5p[9])
             .field("i2s6p_1", i2s6p[0]).field("i2s6p_2", i2s6p[1]).field("i2s6p_3", i2s6p[2])
             .field("i2s6p_4", i2s6p[3]).field("i2s6p_5", i2s6p[4]).field("i2s6p_6", i2s6p[5])
             .field("i2s6p_7", i2s6p[6]).field("i2s6p_8", i2s6p[7]).field("i2s6p_9", i2s6p[8])
             .field("i2s6p_10", i2s6p[9])
             .field("i1s1sf_1", i1s1sf[0]).field("i1s1sf_2", i1s1sf[1]).field("i1s1sf_3", i1s1sf[2])
             .field("i1s1sf_4", i1s1sf[3]).field("i1s1sf_5", i1s1sf[4]).field("i1s1sf_6", i1s1sf[5])
             .field("i1s1sf_7", i1s1sf[6]).field("i1s1sf_8", i1s1sf[7]).field("i1s1sf_9", i1s1sf[8])
             .field("i1s1sf_10", i1s1sf[9])
             .field("i1s2sf_1", i1s2sf[0]).field("i1s2sf_2", i1s2sf[1]).field("i1s2sf_3", i1s2sf[2])
             .field("i1s2sf_4", i1s2sf[3]).field("i1s2sf_5", i1s2sf[4]).field("i1s2sf_6", i1s2sf[5])
             .field("i1s2sf_7", i1s2sf[6]).field("i1s2sf_8", i1s2sf[7]).field("i1s2sf_9", i1s2sf[8])
             .field("i1s2sf_10", i1s2sf[9])
             .field("i1s3sf_1", i1s3sf[0]).field("i1s3sf_2", i1s3sf[1]).field("i1s3sf_3", i1s3sf[2])
             .field("i1s3sf_4", i1s3sf[3]).field("i1s3sf_5", i1s3sf[4]).field("i1s3sf_6", i1s3sf[5])
             .field("i1s3sf_7", i1s3sf[6]).field("i1s3sf_8", i1s3sf[7]).field("i1s3sf_9", i1s3sf[8])
             .field("i1s3sf_10", i1s3sf[9])
             .field("i1s4sf_1", i1s4sf[0]).field("i1s4sf_2", i1s4sf[1]).field("i1s4sf_3", i1s4sf[2])
             .field("i1s4sf_4", i1s4sf[3]).field("i1s4sf_5", i1s4sf[4]).field("i1s4sf_6", i1s4sf[5])
             .field("i1s4sf_7", i1s4sf[6]).field("i1s4sf_8", i1s4sf[7]).field("i1s4sf_9", i1s4sf[8])
             .field("i1s4sf_10", i1s4sf[9])
             .field("i1s5sf_1", i1s5sf[0]).field("i1s5sf_2", i1s5sf[1]).field("i1s5sf_3", i1s5sf[2])
             .field("i1s5sf_4", i1s5sf[3]).field("i1s5sf_5", i1s5sf[4]).field("i1s5sf_6", i1s5sf[5])
             .field("i1s5sf_7", i1s5sf[6]).field("i1s5sf_8", i1s5sf[7]).field("i1s5sf_9", i1s5sf[8])
             .field("i1s5sf_10", i1s5sf[9])
             .field("i1s6sf_1", i1s6sf[0]).field("i1s6sf_2", i1s6sf[1]).field("i1s6sf_3", i1s6sf[2])
             .field("i1s6sf_4", i1s6sf[3]).field("i1s6sf_5", i1s6sf[4]).field("i1s6sf_6", i1s6sf[5])
             .field("i1s6sf_7", i1s6sf[6]).field("i1s6sf_8", i1s6sf[7]).field("i1s6sf_9", i1s6sf[8])
             .field("i1s6sf_10", i1s6sf[9])
             # dataset6
             .field("i2s1sf_1", i2s1sf[0]).field("i2s1sf_2", i2s1sf[1]).field("i2s1sf_3", i2s1sf[2])
             .field("i2s1sf_4", i2s1sf[3]).field("i2s1sf_5", i2s1sf[4]).field("i2s1sf_6", i2s1sf[5])
             .field("i2s1sf_7", i2s1sf[6]).field("i2s1sf_8", i2s1sf[7]).field("i2s1sf_9", i2s1sf[8])
             .field("i2s1sf_10", i2s1sf[9])
             .field("i2s2sf_1", i2s2sf[0]).field("i2s2sf_2", i2s2sf[1]).field("i2s2sf_3", i2s2sf[2])
             .field("i2s2sf_4", i2s2sf[3]).field("i2s2sf_5", i2s2sf[4]).field("i2s2sf_6", i2s2sf[5])
             .field("i2s2sf_7", i2s2sf[6]).field("i2s2sf_8", i2s2sf[7]).field("i2s2sf_9", i2s2sf[8])
             .field("i2s2sf_10", i2s2sf[9])
             .field("i2s3sf_1", i2s3sf[0]).field("i2s3sf_2", i2s3sf[1]).field("i2s3sf_3", i2s3sf[2])
             .field("i2s3sf_4", i2s3sf[3]).field("i2s3sf_5", i2s3sf[4]).field("i2s3sf_6", i2s3sf[5])
             .field("i2s3sf_7", i2s3sf[6]).field("i2s3sf_8", i2s3sf[7]).field("i2s3sf_9", i2s3sf[8])
             .field("i2s3sf_10", i2s3sf[9])
             .field("i2s4sf_1", i2s4sf[0]).field("i2s4sf_2", i2s4sf[1]).field("i2s4sf_3", i2s4sf[2])
             .field("i2s4sf_4", i2s4sf[3]).field("i2s4sf_5", i2s4sf[4]).field("i2s4sf_6", i2s4sf[5])
             .field("i2s4sf_7", i2s4sf[6]).field("i2s4sf_8", i2s4sf[7]).field("i2s4sf_9", i2s4sf[8])
             .field("i2s4sf_10", i2s4sf[9])
             .field("i2s5sf_1", i2s5sf[0]).field("i2s5sf_2", i2s5sf[1]).field("i2s5sf_3", i2s5sf[2])
             .field("i2s5sf_4", i2s5sf[3]).field("i2s5sf_5", i2s5sf[4]).field("i2s5sf_6", i2s5sf[5])
             .field("i2s5sf_7", i2s5sf[6]).field("i2s5sf_8", i2s5sf[7]).field("i2s5sf_9", i2s5sf[8])
             .field("i2s5sf_10", i2s5sf[9])
             .field("i2s6sf_1", i2s6sf[0]).field("i2s6sf_2", i2s6sf[1]).field("i2s6sf_3", i2s6sf[2])
             .field("i2s6sf_4", i2s6sf[3]).field("i2s6sf_5", i2s6sf[4]).field("i2s6sf_6", i2s6sf[5])
             .field("i2s6sf_7", i2s6sf[6]).field("i2s6sf_8", i2s6sf[7]).field("i2s6sf_9", i2s6sf[8])
             .field("i2s6sf_10", i2s6sf[9])

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
