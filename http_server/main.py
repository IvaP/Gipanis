import json
import logging
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

from influxdb_client_3 import InfluxDBClient3

HOST = "0.0.0.0"
PORT = 8000
DATE_TIME_PATTERN = "%d.%m.%Y %H:%M:%S"
REQUIRED_PARAMS = ("start", "end", "bucket")
INFLUX_HOST = "http://gipanis.pp.ua:8282"
INFLUX_DB = "gipanis"
INFLUX_TABLE = "metrics"
INFLUX_TOKEN = "apiv3_NUIMypuZlWJ-LNHFGxPh5qle2RZ1vYeWTlC0LolZL5vrAgNDiUULkRez6O5pK6TZBf1TAiAnjxMYuWuMjfegxg"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parts, params = self.parse_uri()[1:3]

        if len(parts) == 3 and parts[0] == "machines" and parts[2] == "productivity":
            if missing_params := self.get_missing_params(params):
                self.send_json(400, {
                    "error": f"missing required parameters: {', '.join(missing_params)}"
                })
            else:
                machine_name = parts[1].lower()
                utc_start = self.ukraine_time_to_utc_str(params["start"][0])
                utc_end = self.ukraine_time_to_utc_str(params["end"][0])
                bucket = params["bucket"][0]
                rows = self.get_machine_productivity(machine_name, utc_start, utc_end, bucket)
                rows = self.rows_to_json_ready(rows)
                json_text = json.dumps(rows, ensure_ascii=False)
                self.send_json(200, json_text)
        else:
            self.send_json(404, {"error": "unknown endpoint"})

    def parse_uri(self):
        """Function parsing URI."""
        parsed = urlparse(self.path)
        path = parsed.path
        parts = path.strip("/").split("/")
        params = parse_qs(parsed.query)
        return path, parts, params

    @staticmethod
    def get_missing_params(params):
        missing_params = [name for name in REQUIRED_PARAMS if params.get(name, None) is None]
        return missing_params

    @staticmethod
    def ukraine_time_to_utc_str(dt_str):
        # строка без timezone, например "2029-04-24T21:46:41"
        dt_local = datetime.fromisoformat(dt_str)

        # говорим Python, что это время в украинской timezone
        dt_local = dt_local.replace(tzinfo=ZoneInfo("Europe/Kyiv"))

        # переводим в UTC
        dt_utc = dt_local.astimezone(timezone.utc)

        # формат для InfluxDB
        return dt_utc.isoformat().replace("+00:00", "Z")

    @staticmethod
    def get_machine_productivity(machine_name, start_timestamp, end_timestamp, bucket):
        client = InfluxDBClient3(
            host=INFLUX_HOST,
            token=INFLUX_TOKEN,
            database=INFLUX_DB,
        )

        query = f"""
            SELECT
                DATE_BIN(INTERVAL '{bucket}', time) + INTERVAL '{bucket}' AS end_bucket_timestamp,

                selector_last(ltp_1, time)['value'] - selector_first(ltp_1, time)['value'] AS ws1_l,
                selector_last(rtp_1, time)['value'] - selector_first(rtp_1, time)['value'] AS ws1_r,

                selector_last(ltp_2, time)['value'] - selector_first(ltp_2, time)['value'] AS ws2_l,
                selector_last(rtp_2, time)['value'] - selector_first(rtp_2, time)['value'] AS ws2_r,

                selector_last(ltp_3, time)['value'] - selector_first(ltp_3, time)['value'] AS ws3_l,
                selector_last(rtp_3, time)['value'] - selector_first(rtp_3, time)['value'] AS ws3_r,

                selector_last(ltp_4, time)['value'] - selector_first(ltp_4, time)['value'] AS ws4_l,
                selector_last(rtp_4, time)['value'] - selector_first(rtp_4, time)['value'] AS ws4_r,

                selector_last(ltp_5, time)['value'] - selector_first(ltp_5, time)['value'] AS ws5_l,
                selector_last(rtp_5, time)['value'] - selector_first(rtp_5, time)['value'] AS ws5_r,

                selector_last(ltp_6, time)['value'] - selector_first(ltp_6, time)['value'] AS ws6_l,
                selector_last(rtp_6, time)['value'] - selector_first(rtp_6, time)['value'] AS ws6_r

            FROM metrics
            WHERE
                machine_name = '{machine_name}'
                AND time >= CAST('{start_timestamp}' AS TIMESTAMP)
                AND time <  CAST('{end_timestamp}' AS TIMESTAMP)
            GROUP BY
                DATE_BIN(INTERVAL '{bucket}', time)
            ORDER BY
                end_bucket_timestamp;
        """

        table = client.query(query=query)
        rows = table.to_pylist()
        client.close()
        return rows

    @staticmethod
    def rows_to_json_ready(rows):
        result = []

        for row in rows:
            row = dict(row)
            dt = row["end_bucket_timestamp"]

            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

            row["end_bucket_timestamp"] = (
                dt.astimezone(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )

            result.append(row)

        return result

    def send_json(self, status, data):
        """Function sending JSON."""
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def setup_logging():
    log_path = Path(__file__).resolve().parent / "http_server.log"

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


try:
    http_server = ThreadingHTTPServer((HOST, PORT), Handler)
    setup_logging()
    logging.info(f"HTTP server {HOST}:{PORT} was created")
    http_server.serve_forever()
except KeyboardInterrupt:
    logging.info(f"HTTP server {HOST}:{PORT} was stopped manually")
    http_server.server_close()
