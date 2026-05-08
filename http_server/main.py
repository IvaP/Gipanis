import json
import logging
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

from influxdb_client_3 import InfluxDBClient3

HOST = "0.0.0.0"
PORT = 8000
CLIENT_TOKEN = "yT4oJneVwBaQMJeO8TiUN1qycBf9AGem"
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
            auth_header = self.headers.get("Authorization")

            if auth_header != "Bearer " + CLIENT_TOKEN:
                self.send_json(401, "unauthorized")
            else:
                if missing_params := self.get_missing_params(params):
                    self.send_json(400, {
                        "error": f"missing required parameters: {', '.join(missing_params)}"
                    })
                else:
                    machine_name = parts[1].lower()
                    utc_start = params["start"][0]
                    utc_end = params["end"][0]
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

    def get_machine_productivity(self, machine_name, start_timestamp, end_timestamp, bucket):
        start_dt = self.parse_utc_timestamp(start_timestamp)
        end_dt = self.parse_utc_timestamp(end_timestamp)

        sql_bucket, bucket_delta = self.normalize_bucket(bucket)

        # Например: по 24 бакета за один запрос.
        # Для bucket=1h это будет 24 часа.
        # Для bucket=30m это будет 12 часов.
        chunk_bucket_count = 24
        chunk_delta = bucket_delta * chunk_bucket_count

        # Сначала создаём все ожидаемые бакеты с нулями.
        # Это решает проблему:
        # - пустых бакетов внутри диапазона
        # - полного отсутствия данных по машине
        result_by_timestamp = self.make_empty_productivity_rows_dict(
            start_dt,
            end_dt,
            bucket_delta,
        )

        client = InfluxDBClient3(
            host=INFLUX_HOST,
            token=INFLUX_TOKEN,
            database=INFLUX_DB,
        )

        try:
            current_start = start_dt

            while current_start < end_dt:
                current_end = current_start + chunk_delta

                if current_end > end_dt:
                    current_end = end_dt

                chunk_rows = self.query_machine_productivity_chunk(
                    client=client,
                    machine_name=machine_name,
                    start_dt=current_start,
                    end_dt=current_end,
                    sql_bucket=sql_bucket,
                )

                for row in chunk_rows:
                    row = dict(row)

                    dt = row["end_bucket_timestamp"]

                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)

                    dt = dt.astimezone(timezone.utc)

                    # Если InfluxDB вернул бакет — заменяем нулевую строку реальными данными.
                    result_by_timestamp[dt] = row

                current_start = current_end

        finally:
            client.close()

        rows = []

        for dt in sorted(result_by_timestamp.keys()):
            rows.append(result_by_timestamp[dt])

        return rows

    def query_machine_productivity_chunk(self, client, machine_name, start_dt, end_dt, sql_bucket):
        start_str = self.datetime_to_utc_str(start_dt)
        end_str = self.datetime_to_utc_str(end_dt)

        # Минимальная защита от одинарной кавычки в имени машины
        machine_name_sql = machine_name.replace("'", "''")

        query = f"""
            SELECT
                DATE_BIN(INTERVAL '{sql_bucket}', time) + INTERVAL '{sql_bucket}' AS end_bucket_timestamp,

                COALESCE(selector_last(ltp_1, time)['value'] - selector_first(ltp_1, time)['value'], 0) AS ws1_l,
                COALESCE(selector_last(rtp_1, time)['value'] - selector_first(rtp_1, time)['value'], 0) AS ws1_r,

                COALESCE(selector_last(ltp_2, time)['value'] - selector_first(ltp_2, time)['value'], 0) AS ws2_l,
                COALESCE(selector_last(rtp_2, time)['value'] - selector_first(rtp_2, time)['value'], 0) AS ws2_r,

                COALESCE(selector_last(ltp_3, time)['value'] - selector_first(ltp_3, time)['value'], 0) AS ws3_l,
                COALESCE(selector_last(rtp_3, time)['value'] - selector_first(rtp_3, time)['value'], 0) AS ws3_r,

                COALESCE(selector_last(ltp_4, time)['value'] - selector_first(ltp_4, time)['value'], 0) AS ws4_l,
                COALESCE(selector_last(rtp_4, time)['value'] - selector_first(rtp_4, time)['value'], 0) AS ws4_r,

                COALESCE(selector_last(ltp_5, time)['value'] - selector_first(ltp_5, time)['value'], 0) AS ws5_l,
                COALESCE(selector_last(rtp_5, time)['value'] - selector_first(rtp_5, time)['value'], 0) AS ws5_r,

                COALESCE(selector_last(ltp_6, time)['value'] - selector_first(ltp_6, time)['value'], 0) AS ws6_l,
                COALESCE(selector_last(rtp_6, time)['value'] - selector_first(rtp_6, time)['value'], 0) AS ws6_r

            FROM metrics
            WHERE
                machine_name = '{machine_name_sql}'
                AND time >= CAST('{start_str}' AS TIMESTAMP)
                AND time <  CAST('{end_str}' AS TIMESTAMP)
            GROUP BY
                DATE_BIN(INTERVAL '{sql_bucket}', time)
            ORDER BY
                end_bucket_timestamp;
        """

        table = client.query(query=query)
        return table.to_pylist()

    def make_empty_productivity_rows_dict(self, start_dt, end_dt, bucket_delta):
        rows = {}

        current = self.floor_datetime_to_bucket(start_dt, bucket_delta)

        while current < end_dt:
            end_bucket = current + bucket_delta

            # Бакет должен хотя бы частично попадать в пользовательский диапазон
            if end_bucket > start_dt:
                rows[end_bucket] = {
                    "end_bucket_timestamp": end_bucket,
                    "ws1_l": 0,
                    "ws1_r": 0,
                    "ws2_l": 0,
                    "ws2_r": 0,
                    "ws3_l": 0,
                    "ws3_r": 0,
                    "ws4_l": 0,
                    "ws4_r": 0,
                    "ws5_l": 0,
                    "ws5_r": 0,
                    "ws6_l": 0,
                    "ws6_r": 0,
                }

            current = end_bucket

        return rows

    @staticmethod
    def normalize_bucket(bucket):
        bucket = bucket.strip().lower()

        if bucket not in bucket_map:
            raise ValueError(f"Unsupported bucket: {bucket}")

        return bucket_map[bucket]

    @staticmethod
    def parse_utc_timestamp(timestamp_str):
        if timestamp_str.endswith("Z"):
            timestamp_str = timestamp_str[:-1] + "+00:00"

        dt = datetime.fromisoformat(timestamp_str)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)

    @staticmethod
    def datetime_to_utc_str(dt):
        return (
            dt.astimezone(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )

    @staticmethod
    def floor_datetime_to_bucket(dt, bucket_delta):
        dt = dt.astimezone(timezone.utc)

        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)

        seconds = int((dt - epoch).total_seconds())
        bucket_seconds = int(bucket_delta.total_seconds())

        floored_seconds = seconds - (seconds % bucket_seconds)

        return epoch + timedelta(seconds=floored_seconds)

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


bucket_map = {
    "1m": ("1 minute", timedelta(minutes=1)),
    "1 minute": ("1 minute", timedelta(minutes=1)),

    "15m": ("15 minutes", timedelta(minutes=15)),
    "15 minutes": ("15 minutes", timedelta(minutes=15)),

    "30m": ("30 minutes", timedelta(minutes=30)),
    "30 minutes": ("30 minutes", timedelta(minutes=30)),

    "1h": ("1 hour", timedelta(hours=1)),
    "1 hour": ("1 hour", timedelta(hours=1)),

    "2h": ("2 hours", timedelta(hours=2)),
    "2 hours": ("2 hours", timedelta(hours=2)),

    "6h": ("6 hours", timedelta(hours=6)),
    "6 hours": ("6 hours", timedelta(hours=6)),

    "12h": ("12 hours", timedelta(hours=12)),
    "12 hours": ("12 hours", timedelta(hours=12)),

    "1d": ("1 day", timedelta(days=1)),
    "1 day": ("1 day", timedelta(days=1)),
}

try:
    http_server = ThreadingHTTPServer((HOST, PORT), Handler)
    setup_logging()
    logging.info(f"HTTP server {HOST}:{PORT} was created")
    http_server.serve_forever()
except KeyboardInterrupt:
    logging.info(f"HTTP server {HOST}:{PORT} was stopped manually")
    http_server.server_close()
