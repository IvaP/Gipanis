from datetime import datetime, timedelta, timezone

from influxdb_client_3 import InfluxDBClient3, Point, WritePrecision

# ===== SOURCE: откуда копируем =====
SRC_HOST = "http://gipanis.pp.ua:8282"
SRC_DB = "gipanis"
SRC_TABLE = "metrics"
SRC_TOKEN = "apiv3_NUIMypuZlWJ-LNHFGxPh5qle2RZ1vYeWTlC0LolZL5vrAgNDiUULkRez6O5pK6TZBf1TAiAnjxMYuWuMjfegxg"

# ===== DESTINATION: куда копируем =====
DST_HOST = "https://db.gipanis.pp.ua"
DST_DB = "gipanis"
DST_TABLE = "metrics"
DST_TOKEN = "apiv3_QVe-TMB1BUbdeBC3bRugTAx9psOk6DZDv-2Nely7sqWj4PyQnDUuBLCJHta5jzkiMKztJBdiYRxhp_GRIj71iw"

# Диапазон миграции задаём вручную,
# чтобы не выполнять MIN(time), MAX(time) по всей таблице.
START_TIME = "2025-06-13T00:00:00Z"
END_TIME = "2026-06-13T00:00:00Z"

CHUNK_HOURS = 12
WRITE_BATCH_SIZE = 500

TAG_COLUMNS = {"machine_name"}


def parse_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def dt_to_sql(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def read_chunk(client, start_dt, end_dt):
    query = f"""
        SELECT *
        FROM {SRC_TABLE}
        WHERE time >= CAST('{dt_to_sql(start_dt)}' AS TIMESTAMP)
          AND time <  CAST('{dt_to_sql(end_dt)}' AS TIMESTAMP)
        ORDER BY time
    """

    return client.query(query=query).to_pylist()


def row_to_point(row):
    point = Point(DST_TABLE)

    row_time = row["time"]

    for column, value in row.items():
        if column == "time" or value is None:
            continue

        if column in TAG_COLUMNS:
            point = point.tag(column, str(value))
        else:
            point = point.field(column, value)

    point = point.time(row_time, WritePrecision.NS)

    return point


def write_points(dst_client, points):
    if points:
        dst_client.write(points, write_precision="ns")


def make_chunks(start_dt, end_dt):
    chunks = []
    current_start = start_dt
    chunk_delta = timedelta(hours=CHUNK_HOURS)

    while current_start < end_dt:
        current_end = current_start + chunk_delta

        if current_end > end_dt:
            current_end = end_dt

        chunks.append((current_start, current_end))
        current_start = current_end

    return chunks


def main():
    src_client = InfluxDBClient3(
        host=SRC_HOST,
        token=SRC_TOKEN,
        database=SRC_DB,
    )

    dst_client = InfluxDBClient3(
        host=DST_HOST,
        token=DST_TOKEN,
        database=DST_DB,
    )

    try:
        start_dt = parse_time(START_TIME)
        end_dt = parse_time(END_TIME)

        if end_dt <= start_dt:
            print("Ошибка: END_TIME должен быть больше START_TIME.")
            return

        chunks = make_chunks(start_dt, end_dt)
        total_seconds = (end_dt - start_dt).total_seconds()

        print("Диапазон миграции:")
        print(f"  from: {dt_to_sql(start_dt)}")
        print(f"  to:   {dt_to_sql(end_dt)}")
        print(f"Количество чанков по {CHUNK_HOURS} часов: {len(chunks)}")

        copied_rows = 0

        for chunk_index, (chunk_start, chunk_end) in enumerate(chunks, start=1):
            print()
            print(f"Чанк {chunk_index}/{len(chunks)}:")
            print(f"  {dt_to_sql(chunk_start)} -> {dt_to_sql(chunk_end)}")

            rows = read_chunk(src_client, chunk_start, chunk_end)
            print(f"  прочитано строк: {len(rows)}")

            batch = []

            for row in rows:
                batch.append(row_to_point(row))

                if len(batch) >= WRITE_BATCH_SIZE:
                    write_points(dst_client, batch)
                    copied_rows += len(batch)
                    batch.clear()

            if batch:
                write_points(dst_client, batch)
                copied_rows += len(batch)

            done_seconds = (chunk_end - start_dt).total_seconds()
            percent = min(done_seconds * 100 / total_seconds, 100)

            print(f"  progress: {percent:.2f}%")
            print(f"  всего скопировано строк: {copied_rows}")

        print()
        print(f"Готово. Всего скопировано строк: {copied_rows}")

    finally:
        src_client.close()
        dst_client.close()


if __name__ == "__main__":
    main()
