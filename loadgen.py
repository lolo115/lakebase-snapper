#!/usr/bin/env python3
"""Tiny concurrent load generator for testing lakebase_snapper against a Lakebase endpoint.

Opens N worker connections that loop over a mix of CPU-bound aggregates, temp-spilling
sorts, and pg_sleep()s (to create observable wait events) for a fixed duration.
Auth mirrors lakebase_snapper: Databricks CLI profile+endpoint, or LAKEBASE_SNAPPER_DSN.
"""
import os, sys, json, time, threading, subprocess, random
import psycopg

PROFILE = os.environ.get("LAKEBASE_SNAPPER_PROFILE")
ENDPOINT = os.environ.get("LAKEBASE_SNAPPER_ENDPOINT")
DBNAME = os.environ.get("LAKEBASE_SNAPPER_DBNAME", "postgres")
DSN = os.environ.get("LAKEBASE_SNAPPER_DSN")
WORKERS = int(sys.argv[1]) if len(sys.argv) > 1 else 4
DURATION = float(sys.argv[2]) if len(sys.argv) > 2 else 20.0


def cli(*cmd):
    out = subprocess.run(["databricks", *cmd, "--profile", PROFILE, "--output", "json"],
                         capture_output=True, text=True, check=True)
    return json.loads(out.stdout)


def connect():
    if DSN:
        return psycopg.connect(DSN, autocommit=True)
    branch = ENDPOINT.rsplit("/endpoints/", 1)[0]
    host = cli("postgres", "list-endpoints", branch)[0]["status"]["hosts"]["host"]
    user = cli("current-user", "me")["userName"]
    token = cli("postgres", "generate-database-credential", ENDPOINT)["token"]
    return psycopg.connect(host=host, port=5432, dbname=DBNAME, user=user,
                           password=token, sslmode="require", autocommit=True)


QUERIES = [
    # CPU-bound aggregate
    "SELECT count(*), avg(g), sum(g::bigint*g) FROM generate_series(1, 500000) g",
    # temp-spilling sort
    "SELECT g FROM generate_series(1, 300000) g ORDER BY md5(g::text) LIMIT 10",
    # a deliberate wait
    "SELECT pg_sleep(0.4)",
    # hashy self-join
    "SELECT count(*) FROM generate_series(1,20000) a JOIN generate_series(1,20000) b ON a=b",
]


def worker(wid, stop_at):
    conn = connect()
    conn.execute(f"SET application_name = 'loadgen-{wid}'")
    n = 0
    while time.time() < stop_at:
        q = random.choice(QUERIES)
        try:
            conn.execute(q).fetchall()
            n += 1
        except Exception as e:
            print(f"[w{wid}] {e}", file=sys.stderr)
            time.sleep(0.5)
    conn.close()
    print(f"[w{wid}] ran {n} queries")


def main():
    stop_at = time.time() + DURATION
    threads = [threading.Thread(target=worker, args=(i, stop_at)) for i in range(WORKERS)]
    print(f"loadgen: {WORKERS} workers for {DURATION}s")
    for t in threads: t.start()
    for t in threads: t.join()
    print("loadgen done")


if __name__ == "__main__":
    main()
