#!/usr/bin/env python3
"""Manage the Feldera benchmark pipeline lifecycle via REST API."""

import json
import os
import sys
import time
import urllib.request
import urllib.error

PIPELINE_NAME = "benchmark"


def load_dotenv():
    """Read .env file from repo root and populate os.environ for missing keys."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(script_dir, "..", "..", ".env")
    if not os.path.isfile(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, sep, value = line.partition("=")
            if sep and key:
                os.environ.setdefault(key.strip(), value)


load_dotenv()

FELDERA_HOST = os.environ.get("FELDERA_HOST", "http://localhost:8090")
EVENTHUB_CONNECTION_STRING = os.environ.get("EVENTHUB_CONNECTION_STRING", "")
EVENTHUB_NAME = os.environ.get("EVENTHUB_NAME", "benchmark")
ADLSG2_ACCOUNT_NAME = os.environ.get("ADLSG2_ACCOUNT_NAME", "")
ADLSG2_ACCOUNT_KEY = os.environ.get("ADLSG2_ACCOUNT_KEY", "")
ADLSG2_CONTAINER = os.environ.get("ADLSG2_CONTAINER", "benchmark")
FELDERA_WORKERS = int(os.environ.get("FELDERA_WORKERS", "16"))

BASE_URL = f"{FELDERA_HOST}/v0/pipelines/{PIPELINE_NAME}"


def api_request(url, method="GET", body=None):
    """Send an HTTP request and return (status_code, parsed_json | raw_text)."""
    headers = {"Content-Type": "application/json"} if body is not None else {}
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            text = resp.read().decode()
            try:
                return resp.status, json.loads(text)
            except json.JSONDecodeError:
                return resp.status, text
    except urllib.error.HTTPError as e:
        text = e.read().decode()
        try:
            return e.code, json.loads(text)
        except json.JSONDecodeError:
            return e.code, text


def extract_namespace(connection_string):
    """Extract the namespace from an Event Hubs connection string."""
    for part in connection_string.split(";"):
        if part.startswith("Endpoint="):
            # Endpoint=sb://<namespace>.servicebus.windows.net/
            endpoint = part.split("=", 1)[1]
            host = endpoint.replace("sb://", "").rstrip("/")
            return host.split(".")[0]
    raise ValueError("Cannot extract namespace from connection string")


def build_sql():
    """Build the SQL program with embedded connector configs."""
    namespace = extract_namespace(EVENTHUB_CONNECTION_STRING)

    kafka_connectors = json.dumps([{
        "transport": {
            "name": "kafka_input",
            "config": {
                "topic": EVENTHUB_NAME,
                "start_from": "latest",
                "bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": "$ConnectionString",
                "sasl.password": EVENTHUB_CONNECTION_STRING
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw"
            }
        }
    }])

    delta_connectors = json.dumps([{
        "transport": {
            "name": "delta_table_output",
            "config": {
                "uri": f"abfss://{ADLSG2_CONTAINER}@{ADLSG2_ACCOUNT_NAME}.dfs.core.windows.net/feldera/benchmark",
                "mode": "truncate",
                "azure_storage_account_name": ADLSG2_ACCOUNT_NAME,
                "azure_storage_account_key": ADLSG2_ACCOUNT_KEY
            }
        },
        "enable_output_buffer": True,
        "max_output_buffer_time_millis": 1000
    }])

    sql = (
        f"CREATE TABLE events (\n"
        f"    ts TIMESTAMP NOT NULL,\n"
        f"    producer_id INT,\n"
        f"    seq BIGINT\n"
        f") WITH (\n"
        f"    'connectors' = '{kafka_connectors}'\n"
        f");\n"
        f"\n"
        f"CREATE VIEW benchmark_output\n"
        f"WITH (\n"
        f"    'connectors' = '{delta_connectors}'\n"
        f") AS SELECT ts FROM events;"
    )
    return sql


def cmd_create():
    sql = build_sql()
    body = {
        "name": PIPELINE_NAME,
        "description": "Stream processing benchmark - Feldera",
        "program_code": sql,
        "program_config": {"profile": "optimized"},
        "runtime_config": {"workers": FELDERA_WORKERS}
    }
    print(f"Creating pipeline '{PIPELINE_NAME}' ...")
    status, result = api_request(BASE_URL, method="PUT", body=body)
    print(f"HTTP {status}: {json.dumps(result, indent=2) if isinstance(result, (dict, list)) else result}")


def cmd_wait_compiled():
    print(f"Waiting for pipeline '{PIPELINE_NAME}' to compile ...", end="", flush=True)
    deadline = time.time() + 600
    while time.time() < deadline:
        status, data = api_request(BASE_URL)
        if status != 200:
            print(f"\nError polling pipeline: HTTP {status}")
            sys.exit(1)
        program_status = data.get("program_status", "")
        if program_status == "Success":
            print(" compiled!")
            sys.exit(0)
        if program_status in ("SqlError", "RustError", "SystemError"):
            print(f"\nCompilation failed ({program_status}):")
            error = data.get("program_error")
            print(json.dumps(error, indent=2) if error else "No error details available")
            sys.exit(1)
        print(".", end="", flush=True)
        time.sleep(5)
    print("\nTimeout: compilation did not complete within 600 seconds")
    sys.exit(1)


def cmd_start():
    print(f"Starting pipeline '{PIPELINE_NAME}' ...")
    status, result = api_request(f"{BASE_URL}/start?initial=running", method="POST")
    if status >= 400:
        print(f"Start request failed: HTTP {status}")
        print(json.dumps(result, indent=2) if isinstance(result, (dict, list)) else result)
        sys.exit(1)
    print("Start request accepted, waiting for Running status ...", end="", flush=True)
    deadline = time.time() + 120
    while time.time() < deadline:
        status, data = api_request(BASE_URL)
        if status != 200:
            print(f"\nError polling pipeline: HTTP {status}")
            sys.exit(1)
        deployment_status = data.get("deployment_status", "")
        if deployment_status == "Running":
            print(" running!")
            sys.exit(0)
        print(".", end="", flush=True)
        time.sleep(2)
    print("\nTimeout: pipeline did not reach Running state within 120 seconds")
    sys.exit(1)


def cmd_stop():
    print(f"Stopping pipeline '{PIPELINE_NAME}' ...")
    status, result = api_request(f"{BASE_URL}/stop?force=true", method="POST")
    if status >= 400:
        print(f"Stop request failed: HTTP {status}")
        print(json.dumps(result, indent=2) if isinstance(result, (dict, list)) else result)
        sys.exit(1)
    print("Stop request accepted, waiting for Stopped status ...", end="", flush=True)
    deadline = time.time() + 120
    while time.time() < deadline:
        status, data = api_request(BASE_URL)
        if status != 200:
            print(f"\nError polling pipeline: HTTP {status}")
            sys.exit(1)
        deployment_status = data.get("deployment_status", "")
        if deployment_status == "Stopped":
            print(" stopped!")
            sys.exit(0)
        print(".", end="", flush=True)
        time.sleep(2)
    print("\nTimeout: pipeline did not reach Stopped state within 120 seconds")
    sys.exit(1)


def cmd_delete():
    print(f"Deleting pipeline '{PIPELINE_NAME}' ...")
    status, result = api_request(BASE_URL, method="DELETE")
    print(f"HTTP {status}: {json.dumps(result, indent=2) if isinstance(result, (dict, list)) else result}")


COMMANDS = {
    "create": cmd_create,
    "wait-compiled": cmd_wait_compiled,
    "start": cmd_start,
    "stop": cmd_stop,
    "delete": cmd_delete,
}

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in COMMANDS:
        print(f"Usage: {sys.argv[0]} <command>")
        print(f"Commands: {', '.join(COMMANDS)}")
        sys.exit(1)
    COMMANDS[sys.argv[1]]()
