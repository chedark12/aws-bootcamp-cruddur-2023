# ============================================================
# helpers.py
# Utility methods for ConfigManager:
# - Logging
# - JSON/CSV file loading
# - Error/return builders
# - ID conversion
# - Dynamic parameter file processing
# - Date parameter processing
# ============================================================
import json
import csv
import logging
from pathlib import Path
from datetime import datetime
from constants import (
    RETURN_NORMAL,
    RETURN_WARNING,
    RETURN_ABNORMAL,
    MESSAGE_COMM0001I,
    MESSAGE_COMM0016E,
    MESSAGE_COMM0017E,
    MESSAGE_COMM0018E,
    MESSAGE_COMM0019E,
)
# ------------------------------------------------------------
# Helper: timestamp for log filename
# ------------------------------------------------------------
def get_timestamp():
    return datetime.now().strftime("%Y%m%d_%H%M%S")
# ------------------------------------------------------------
# Helper: write log line to MST log file AND console
# ------------------------------------------------------------
def write_log(log_file_path: Path, message_id: str, log_level: str, message: str):
    """
    Writes log entries to BOTH:
    (1) The .MSS.log file
    (2) The console (stdout)
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"{timestamp} [{log_level}] {message_id} - {message}"
    # Write to console
    print(log_line)
    # Ensure directory exists
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    # Write to file
    with open(log_file_path, "a", encoding="utf-8") as f:
        f.write(log_line + "\n")
# ------------------------------------------------------------
# Helper: JSON loader with validation (Step 4.2.2)
# ------------------------------------------------------------
def load_json_file(file_path: Path):
    """
    Attempts to load a JSON file.
    Raises JSONDecodeError if invalid.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)
# ------------------------------------------------------------
# Helper: CSV loader with key/value format
# ------------------------------------------------------------
def load_csv_keyvalue(file_path: Path, key_field="key", value_field="value"):
    """
    Loads a CSV into a dict {key: value}.
    """
    result = {}
    with open(file_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if key_field in row and row[key_field] != "":
                result[row[key_field]] = row.get(value_field)
    return result
# ------------------------------------------------------------
# Helper: Exception Builder (used for Steps 4.2.x, 5.2.x, 5.3.x, 5.4.x)
# ------------------------------------------------------------
def build_exception_response(
    message_id: str,
    log_level: str,
    log_file_path: Path,
    exception_content: str,
    message: str,
):
    """
    Builds the standardized return object for abnormal termination (return_code=1 or 8),
    writes a log entry, and returns the dict.
    """
    # Write log to MST + console
    write_log(log_file_path, message_id, log_level, message)
    # Abnormal end
    return {
        "return_code": RETURN_ABNORMAL if log_level == "Error" else RETURN_WARNING,
        "value": None,
        "message": message,
        "exception_content": exception_content,
        "log_file": str(log_file_path),
    }
# ------------------------------------------------------------
# ID Conversion Helper (Step 5.2)
# ------------------------------------------------------------
def convert_internal_id(
    file_path: Path,
    original_value: str,
):
    """
    Reads COMF001.csv and returns internal ID mapping.
    """
    with open(file_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("external_id") == str(original_value):
                return row.get("internal_id")
    return None
# ------------------------------------------------------------
# Dynamic Parameter File Helper (Step 5.3)
# ------------------------------------------------------------
def load_dynamic_parameter_file(file_path: Path):
    """
    Loads the DYNAMIC_<JOBID>.csv into a key-value dictionary.
    """
    return load_csv_keyvalue(file_path)
def update_dynamic_parameter_file(file_path: Path, prev_exec_date: str):
    """
    Updates dynamic parameter file with:
    - previous execution date
    - current execution date
    (Steps 5.3.1.2.1 to 5.3.1.2.3)
    """
    current_exec_date = datetime.now().strftime("%Y%m%d%H%M%S")
    # Load existing dynamic CSV
    data = {}
    if file_path.exists():
        with open(file_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data[row.get("key")] = row.get("value")
    # Update fields
    data["previous_execution_date"] = prev_exec_date
    data["current_execution_date"] = current_exec_date
    # Write back updated CSV
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        fieldnames = ["key", "value"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for k, v in data.items():
            writer.writerow({"key": k, "value": v})
# ------------------------------------------------------------
# Date Parameter Helper (Step 5.4)
# ------------------------------------------------------------
def load_date_parameter_mapping(file_path: Path):
    """Loads BASEDATE.csv to a dict like: {'BASEDATE_': '20260320'}"""
    return load_csv_keyvalue(file_path)
def replace_date_template(date_value: str, actual_date: str):
    """
    Replaces template placeholders inside the date parameter value.
    Example:
        "BASEDATE_" → "20260320"
    """
    return date_value.replace("BASEDATE_", actual_date)
# ------------------------------------------------------------
# Helper: Build success response
# ------------------------------------------------------------
def build_success_response(value, log_file_path: Path):
    """
    Builds standardized success response (return_code=0).
    """
    return {
        "return_code": RETURN_NORMAL,
        "value": value,
        "message": "Normal End",
        "log_file": str(log_file_path),
    }