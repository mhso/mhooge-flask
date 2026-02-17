import argparse
from datetime import datetime, timedelta
from glob import glob
import json
import re
from multiprocessing.pool import ThreadPool
from time import sleep

from . import logging

def get_log_timestamp(log_data):
    if isinstance(log_data, str):
        try:
            dt = datetime.fromisoformat(log_data.split(" ")[0])
            return dt.timestamp()
        except ValueError:
            return None

    return log_data["record"].get("time", {}).get("timestamp", None)

def search_regex(text, pattern):
    match = re.search(pattern, text)

    if text.strip() == "":
        return None

    return None if match is None else match.group(0)

def search_for_key(record, key, value):
    nested_keys = key.split(".")

    unpacked_value = record

    key_in_container = True

    for nested_key in nested_keys:
        if unpacked_value is None or nested_key not in unpacked_value:
            key_in_container = False

        unpacked_value = unpacked_value.get(nested_key)

    if isinstance(unpacked_value, dict):
        raise ValueError(
            f"The key to search for: {key} is invalid. "
            "Maybe you need sub key (main.sub)?."
        )

    if not key_in_container:
        return None
    
    if value is None:
        return str(unpacked_value)

    if (part := search_regex(unpacked_value, value)) is not None:
        return part

    return None

def search_json(record, include_keys, exclude_keys):
    for key, value in exclude_keys:
        match_data = search_for_key(record, key, value)
        if match_data is not None:
            return None

    for key, value in include_keys:
        match_data = search_for_key(record, key, value)
        if match_data is not None:
            return match_data

    if include_keys == []:
        return str(record) 

    return None

def search_line(line, args):
    if line.strip() == "":
        return None

    log_record = None

    try:
        log_record = json.loads(line)
    except (json.JSONDecodeError, TypeError):
        return None

    if args.date_from is not None or args.date_to is not None:
        time_from = args.date_from
        time_to = args.date_to
        log_date = get_log_timestamp(log_record if log_record is not None else line)

        # Check if log record falls within specified timeframe.
        if (
            log_date is None
            or (time_from is not None and log_date < time_from)
            or (time_to is not None and log_date > time_to)
        ):
            return None

    if args.include or args.exclude:
        return search_json(log_record["record"], args.include, args.exclude)
    elif args.regex is not None:
        return search_regex(line, args.regex)

    return line

def datetime_type(iso_string, max_date):
    try:
        dt = datetime.fromisoformat(iso_string)
        if len(iso_string) <= 10:
            if max_date:
                dt = dt.replace(hour=23, minute=59, second=59)

        return (dt - timedelta(hours=1)).timestamp()
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid ISO datetime string")

def key_value_type(kv_string):
    split = kv_string.split("=")

    if len(split) == 1:
        return kv_string, None
    if len(split) == 2:
        return tuple(split)

    raise argparse.ArgumentTypeError("Invalid key-value string")

def process_file(log_file, args):
    found_lines = []
    with open(log_file, "r", encoding="utf-8") as fp:
        for line in fp:
            if (found_part := search_line(line, args)) is not None:
                found_lines.append((line, found_part))

    return found_lines

def main():
    log_path = logging.LOG_FOLDER

    parser = argparse.ArgumentParser()

    parser.add_argument("log_file", type=str)
    parser.add_argument("--include", nargs="+", default=[], type=key_value_type)
    parser.add_argument("--exclude", nargs="+", default=[], type=key_value_type)
    parser.add_argument("--regex", type=str)
    parser.add_argument("--date-from", type=lambda x: datetime_type(x, False), help="ISO format datetime string")
    parser.add_argument("--date-to", type=lambda x: datetime_type(x, True), help="ISO format datetime string")

    args = parser.parse_args()

    log_files = glob(f"{log_path}/{args.log_file}*.log")

    futures = []
    with ThreadPool(32) as pool:
        for log_file in log_files:
            future = pool.apply_async(process_file, (log_file,args))
            futures.append(future)

        while any(not future.ready() for future in futures):
            sleep(0.1)

    pool.join()

    found_lines = []
    for future in futures:
        found_lines.extend(future.get())

    for line, found_part in found_lines:
        new_line = ""
        for c in line:
            try:
                c.encode("cp1252")
                new_line += c
            except UnicodeEncodeError:
                pass

        try:
            to_json = json.loads(new_line)
            new_line = json.dumps(to_json, indent=4)
        except json.JSONDecodeError:
            pass

        new_line = new_line.replace("<", "\\<")
        new_line = new_line.replace(found_part, f"<yellow>{found_part}</yellow>")

        logging.logger.bind(ignore=True).opt(colors=True).info(new_line)

if __name__ == "__main__":
    main()
