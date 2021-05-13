#!/usr/bin/env python3

import argparse
import csv
import glob
import json
import logging
import re
import sys

import coloredlogs

logger = logging.getLogger("gen_csv")

regex = re.compile(r"(?:(?P<type>(?:timens|events)):)?(?P<name>[a-zA-Z0-9-_]+):(?P<amount>[\d.]+)")


def parse_lines(content):
    result = {}
    for line in content.splitlines():
        match = regex.search(line)
        if not match:
            continue
        typ = match.group("type")
        name = match.group("name")
        dur = match.group("amount")
        if typ == "events":
            continue
        result[name] = dur
    return result


def extract_dict(path):
    with open(path) as f:
        content = json.load(f)
    result = {
        name: value
        for name, value in content.items()
        if name not in ["hosts", "result"]
    }
    result.update(parse_lines(content["result"]["stdout"]))
    result.update(parse_lines(content["result"]["stderr"]))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("results_dir", help="Directory where the results are stored", nargs="+")
    parser.add_argument(
        "-o",
        help="CSV file where to write the results. - for stdout",
        dest="output_file",
        default="-",
        nargs="?",
    )

    args = parser.parse_args()

    coloredlogs.install(
        "INFO",
        milliseconds=True,
        logger=logger,
        fmt="%(asctime)s,%(msecs)03d %(levelname)s %(message)s",
    )

    files = []
    for path in args.results_dir:
        files += list(glob.glob(path + "/**/*.json", recursive=True))
    logger.info("%d JSON found", len(files))

    if not files:
        logger.fatal("No JSON found!")
        sys.exit(1)

    result = []
    columns = []

    for path in sorted(files):
        logger.info("Processing %s", path)
        file_data = extract_dict(path)
        for key in file_data.keys():
            if key not in columns:
                columns.append(key)
        result += [file_data]

    if args.output_file == "-":
        output_file = sys.stdout
    else:
        output_file = open(args.output_file, "w")
    writer = csv.DictWriter(output_file, columns)
    writer.writeheader()
    writer.writerows(result)
    output_file.close()
