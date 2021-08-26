#!/usr/bin/env python3

import argparse
import csv
import sys
from typing import Dict


class Experiment:
    def __init__(self, name):
        self.name = name
        self.times = {}

    def add(self, row, column_name):
        hosts = int(row["num_hosts"])
        cores = int(row["procs_per_host"])
        time = float(row[column_name]) / 10 ** 9
        self.times.setdefault((hosts, cores), []).append(time)

    def row_ids(self):
        return set(self.times.keys())

    def get_data(self, row_id):
        if row_id not in self.times:
            return None
        data = self.times[row_id]
        delta = (max(data) - min(data)) / 2
        avg = sum(data) / len(data)
        return avg, delta

    def get_row(self, row_id, human):
        if row_id not in self.times:
            if human:
                return [""]
            else:
                return ["", ""]
        avg, delta = self.get_data(row_id)
        if human:
            return [f"{avg:.2f}s (± {delta:.2f}s)"]
        else:
            return [str(avg), str(delta)]


class System:
    def __init__(self, system, time_column_name):
        self.system = system
        self.time_column_name = time_column_name
        self.experiments = {}  # type: Dict[str, Experiment]

    def add(self, row):
        exp = row["experiment"]
        if exp not in self.experiments:
            self.experiments[exp] = Experiment(exp)
        self.experiments[exp].add(row, self.time_column_name)

    def header(self, experiment, single_experiment, human):
        if experiment not in self.experiments:
            return []

        if single_experiment:
            if human:
                return [self.system]
            else:
                return [f"{self.system} (s)", f"{self.system} (± s)"]
        else:
            if human:
                return [f"{experiment} ({self.system})"]
            else:
                return [f"{experiment} ({self.system}) ({h}s)" for h in ["", "± "]]

    def get_row(self, experiment, row_id, human):
        if experiment not in self.experiments:
            return []
        return self.experiments[experiment].get_row(row_id, human)

    def get_experiments(self):
        return set(self.experiments.keys())

    def row_ids(self):
        ids = set()
        for exp in self.experiments.values():
            ids |= exp.row_ids()
        return ids


class RStream1(System):
    def __init__(self):
        System.__init__(self, "rstream1", "Run")


class RStream2(System):
    def __init__(self):
        System.__init__(self, "rstream2", "max-remote-execution")


class Flink(System):
    def __init__(self):
        System.__init__(self, "flink", "total")


class MPI(System):
    def __init__(self):
        System.__init__(self, "mpi", "total")


def get_systems():
    return [RStream1(), RStream2(), Flink(), MPI()]


def parse_stdin(systems):
    for row in csv.DictReader(sys.stdin):
        system = row["system"]
        if system == "rstream":
            systems[0].add(row)
        elif system == "rstream2":
            systems[1].add(row)
        elif system == "flink":
            systems[2].add(row)
        elif system == "mpi":
            systems[3].add(row)
        else:
            raise ValueError("Unsupported system: " + system)


def main(args):
    systems = get_systems()
    parse_stdin(systems)

    experiments = set()
    for system in systems:
        experiments |= system.get_experiments()
    experiments = list(sorted(experiments))
    single_experiment = len(experiments) == 1

    headers = ["hosts", "cores"]
    for experiment in experiments:
        for system in systems:
            headers += system.header(experiment, single_experiment, not args.no_human)

    ids = set()
    for system in systems:
        ids |= system.row_ids()
    ids = list(sorted(ids))

    writer = csv.writer(sys.stdout)
    writer.writerow(headers)
    for row_id in ids:
        row = [row_id[0], row_id[1]]
        for experiment in experiments:
            for system in systems:
                row += system.get_row(experiment, row_id, not args.no_human)
        writer.writerow(row)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aggregate the output of gen_csv.py")
    parser.add_argument(
        "--no-human", action="store_true", help="Do not print human-friendly values"
    )
    args = parser.parse_args()
    main(args)
