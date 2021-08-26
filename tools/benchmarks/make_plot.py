#!/usr/bin/env python3

import argparse

import matplotlib.pyplot as plt
import numpy as np

from aggregate_csv import get_systems, parse_stdin


def get_ids(systems):
    ids = set()
    for system in systems:
        ids |= system.row_ids()
    ids = list(sorted(ids))
    cores = [a[0] * a[1] for a in ids]
    assert len(cores) == len(set(cores)), "Number of cores is not unique"
    return ids, cores


def make_time_plot(args, systems):
    ids, cores = get_ids(systems)
    for system in systems:
        exp = system.get_experiments()
        for experiment in exp:
            x = []
            times = []
            errors = []
            for num_cores, id in zip(cores, ids):
                data = system.experiments[experiment].get_data(id)
                if data is not None:
                    x.append(num_cores)
                    times.append(data[0])
                    errors.append(data[1])

            if len(exp) == 1:
                label = system.system
            else:
                label = f"{system.system} ({experiment})"
            plt.errorbar(
                x,
                times,
                yerr=errors,
                capsize=3,
                label=label,
            )

    plt.xlabel("Number of cores")
    plt.xticks(cores)
    plt.grid()
    plt.ylabel("Execution time (s)")


def make_throughput_plot(args, systems):
    ids, cores = get_ids(systems)
    assert args.size is not None, "throughput requires --size"
    for system in systems:
        exp = system.get_experiments()
        for experiment in exp:
            throughput = []
            ideal_slope = None
            for num_cores, id in zip(cores, ids):
                data = system.experiments[experiment].get_data(id)
                if data is None:
                    throughput.append(None)
                else:
                    throughput.append(args.size / data[0])
                    if ideal_slope is None:
                        ideal_slope = args.size / data[0] / num_cores
            ideal = [core * ideal_slope for core in cores]

            if len(exp) == 1:
                label = system.system
            else:
                label = f"{system.system} ({experiment})"
            l = plt.plot(
                cores,
                throughput,
                label=label,
            )
            color = l[0].get_color()
            if args.ideal:
                plt.plot(
                    cores,
                    ideal,
                    color=color,
                    linestyle="--",
                    linewidth=0.5,
                )

    plt.xlabel("Number of cores")
    plt.xticks(cores)
    plt.grid()
    if args.unit:
        plt.ylabel(f"Throughput ({args.unit}/s)")
    else:
        plt.ylabel("Throughput")


def make_scaling_plot(args, systems):
    ids, cores = get_ids(systems)
    for system in systems:
        exp = system.get_experiments()
        for experiment in exp:
            baseline_t = None
            baseline_c = None
            for num_cores, id in zip(cores, ids):
                data = system.experiments[experiment].get_data(id)
                if data is not None:
                    baseline_t = data[0]
                    baseline_c = num_cores
                    break

            x = []
            scale = []
            error = []
            for num_cores, id in zip(cores, ids):
                data = system.experiments[experiment].get_data(id)
                if data is not None:
                    s = (1 / data[0]) / ((1 / baseline_t) / baseline_c) / cores[0]
                    e = s - (
                        (1 / (data[0] - data[1]))
                        / ((1 / baseline_t) / baseline_c)
                        / cores[0]
                    )
                    x.append(num_cores)
                    scale.append(s)
                    error.append(e)

            if len(exp) == 1:
                label = system.system
            else:
                label = f"{system.system} ({experiment})"
            plt.errorbar(
                x,
                scale,
                yerr=error,
                capsize=3,
                label=label,
            )
    if args.ideal:
        ideal = [c / cores[0] for c in cores]
        plt.plot(
            cores,
            ideal,
            color="black",
            linestyle="--",
            linewidth=0.5,
            label="ideal",
        )

    plt.xlabel("Number of cores")
    plt.xticks(cores)
    plt.grid()
    plt.ylabel(f"Speedup")


def main(args):
    systems = get_systems()
    parse_stdin(systems)

    experiments = set()
    for system in systems:
        experiments |= system.get_experiments()
    experiments = list(sorted(experiments))

    if args.title:
        title = args.title
    else:
        title = " ".join(experiments)

    if args.variant == "time":
        make_time_plot(args, systems)
    elif args.variant == "throughput":
        make_throughput_plot(args, systems)
    elif args.variant == "scaling":
        make_scaling_plot(args, systems)
    else:
        raise ValueError(f"Unknown variant: {args.variant}")

    plt.title(title)
    plt.legend()
    if args.output:
        plt.savefig(args.output)
    else:
        plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Aggregate the output of gen_csv.py and produce a plot"
    )
    parser.add_argument("variant", choices=["time", "throughput", "scaling"])
    parser.add_argument("--output", "-o", help="Where to save the plot")
    parser.add_argument("--title", "-t", help="Title of the plot")
    parser.add_argument(
        "--size",
        "-s",
        help="Size of the dataset (for thoughput)",
        type=int,
    )
    parser.add_argument("--unit", "-u", help="Unit of the size")
    parser.add_argument("--ideal", help="Display the ideal line", action="store_true")
    args = parser.parse_args()
    main(args)
