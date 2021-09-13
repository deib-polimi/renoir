#!/usr/bin/env python3

import argparse

import matplotlib.pyplot as plt

from aggregate_csv import get_systems, parse_stdin

SYSTEM_NAMES = {
    "mpi": "MPI",
    "rstream1": "RStream",
    "rstream2": "Noir",
    "flink": "Flink",
    "timely": "Timely",
}

SYSTEM_COLORS = {
    "mpi": "blue",
    "rstream1": "red",
    "rstream2": "black",
    "flink": "orange",
    "timely": "green",
}


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

            color = SYSTEM_COLORS[system.system]
            label = SYSTEM_NAMES[system.system]
            linestyle = None
            if len(exp) > 1:
                if experiment == args.base_experiment:
                    label = SYSTEM_NAMES[system.system]
                elif experiment == args.extra_experiment:
                    label = f"{label} ({args.extra_experiment_name})"
                    linestyle = "--"
                else:
                    raise RuntimeError(
                        f"Experiment {experiment} not passed to --extra-experiment"
                    )
            plt.errorbar(
                x,
                times,
                yerr=errors,
                capsize=3,
                label=label,
                color=color,
                linestyle=linestyle,
            )

    plt.gca().set_ylim(bottom=0)
    plt.xlabel("Number of cores")
    plt.xticks(cores)
    plt.grid()
    plt.ylabel("Execution time (s)")


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

            color = SYSTEM_COLORS[system.system]
            label = SYSTEM_NAMES[system.system]
            linestyle = None
            if len(exp) > 1:
                if experiment == args.base_experiment:
                    label = SYSTEM_NAMES[system.system]
                elif experiment == args.extra_experiment:
                    label = f"{label} ({args.extra_experiment_name})"
                    linestyle = "--"
                else:
                    raise RuntimeError(
                        f"Experiment {experiment} not passed to --extra-experiment"
                    )
            plt.errorbar(
                x,
                scale,
                yerr=error,
                capsize=3,
                label=label,
                color=color,
                linestyle=linestyle,
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

    plt.rc("font", size=14)
    plt.rc("axes", titlesize=16)
    plt.rc("axes", labelsize=16)

    if args.variant == "time":
        make_time_plot(args, systems)
    elif args.variant == "scaling":
        make_scaling_plot(args, systems)
    else:
        raise ValueError(f"Unknown variant: {args.variant}")

    plt.title(title)
    plt.legend()
    if args.output:
        plt.tight_layout()
        plt.savefig(args.output)
    else:
        plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Aggregate the output of gen_csv.py and produce a plot"
    )
    parser.add_argument("variant", choices=["time", "scaling"])
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
    parser.add_argument("--base-experiment", help="Name of the base experiment")
    parser.add_argument("--extra-experiment", help="Name of the extra experiment")
    parser.add_argument(
        "--extra-experiment-name", help="Name to give to the extra experiment"
    )
    args = parser.parse_args()
    main(args)
