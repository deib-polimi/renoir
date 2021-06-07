#!/usr/bin/env python3

import abc
import argparse
import json
import logging
import math
import os.path
import shlex
import subprocess
import sys
import tempfile

import coloredlogs
import paramiko
import ruamel.yaml

logger = logging.getLogger("benchmark")
verbosity = 0


class Benchmark(abc.ABC):
    @abc.abstractmethod
    def prepare(self, args):
        """Prepare the solution (e.g. compiling it if needed)"""

    def setup_hyp(self, hyperparameters):
        """Setup the system for the selected hyperparameters, for example
        raising up the cluster if needed.
        """

    @abc.abstractmethod
    def run(self, args, hyperparameters, run_index):
        """Run the benchmark of the prepared solution"""


class MPI(Benchmark):
    exec_path = "/tmp/mpibin"
    mpi_extra_args = [
        "-oversubscribe",
        "-mca",
        "btl_base_warn_component_unused",
        "0",
        "--map-by",
        #"NUMA:PE=4",
        "node",
        "-display-map",
        "--mca",
        "mpi_yield_when_idle",
        "1",
    ]

    def __init__(self, config):
        self.config = config

    def prepare(self, args):
        logger.info("Preparing MPI")
        compiler_host = self.config["known_hosts"][0]
        source = args.directory
        if not source.endswith("/"):
            source += "/"

        logger.info("Copying source files from %s to %s", source, compiler_host)
        compilation_dir = self.config["compilation_dir"]
        sync_on(compiler_host, source, compilation_dir)

        logger.info("Compinging using cmake")
        run_on(compiler_host, "mkdir", "-p", "%s/build" % compilation_dir)
        run_on(
            compiler_host,
            "cd %s/build && cmake .. -DCMAKE_BUILD_TYPE=Release" % compilation_dir,
            shell=True,
        )
        run_on(compiler_host, "cd %s/build && make -j 8" % compilation_dir, shell=True)
        run_on(
            compiler_host,
            "cd %s/build && ls -lah && file -E ./%s" % (compilation_dir, args.bin_name),
            shell=True,
        )
        logger.info(
            "Executable compiled at %s/build/%s on %s"
            % (compilation_dir, args.bin_name, compiler_host)
        )

        logger.info("Copying executable to this machine")
        local_path = "%s/%s/mpi/%s" % (
            self.config["temp_dir"],
            args.experiment,
            args.bin_name,
        )
        base = os.path.dirname(local_path)
        os.makedirs(base, exist_ok=True)
        sync_from(
            compiler_host, "%s/build/%s" % (compilation_dir, args.bin_name), local_path
        )

        logger.info("Copying executable to all the hosts")
        for host in self.config["known_hosts"]:
            sync_on(host, local_path, self.exec_path)

    def run(self, args, hyperparameters, run_index):
        num_hosts = hyperparameters["num_hosts"]
        procs_per_host = hyperparameters["procs_per_host"]
        with tempfile.NamedTemporaryFile("w", dir=self.config["temp_dir"]) as hostfile:
            hostfile_content = ""
            for host in self.config["known_hosts"][:num_hosts]:
                hostfile_content += "%s slots=%d\n" % (host, procs_per_host)
            hostfile.write(hostfile_content)
            hostfile.flush()
            if verbosity > 1:
                logger.debug("Hostfile at %s:\n%s", hostfile.name, hostfile_content)

            num_proc = num_hosts * procs_per_host
            extra_args = map(
                lambda arg: replace_vars(arg, hyperparameters),
                args.extra_args,
            )
            cmd = [
                "mpirun",
                #"-x",
                #"LD_PRELOAD=/usr/bin/libmimalloc.so",
                "-np",
                str(num_proc),
                "-hostfile",
                hostfile.name,
                *self.mpi_extra_args,
                self.exec_path,
                *extra_args,
            ]
            return run_benchmark(cmd)


class RStream(Benchmark):
    exec_path = "/tmp/rstreambin"
    compilation_suffix = "rstream"

    def __init__(self, config):
        self.config = config

    def prepare(self, args):
        logger.info("Preparing RStream")
        compiler_host = self.config["known_hosts"][0]
        source = args.directory
        if not source.endswith("/"):
            source += "/"

        logger.info("Copying source files from %s to %s", source, compiler_host)
        compilation_dir = self.config["compilation_dir"] + "/" + self.compilation_suffix
        run_on(compiler_host, "mkdir -p %s" % compilation_dir, shell=True)
        sync_on(compiler_host, source, compilation_dir)

        logger.info("Compinging using cargo")
        run_on(
            compiler_host,
            "cd %s && cargo build --release --example %s"
            % (compilation_dir, args.example),
            shell=True,
        )
        remote_path = "%s/target/release/examples/%s" % (compilation_dir, args.example)
        logger.info("Executable compiled at %s on %s" % (remote_path, compiler_host))

        logger.info("Copying executable to this machine")
        self.local_path = "%s/%s/rstream/%s" % (
            self.config["temp_dir"],
            args.experiment,
            args.example,
        )
        base = os.path.dirname(self.local_path)
        os.makedirs(base, exist_ok=True)
        sync_from(compiler_host, remote_path, self.local_path)

        logger.info("Copying executable to all the hosts")
        for host in self.config["known_hosts"]:
            sync_on(host, self.local_path, self.exec_path)

    def run(self, args, hyperparameters, run_index):
        num_hosts = hyperparameters["num_hosts"]
        procs_per_host = hyperparameters["procs_per_host"]
        with tempfile.NamedTemporaryFile("w", dir=self.config["temp_dir"]) as hostfile:
            hostfile_content = ""
            for i, host in enumerate(self.config["known_hosts"][:num_hosts]):
                slots = procs_per_host * args.num_steps
                if i == 0:
                    # source process
                    slots += 1
                if i == num_hosts - 1:
                    # sink process
                    slots += 1
                hostfile_content += "- hostname: %s\n" % host
                hostfile_content += "  slots: %d\n" % slots
            hostfile.write(hostfile_content)
            hostfile.flush()
            logger.debug("Hostfile at %s:\n%s", hostfile.name, hostfile_content)

            extra_args = map(
                lambda arg: replace_vars(arg, hyperparameters),
                args.extra_args,
            )
            cmd = [
                "cargo",
                "run",
                "--release",
                "--",
                "--hostfile",
                hostfile.name,
                self.exec_path,
                *extra_args,
            ]
            return run_benchmark(cmd, cwd=args.directory)

class RStream2(RStream):
    compilation_suffix = "rstream2"
    def run(self, args, hyperparameters, run_index):
        num_hosts = hyperparameters["num_hosts"]
        procs_per_host = hyperparameters["procs_per_host"]
        with tempfile.NamedTemporaryFile("w", dir=self.config["temp_dir"]) as configfile:
            config_content = "hosts:\n"
            for i, host in enumerate(self.config["known_hosts"][:num_hosts]):
                config_content += " - address: %s\n" % host
                config_content += "   base_port: 10000\n"
                config_content += "   num_cores: %d\n" % procs_per_host
            configfile.write(config_content)
            configfile.flush()
            logger.debug("Config at %s:\n%s", configfile.name, config_content)

            extra_args = map(
                lambda arg: replace_vars(arg, hyperparameters),
                args.extra_args,
            )
            cmd = [
                self.local_path,
                "--remote",
                configfile.name,
                *extra_args,
            ]
            return run_benchmark(cmd)


class Flink(Benchmark):
    def __init__(self, config):
        self.config = config
        self.flink = self.config["flink_base_path"]

    def prepare(self, args):
        logger.info("Preparing Flink")
        compiler_host = self.config["known_hosts"][0]
        source = args.directory
        if not source.endswith("/"):
            source += "/"

        logger.info("Copying source files from %s to %s", source, compiler_host)
        compilation_dir = self.config["compilation_dir"]
        sync_on(compiler_host, source, compilation_dir)

        logger.info("Compinging using mvn")
        run_on(
            compiler_host,
            "cd %s && mvn package" % compilation_dir,
            shell=True,
        )
        run_on(
            compiler_host,
            "cd %s/target && ls -lah && file -E ./%s"
            % (compilation_dir, args.jar_name),
            shell=True,
        )
        remote_path = "%s/target/%s" % (compilation_dir, args.jar_name)
        logger.info("Executable compiled at %s on %s" % (remote_path, compiler_host))

        logger.info("Copying executable to this machine")
        local_path = "%s/%s/flink/%s" % (
            self.config["temp_dir"],
            args.experiment,
            args.jar_name,
        )
        base = os.path.dirname(local_path)
        os.makedirs(base, exist_ok=True)
        sync_from(compiler_host, remote_path, local_path)

    def setup_hyp(self, hyperparameters):
        logger.info("Stopping the cluster")
        subprocess.run([self.flink + "/bin/stop-cluster.sh"], check=True)

        logger.info("Setting up ./conf/workers file")
        workers = self.flink + "/conf/workers"
        num_hosts = hyperparameters["num_hosts"]
        with open(workers, "w") as f:
            for host in self.config["known_hosts"][:num_hosts]:
                f.write(host + "\n")

        logger.info("Starting the cluster")
        subprocess.run([self.flink + "/bin/start-cluster.sh"], check=True)

    def run(self, args, hyperparameters, run_index):
        num_hosts = hyperparameters["num_hosts"]
        procs_per_host = hyperparameters["procs_per_host"]
        parallelism = num_hosts * procs_per_host
        local_path = "%s/%s/flink/%s" % (
            self.config["temp_dir"],
            args.experiment,
            args.jar_name,
        )
        extra_args = map(
            lambda arg: replace_vars(arg, hyperparameters),
            args.extra_args,
        )
        cmd = [
            self.flink + "/bin/flink",
            "run",
            "-p",
            str(parallelism),
            local_path,
            *extra_args,
        ]
        return run_benchmark(cmd)


def run_benchmark(cmd, cwd=None):
    logger.debug("Command: %s", shlex.join(cmd))
    out = subprocess.run(cmd, capture_output=True, cwd=cwd)
    stdout = out.stdout.decode()
    stderr = out.stderr.decode()
    if out.returncode != 0:
        raise RuntimeError(
            "benchmark failed!\nExit code: %d\nStdout:\n%s\nStderr:\n%s"
            % (out.returncode, stdout, stderr)
        )
    return {
        "stdout": stdout,
        "stderr": stderr,
    }


def run_on(host, *cmd, shell=False):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host)

    if shell:
        assert len(cmd) == 1
        cmd = "bash -l -c %s" % shlex.quote(cmd[0])
    else:
        cmd = shlex.join(cmd)
    logger.debug("Remote on %s: %s", host, cmd)
    stdin, stdout, stderr = client.exec_command(cmd)
    exit_code = stdout.channel.recv_exit_status()
    stdout = stdout.read().decode()
    stderr = stderr.read().decode()
    if exit_code != 0:
        raise RuntimeError(
            "Remote command failed on host %s: %s\nStdout:\n%s\nStderr:\n%s"
            % (host, cmd, stdout, stderr)
        )
    return stdout + stderr


def sync_on(host, local_path, remote_path):
    remote = host + ":" + remote_path
    logger.debug("rsync: %s -> %s", local_path, remote)
    subprocess.run(
        [
            "rsync",
            "-a",
            "--exclude",
            "target",
            "--exclude",
            "build",
            local_path,
            remote,
        ],
        check=True,
    )


def sync_from(host, remote_path, local_path):
    remote = host + ":" + remote_path
    logger.debug("rsync: %s -> %s", remote, local_path)
    subprocess.run(
        ["rsync", "-a", "--exclude", "target", remote, local_path],
        check=True,
    )


def ping_host(host):
    proc = subprocess.run(
        "ping %s -c 5 -i 0.2" % host, shell=True, capture_output=True, check=True
    )
    stdout = proc.stdout.decode().splitlines()
    times = []
    for line in stdout:
        if "time=" in line:
            time = float(line.split("time=")[1].split(" ")[0])
            times += [time]
    return sum(times) / len(times)


def test_hosts(config):
    logger.info("Testing hosts connection:")
    for host in config["known_hosts"]:
        model_name = run_on(host, 'lscpu | grep "Model name"', shell=True)
        model_name = model_name.split(":")[1].strip()
        ping = ping_host(host)
        logger.info("   - %s: %s (%.3fms)", host, model_name, ping)


def hyperparameter_generator(hyperparameters):
    """Yield all the possibile combinations of hyperparameters"""
    if isinstance(hyperparameters, dict):
        yield from hyperparameter_generator(list(hyperparameters.items()))
        return
    if not hyperparameters:
        yield {}
        return
    head, *rest = hyperparameters
    name, values = head
    for value in values:
        for next_hyp in hyperparameter_generator(rest):
            yield {name: value, **next_hyp}


def sanity_check(config):
    hyperparameters = config["hyperparameters"]
    for name, values in hyperparameters.items():
        if not isinstance(values, list):
            raise ValueError("Invalid hyperparameter: %s. It should be a list" % (name))
        if not values:
            raise ValueError(
                "Invalid hyperparameter: %s. An empty list will void all the "
                "combinations, please comment it if you don't want to use that hyperparameter."
                % (name)
            )
    num_hosts = len(config["known_hosts"])
    max_hosts = max(hyperparameters["num_hosts"])
    if max_hosts > num_hosts:
        raise ValueError(
            "Invalid hyperparameter: num_hosts. Cannot use %d hosts, %d known"
            % (max_hosts, num_hosts)
        )
    if len(config["known_hosts"]) != len(set(config["known_hosts"])):
        raise ValueError(
            "known_hosts contains duplicated host names, this is not supported by MPI"
        )


def replace_vars(pattern, vars):
    for name, value in vars.items():
        pattern = pattern.replace("{" + name + "}", str(value))
    return pattern


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "experiment",
        help="Name of the experiment, used for naming the results file",
    )
    parser.add_argument(
        "--config",
        help="Path to the config.yaml file",
        default="config.yaml",
    )
    parser.add_argument(
        "--overwrite",
        help="Overwrite the results instead of skipping",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        help="Log more output",
        default=0,
        action="count",
    )

    subparsers = parser.add_subparsers(
        required=True, help="System to test", dest="system"
    )

    mpi = subparsers.add_parser("mpi")
    mpi.add_argument(
        "directory",
        help="Local path to the folder with the MPI project",
    )
    mpi.add_argument(
        "bin_name",
        help="Name of the binary file produced by cmake",
    )
    mpi.add_argument(
        "extra_args",
        help="Additional arguments to pass to the executable",
        nargs="*",
    )

    rstream = subparsers.add_parser("rstream")
    rstream.add_argument(
        "directory",
        help="Local path to the folder with the RStream repo",
    )
    rstream.add_argument(
        "example",
        help="Name of the example to compile and run",
    )
    rstream.add_argument(
        "num_steps",
        help="Number of steps of the pipeline: the number of processes to spawn is adjusted based on this.",
        type=int,
    )
    rstream.add_argument(
        "extra_args",
        help="Additional arguments to pass to the executable",
        nargs="*",
    )

    rstream2 = subparsers.add_parser("rstream2")
    rstream2.add_argument(
        "directory",
        help="Local path to the folder with the RStream2 repo",
    )
    rstream2.add_argument(
        "example",
        help="Name of the example to compile and run",
    )
    rstream2.add_argument(
        "extra_args",
        help="Additional arguments to pass to the executable",
        nargs="*",
    )

    flink = subparsers.add_parser("flink")
    flink.add_argument(
        "directory",
        help="Local path to the folder with the flink project",
    )
    flink.add_argument(
        "jar_name",
        help="Name of the jar generated by 'mvn package'",
    )
    flink.add_argument(
        "extra_args",
        help="Additional arguments to pass to the executable",
        nargs="*",
    )

    args = parser.parse_args()

    verbosity = args.verbose
    coloredlogs.install(
        level="DEBUG" if args.verbose > 0 else "INFO",
        milliseconds=True,
        logger=logger,
        fmt="%(asctime)s,%(msecs)03d %(levelname)s %(message)s",
    )

    logger.info("Using configuration file: %s", args.config)
    with open(args.config) as f:
        config = ruamel.yaml.safe_load(f)

    try:
        sanity_check(config)
    except ValueError as e:
        logger.fatal(e.args[0])
        sys.exit(1)

    hyperparameters = config["hyperparameters"]
    num_tests = math.prod([len(hyp) for hyp in hyperparameters.values()])
    logger.info("- %d hyperparameters", len(hyperparameters))
    logger.info("- %d total combinations", num_tests)

    if args.system == "mpi":
        runner = MPI(config)
    elif args.system == "rstream":
        runner = RStream(config)
    elif args.system == "rstream2":
        runner = RStream2(config)
    elif args.system == "flink":
        runner = Flink(config)
    else:
        raise ValueError("%s is not supported yet" % args.system)

    test_hosts(config)

    logger.info("Preparation step")
    runner.prepare(args)

    for hyp_index, hyp in enumerate(hyperparameter_generator(hyperparameters)):
        logger.info("-" * 80)
        logger.info("Current hyperparameters (%d / %d):" % (hyp_index + 1, num_tests))
        for name, value in hyp.items():
            logger.info("  - %s: %s", name, value)
        logger.info("Running on:")
        for host in config["known_hosts"][: hyp["num_hosts"]]:
            logger.info("  - %s", host)
        logger.info("Setting up...")
        runner.setup_hyp(hyp)

        if config["warmup"]:
            logger.info("Running a warmup run for caching the input")
            try:
                result = runner.run(args, hyp, None)
            except:
                logger.exception("Execution failed!", exc_info=True)

        for run_index in range(config["num_runs"]):
            metadata = {
                "system": args.system,
                "run": run_index,
                "experiment": args.experiment,
                **hyp,
            }
            results_dir = replace_vars(config["results_dir"], metadata)
            file_name = "-".join(
                [
                    "{}-{}".format(name, value).replace("/", "_")
                    for name, value in hyp.items()
                ]
            )
            file_name += "-run-" + str(run_index) + ".json"
            results_file = results_dir + "/" + file_name
            logger.info(
                "Run %d / %d: %s", run_index + 1, config["num_runs"], results_file
            )
            basedir = os.path.dirname(results_file)
            os.makedirs(basedir, exist_ok=True)
            if os.path.exists(results_file):
                if args.overwrite:
                    logger.warning("Overwriting previous results!")
                else:
                    logger.warning("Results already present, skipping!")
                    continue
            try:
                result = runner.run(args, hyp, run_index)
                result = {
                    "hosts": config["known_hosts"][: hyp["num_hosts"]],
                    "result": result,
                    **metadata,
                }
            except:
                logger.exception("Execution failed!", exc_info=True)
            else:
                with open(results_file, "w") as f:
                    json.dump(result, f, indent=4)
                    f.write("\n")
