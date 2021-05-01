# Tools for running the benchmarks

`benchmark.py` is able to automate most of the steps required to run a benchmark and collect the results.
In particular it does automatically the following:

- Testing the connection to the workers, measuring ping and collecting info about their CPU
- Compile the program on a worker node ensuring the correct native optimizations
- Sync the executable to all the worker nodes
- Set up the host files
- Run a warmup execution to cache the dataset in RAM
- Run the benchmark N times collecting the outputs in handy json files

This script supports MPI projects compiled with cmake, RStream examples and Flink projects with maven.

Where `benchmark.py` really shines is automating the benchmark varying hyperparameters:
in the configuration file you can define a set of hyperparameter and their possible values, the script will enumerate all the possible combinations and run the benchmarks on them.

The 2 required hyperparameters are:

- `num_hosts`: the number of hosts to use (the values should not be grater than the number of known hosts)
- `procs_per_host`: the number of processes (slots) for each host
   - When using RStream it's the number of slots in the hostfile (ignoring source and sink, they're added automatically)
   - When using MPI or Flink it's the number of slots for that worker

You can use `--verbose` to see all the details of what's happening under the hoods.

## Example Usage

All the commands should be executed inside `tools/`, running elsewhere needs changes in the arguments.

Before running `benchmark.py` copy `example.config.yaml` in `config.yaml` and tune the parameters (e.g. setting the list of known hosts, hyperparameters, ...).

Try running `./benchmark.py experiment_name mpi ../mpi/wordcount/ main -- -T 1 -d ~/rust_stream/data/gutenberg.txt` against this configuration file:
```yaml
known_hosts:
 - localhost
results_file: "./results/{experiment}/{system}/{num_hosts}-hosts-{procs_per_host}-procs-{run}.json"
# [...]
warmup: true
num_runs: 5
hyperparameters:
  num_hosts: [1]
  procs_per_host: [1, 2, 4, 8]
```

This is a possible content of one results file (`num_hosts-1-procs_per_host-1-run-0.json`):
```json
{
    "hosts": [
        "localhost"
    ],
    "result": {
        "stdout": "total:1673354595\ncsv:0\nnetwork:397\n",
        "stderr": "[ 0/ 0] has interval         0 - 104857600\n[ 0/ 0] has interval         0 - 104857600 -- done\n"
    },
    "system": "mpi",
    "run": 0,
    "experiment": "experiment_name",
    "num_hosts": 1,
    "procs_per_host": 1
}
```

### Running an MPI benchmark

```bash
./benchmark.py experiment_name mpi ../mpi/wordcount/ main -- -T 8 -d ~/rust_stream/data/gutenberg.txt
```

- `experiment_name` is used to keep track of which test you are performing, it will be used to organize the results in the `results/` folder.
- `mpi` tell the script you are running an MPI benchmark.
- `../mpi/wordcount/` is the path where the benchmark is stored (in the master node). There should be a `CMakeLists.txt` inside.
- `main` is the name of the executable produced by compiling the benchmark.
- `--` what follows are arguments passed to the executable run with `mpirun`.
- `-T 8` tells the benchmark to use 8 threads. You can also add hyperparameters in `config.yaml` and use them in the command line arguments wrapping them in curly braces (e.g `-T {threads}`).

### Running a RStream example

```bash
./benchmark.py experiment_name rstream .. wordcount_p -- ~/rust_stream/data/gutenberg.txt
```

- `experiment_name` is used to keep track of which test you are performing, it will be used to organize the results in the `results/` folder.
- `rstream` tell the script you are running a RStream benchmark.
- `..` is the path where the RStream project is cloned (`..` since we are inside `tools/`).
- `wordcount_p` is the name of the example to run.
- `--` what follows are arguments passed to the executable.

### Running a Flink example

Remember to set `flink_base_path` inside `config.yaml` to the correct path and to copy flink on all the hosts, including the workers.

```bash
./benchmark.py experiment_name flink ../flink/WordCount wordCount-1.0.jar -- -input ~/rust_stream/data/gutenberg.txt
```

- `experiment_name` is used to keep track of which test you are performing, it will be used to organize the results in the `results/` folder.
- `flink` tell the script you are running a Flink benchmark.
- `../flink/WordCount` is the path where the Flink project is stored. There should be a `pom.xml` inside.
- `wordCount-1.0.jar` is the name of the jar file produced by `mvn package`.
- `--` what follows are arguments passed to the executable.