# Data Stream Processing and Analytics Semester Project
Authors: Alexandru Dan and Matthew Brookes

## Setup
In order to run this project it is required to use the `stable` Rust toolchain which can be configured using [rustup](https://rustup.rs/).

The `init.sh` script can be used to download either of the datasets used in the project. Try running the script to see the available options.

In addition to Rust, it is necessary to have a Kafka/Zookeeper cluster running. As a convenience, we have provided a script `kafka.sh` for managing the cluster. Before running a task, it is necessary to launch the cluster using `./kafka.sh -s` which will also create the required topics across 4 partitions. After running the tasks, it is recommended to delete the cluster using `./kafka.sh -k`. For the `kafka.sh` script to work, the directory of installation of Kafka has to be specified in environment variable `$KAFKA_DIR`. 

## Running the tasks
After starting Kafka using the helper script, each task can be run using `cargo`. Tasks can be ran using the following command:
```bash
$ cargo run -- [-r <num-records>] [-p <path-to-data>] post-stats|who-to-follow|unusual-activity
```
The first two arguments are optional:
* `-r` allows specifying the number of records to load onto each topic. The default is to read all records from the file and load them into Kafka.
* `-p` is used to specify the data directory contains the `streams` and `tables` directories. The default value is `data/1k-users-sorted`.

The main argument is one of the following: `post-stats`, `who-to-follow` or `unusual-activity`.

Before each task is run, the data files will be read and loaded into Kafka. Each task is configured to run on 4 workers, the same as the number of partitions configured for each Kafka topic.

Be patient when running the tasks, especially `unusual-activity` and `who-to-follow` which have a slow start-up time, especially on the larger dataset.
