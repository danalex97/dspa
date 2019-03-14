#!/usr/bin/env bash

set -uo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

function usage() {
    echo "Usage: $0 [OPTION]..."
	echo "Kafka utilities."

    echo -e "\nOptions: "
	printf "\t %- 30s %s\n" "-s | --start" "Start Zookeper and Kafka."
	printf "\t %- 30s %s\n" "-k | --kill" "Stops all Zookeper and Kafka processes."
	printf "\t %- 30s %s\n" "-t | --topic [topic]" "Create topic."
	printf "\t %- 30s %s\n" "-lt | --ltopic" "List topics."
	printf "\t %- 30s %s\n" "-rt | --rtopic [topic] [text]" "Write to topic."
    printf "\t %- 30s %s\n" "-wt | --wtopic [topic]" "Read from topic."
}

function start_kafka() {
	echo "Starting processes..."
	nohup bin/zookeeper-server-start.sh config/zookeeper.properties >/dev/null 2>&1 &
	nohup bin/kafka-server-start.sh config/server.properties >/dev/null 2>&1 &
	echo "Processes started."
}

function kill_process() {
	pids=$(ps -ax | grep $1 | grep -v 'grep' | awk '{print $1}')

	for pid in $pids
	do
		if [ $pid != $$ ]; then
			echo "Killing $1 $pid"
			kill -9 $pid 2>1 || true
		fi
	done
}

function make_topic() {
	bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $1
}

function stop_all() {
	echo "Stoping all..."
	kill_process "zookeeper"
	kill_process "kafka"
	echo "All processes killed."
}

function parse_command_line_options() {
    while [ "${1:-}" != "" ]; do
        case $1 in
            -s | --start)
				shift
				start_kafka
                ;;
			-k | --kill)
				shift
				stop_all
                ;;
            -h | --help )
                usage
                exit 0
                ;;
            * )
                usage
                exit 1
        esac

        shift
    done
}

if [[ ! -v KAFKA_DIR ]]; then
	echo "Please setup KAFKA_DIR environment variable."
	exit 0
fi

pushd $KAFKA_DIR > /dev/null
parse_command_line_options "$@"
popd > /dev/null
