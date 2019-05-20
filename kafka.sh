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
    printf "\t %- 30s %s\n" "-dt | --dtopic [topic]" "Delete topic."
    printf "\t %- 30s %s\n" "-lt | --ltopic" "List topics."
    printf "\t %- 30s %s\n" "-wt | --wtopic [topic]" "Write to topic."
    printf "\t %- 30s %s\n" "-rt | --rtopic [topic]" "Read from topic."

    printf "\n"
    printf "Examples:\n"
    printf  "%- 40s %s\n" "echo \"some stuff\" | ./kafka.sh -wt hey" "Writes \"some stuff\" to topic 'hey'"
    printf  "%- 40s %s\n" "./kafka.sh -rt hey" "Reads from topic 'hey'"
    printf "\n"
}

function start_kafka() {
    start_processes
    create_all_topics
    echo "Kafka ready."
}

function start_processes() {
    echo "Starting processes..."
    nohup bin/zookeeper-server-start.sh config/zookeeper.properties >/dev/null 2>&1 &
    nohup bin/kafka-server-start.sh config/server.properties >/dev/null 2>&1 &
    nohup bin/kafka-server-start.sh config/server-1.properties >/dev/null 2>&1 &
    nohup bin/kafka-server-start.sh config/server-2.properties >/dev/null 2>&1 &
    echo "Processes started."
}

function create_all_topics() {
    echo "Creating topics..."

    # wait for Zookeper
    ZK_PORT=`cat config/zookeeper.properties | grep "clientPort" | cut -d "=" -f 2`
    while ! nc -z localhost $ZK_PORT; do
        sleep 0.1
    done

    retry make_topic "comments"
    retry make_topic "posts"
    retry make_topic "likes"

    echo "Topics created."
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

function delete_topic() {
    bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic $1
}

function make_topic() {
    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $1
}

function retry() {
    func=$1
    shift
    while [ true ]; do
        $func $@ 2>/dev/null
        if [ $? -eq 0 ]; then
            break
        fi
        echo "Retrying..."
        sleep 0.1
    done
}

function list_topics() {
    bin/kafka-topics.sh --list --zookeeper localhost:2181
}

function write_topic() {
    cat | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic $@
}

function read_topic() {
    bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $@
}

function stop_all() {
    echo "Stoping all..."
    kill_process "zookeeper"
    kill_process "kafka"
    echo "All processes killed."
    echo "Deleting files..."
    rm -rf /tmp/zookeeper
    rm -rf /tmp/kafka-logs
}

function parse_command_line_options() {
    while [ "${1:-}" != "" ]; do
        case $1 in
            -s | --start)
                shift
                start_kafka
                exit 0
                ;;
            -k | --kill)
                shift
                stop_all
                exit 0
                ;;
            -t | --topic)
                shift
                make_topic $1
                exit 0
                ;;
            -lt | --ltopic)
                shift
                list_topics
                exit 0
                ;;
            -dt | --dtopic)
                shift
                delete_topic $1
                exit 0
                ;;
            -wt | --wtopic)
                shift
                write_topic $@
                exit 0
                ;;
            -rt | --rtopic)
                shift
                read_topic $@
                exit 0
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
