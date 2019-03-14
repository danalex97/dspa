#!/usr/bin/env bash

set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
DATA_DIR=$DIR/data

SMALL="https://polybox.ethz.ch/index.php/s/qRlRpFhoPtdO6bR/download"
BIG="https://polybox.ethz.ch/index.php/s/8JRHOc3fICXtqzN/download"

SMALL_DIR=$DATA_DIR/small
BIG_DIR=$DATA_DIR/big

# make data directory
mkdir -p $DATA_DIR

function usage() {
    echo "Usage: $0 [OPTION]..."
	echo "Download datasets for the project."

    echo -e "\nOptions: "
    printf "\t %- 30s %s\n" "-s | --small" "Download small dataset."
    printf "\t %- 30s %s\n" "-b | --big" "Download big dataset."
    echo ""
    echo "Example usage: "
    echo "$ ./data.sh --small"
}

function download() {
	cd $DATA_DIR

	wget $1
	unzip download
	rm download

	cd $DIR
}

function parse_command_line_options() {
    while [ "${1:-}" != "" ]; do
        case $1 in
            -s | --small)
				download $SMALL
				shift
                ;;
	        -b | --big)
				download $BIG
				shift
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

parse_command_line_options "$@"
