#!/usr/bin/env bash

set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
DATA_DIR=$DIR/data
PLOT_DIR=$DIR/plots

SMALL="https://polybox.ethz.ch/index.php/s/qRlRpFhoPtdO6bR/download"
BIG="https://polybox.ethz.ch/index.php/s/8JRHOc3fICXtqzN/download"

SMALL_DIR=$DATA_DIR/small
BIG_DIR=$DATA_DIR/big

# make data directory
mkdir -p $DATA_DIR
mkdir -p $PLOT_DIR

function usage() {
    echo "Usage: $0 [OPTION]..."
    echo "Download datasets for the project."

    echo -e "\nOptions: "
    printf "\t %- 30s %s\n" "-s | --download-small" "Download small dataset."
    printf "\t %- 30s %s\n" "-b | --download-big" "Download big dataset."
    printf "\t %- 30s %s\n" "-v | --video-outliers" "Generate video for outlier posts."
    echo ""
    echo "Example usage: "
    echo "$ ./data.sh --small"
}

function download() {
    pushd $DATA_DIR

    wget $1
    unzip download
    rm download

    popd
}

function generate_video() {
    pushd $PLOT_DIR

    ffmpeg -framerate 1 -i scatter%000d.svg -c:v libx264 -crf 20 outliers.mp4

    popd
}

function parse_command_line_options() {
    while [ "${1:-}" != "" ]; do
        case $1 in
            -s | --download-small)
                download $SMALL
                shift
                ;;
            -b | --download-big)
                download $BIG
                shift
                ;;
            -v | --video-outliers)
                generate_video
                shift
                ;;
            -h | --help)
                usage
                exit 0
                ;;
            * )
                usage
                exit 1
        esac

        shift
    done
    usage
}

parse_command_line_options "$@"
