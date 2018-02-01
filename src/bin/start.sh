#!/usr/bin/env bash

# bin dir
BIN_DIR=$(cd $(dirname ${0}) && pwd)

# root_dir
ROOT_DIR=$(cd $(dirname ${BIN_DIR}) && pwd)

# virtual env
VENV=${ROOT_DIR}/venv

# activate virtual environment
source ${VENV}/bin/activate

# yesterday
if [ $# -ge 1 ]; then
    DAY=$1
else
    echo "Usage: sh $0 date string, such as, sh $0 20080808}"
    exit 1
fi

# run python script
python ${ROOT_DIR}/src/main.py ${DAY}
