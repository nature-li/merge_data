#!/usr/bin/env bash

# bin dir
BIN_DIR=$(cd $(dirname ${0}) && pwd)

# root_dir
ROOT_DIR=$(cd $(dirname ${BIN_DIR}) && pwd)

# virtual env
VENV=${ROOT_DIR}/venv

# mt_hash
MT_HASH=${ROOT_DIR}/src/mt_hash

# create virtual environment dir if needed
virtualenv --no-site-package ${VENV}

source ${VENV}/bin/activate

# mt_hash
cd ${MT_HASH}
python setup.py build
python setup.py install

pip install phoenixdb
pip install redis
pip list
deactivate