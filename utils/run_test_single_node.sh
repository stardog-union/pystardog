#!/bin/bash -x

source ./utils/wait.sh

wait_for_start_single_node ${STARDOG_ENDPOINT}  5820
pytest test/test_single_node.py --endpoint http://${STARDOG_ENDPOINT}:5820 -s
