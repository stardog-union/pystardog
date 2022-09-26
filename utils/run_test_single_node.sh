#!/bin/bash -x

source ./utils/wait.sh

echo "OK"
wait_for_start_single_node ${STARDOG_ENDPOINT}  5820

echo ${STARDOG_ENDPOING}
echo "READY"
pytest test/test_single_node.py --endpoint http://${STARDOG_ENDPOINT}:5820 -s
