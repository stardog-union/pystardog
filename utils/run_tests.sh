#!/bin/bash -x

source ./utils/wait.sh

wait_for_start ${STARDOG_LB} ${STARDOG_HOSTNAME_STANDBY} 5820
pytest --endpoint http://${STARDOG_LB}:5820 -s -k 'not test_single_node'
