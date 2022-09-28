#!/bin/bash -x

source ./utils/wait.sh

wait_for_start ${STARDOG_LB} 5820
# Disabling because of https://github.com/stardog-union/pystardog/issues/111
# wait_for_standby_node ${STARDOG_HOSTNAME_STANDBY} 5820
pytest --endpoint http://${STARDOG_LB}:5820 -s -k 'not test_single_node'
