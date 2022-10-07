#!/bin/bash -x

source ./utils/wait.sh

echo "Commenting and sleeping instead, to check whether there is a bug with cluster status"
wait_for_start ${STARDOG_LB} 5820
wait_for_standby_node ${STARDOG_HOSTNAME_STANDBY} 5820
pytest --endpoint http://${STARDOG_LB}:5820 -s -k 'not test_single_node'

