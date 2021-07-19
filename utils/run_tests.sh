#!/bin/bash

function wait_for_start {
    (
    HOST=${1}
    PORT=${2}
    # Wait for stardog to be running
    RC=1
    COUNT=0
    set +e
    while [[ ${RC} -ne 0 ]];
    do
      if [[ ${COUNT} -gt 50 ]]; then
          return 1;
      fi
      COUNT=$(expr 1 + ${COUNT} )
      sleep 1
      curl -v  http://${HOST}:${PORT}/admin/healthcheck
      RC=$?
    done
    # Give it a second to finish starting up
    sleep 5

    return 0
    )
}

# depends_on in compose is not enough

# Might need to add a lb, so we hit that instead of a specific node.
wait_for_start pystardog_stardog_1 5820
pytest --endpoint http://stardog1:5820
