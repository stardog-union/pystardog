#!/bin/bash

function wait_for_start {
    (
    HOST=${1}
    PORT=${2}
    # Wait for stardog to be running
    COUNT=0
    set +e
    not_ready=true
    while $not_ready
    do
      if [[ ${COUNT} -gt 50 ]]; then
          echo "Failed to start Stardog cluster on time"
          return 1;
      fi
      COUNT=$(expr 1 + ${COUNT} )
      sleep 5

      # wait for main cluster to be ready
      number_of_nodes=$(curl -s http://${HOST}:${PORT}/admin/cluster/ -u ${STARDOG_USER}:${STARDOG_PASS} | jq .'nodes | length')
      echo "number of nodes ready: " $number_of_nodes
      if [[ $number_of_nodes -eq 2 && $RC -eq 0 ]]; then break; fi


    done
    # Give it a second to finish starting up.
    sleep 5

    return 0
    )
}

function wait_for_standby_node {

    HOST=${1}
    PORT=${2}
    # Wait for stardog to be running
    COUNT=0
    set +e
    not_ready=true
    while $not_ready
    do
      if [[ ${COUNT} -gt 50 ]]; then
          echo "Failed to start Stardog cluster on time"
          return 1;
      fi
      COUNT=$(expr 1 + ${COUNT} )
      sleep 5
      # wait for standby node to be ready. standby nodes needs to wait for main cluster first.
      status_code=$(curl -o /dev/null -s -w "%{http_code}\n" http://${HOST}:${PORT}/admin/healthcheck)
      if [[ $status_code -eq 200 ]]; then break; fi

    done
}


function wait_for_start_single_node {
    (
    HOST=${1}
    PORT=${2}
    # Wait for stardog to be running
    COUNT=0
    set +e
    not_ready=true
    while $not_ready
    do
      if [[ ${COUNT} -gt 50 ]]; then
          echo "Failed to start Stardog server on time"
          return 1;
      fi
      COUNT=$(expr 1 + ${COUNT} )
      sleep 5

      curl -s http://${HOST}:${PORT}/admin/healthcheck -u ${STARDOG_USER}:${STARDOG_PASS}
      if [ $? -eq 0 ]; then
        echo "Stardog server single node up and running"
        break
      fi

    done
    # Give it a second to finish starting up.
    sleep 5

    return 0
    )
}

