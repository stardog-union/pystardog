#!/bin/bash

function wait_for_start_cluster {
    (
    HOST=${1}
    PORT=${2}
    # Wait for stardog to be running
    COUNT=0
    set +ex
    not_ready=true
    while $not_ready
    do
      if [[ ${COUNT} -gt 50 ]]; then
          echo "Failed to start Stardog cluster on time"
          exit 1;
      fi
      COUNT=$(expr 1 + ${COUNT} )
      sleep 5

      # wait for main cluster to be ready
      curl -v http://${HOST}:${PORT}/admin/cluster/ -u admin:admin
      number_of_nodes=$(curl -s http://${HOST}:${PORT}/admin/cluster/ -u admin:admin | jq .'nodes | length')
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
          exit 1;
      fi
      COUNT=$(expr 1 + ${COUNT} )
      sleep 5
      # wait for standby node to be ready. standby nodes needs to wait for main cluster first.
      # Recall that checking healthcheck endpoint will return 503 since the node is still standby, and not part of a cluster, hence we check aliveness
      status_code=$(curl -o /dev/null -s -w "%{http_code}\n" http://${HOST}:${PORT}/admin/alive)
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
          exit 1;
      fi
      COUNT=$(expr 1 + ${COUNT} )
      sleep 5
      curl -s http://${HOST}:${PORT}/admin/healthcheck -u admin:admin
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

