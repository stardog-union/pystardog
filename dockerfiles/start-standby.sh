#!/bin/bash

function wait_for_cluster_ready(){
    (
    # Wait for stardog to be running
    COUNT=0
    set +e
    while :
    do
      if [[ ${COUNT} -gt 50 ]]; then
          echo "Failed to start Stardog cluster on time"
          return 1;
      fi
      COUNT=$(expr 1 + ${COUNT})
      sleep 3

      number_of_nodes=$(/opt/stardog/bin/stardog-admin --server  http://sdlb:5820  cluster status | grep Node | wc -l)
      if [[ $number_of_nodes -eq 2 ]]; then break; fi
    done

    return 0
    )

}
wait_for_cluster_ready
echo "Main cluster ready, starting standby node"
/var/start.sh
