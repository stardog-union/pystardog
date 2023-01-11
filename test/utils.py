import pytest
import os
from time import sleep
import subprocess


SSH_USER = os.environ["SSH_USER"]
SSH_PASS = os.environ["SSH_PASS"]
STARDOG_HOSTNAME_NODE_1 = os.environ["STARDOG_HOSTNAME_NODE_1"]
STARDOG_HOSTNAME_STANDBY = os.environ["STARDOG_HOSTNAME_STANDBY"]


def get_node_ip(node_hostname):
    node_ip = subprocess.run(
        [
            "sshpass",
            "-p",
            SSH_PASS,
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            f"ssh://{SSH_USER}@{node_hostname}:2222",
            "--",
            "hostname",
            "-I",
        ],
        stdout=subprocess.PIPE,
    )
    return node_ip.stdout


def get_current_node_count(admin):
    return len(admin.cluster_info()["nodes"])


def wait_standby_node_to_join(admin):
    standby_node_ip = (
        get_node_ip(STARDOG_HOSTNAME_STANDBY).decode("utf-8").strip() + ":5820"
    )
    retries = 0
    while True:
        try:
            if standby_node_ip in admin.cluster_info()["nodes"]:
                print(f"current nodes: {admin.cluster_info()['nodes']}")
                break
            else:
                print(
                    "http call did not fail, but node is still not listed in cluster info"
                )
        except Exception as e:
            print(
                f"An exception ocurred while connecting to the standby node: {e}, will keep retrying"
            )
        print(f"retries for now: {retries}")
        retries += 1
        sleep(20)
        if retries >= 50:
            raise Exception("Took too long for standby node to join the cluster")


def wait_for_cleaning_cache_target(admin, cache_target_name):
    retries = 0
    while True:
        cache_targets = admin.cache_targets()
        cache_target_names = [cache_target.name for cache_target in cache_targets]
        if cache_target_name in cache_target_names:
            retries += 1
            sleep(1)
            if retries >= 20:
                raise Exception(
                    "Took too long to remove cache target: " + cache_target_name
                )
        else:
            return


def wait_for_creating_cache_target(admin, cache_target_name):
    retries = 0
    while True:
        cache_targets = admin.cache_targets()
        cache_target_names = [cache_target.name for cache_target in cache_targets]
        if cache_target_name not in cache_target_names:
            retries += 1
            sleep(1)
            if retries >= 20:
                raise Exception(
                    "Took too long to register cache target: " + cache_target_name
                )
        else:
            return
