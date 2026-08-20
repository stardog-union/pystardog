#!/bin/bash -e

/usr/bin/sshd -f ${HOME}/custom_ssh/sshd_config -D -p 2222 &
/opt/stardog/bin/stardog-admin server start
tail -f /dev/null