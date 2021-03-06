#!/usr/bin/env bash

prefix=/usr/local
DEPLOY_DIR=${prefix}/etc/mesos

# Pull in deploy specific options.
test -e ${DEPLOY_DIR}/mesos-deploy-env.sh && \
  . ${DEPLOY_DIR}/mesos-deploy-env.sh

# Find the list of agents.
AGENTS_FILE="${DEPLOY_DIR}/slaves"
if test ! -e ${AGENTS_FILE}; then
  echo "Failed to find ${AGENTS_FILE}"
  exit 1
fi

# The expected format of the AGENTS_FILE is one IP/host per line.
# Additionally, you can temporarily comment out a host or IP by placing a hash
# character '#' as the first character of the line.  As an example, changing
# this MASTERS_FILE:
#
# 10.1.1.1
# 10.1.1.2
#
# to this one:
#
# 10.1.1.1
# #10.1.1.2
#
# removes the 10.1.1.2 IP address from being used.  Note that this does NOT
# support comments through the end of line, like this:
#
# 10.1.1.1  # my first IP
# 10.1.1.2  # my second IP
#
AGENTS=`cat ${AGENTS_FILE} | grep -v '^#'`

killall="killall"

# Add sudo if requested.
if test "x${DEPLOY_WITH_SUDO}" = "x1"; then
  killall="sudo ${killall}"
fi

for agent in ${AGENTS}; do
  echo "Stopping mesos-agent on ${agent}"
  ssh ${SSH_OPTS} ${agent} "${killall} mesos-agent" &
  sleep 0.1
done

wait # Wait for all the ssh's to finish.
