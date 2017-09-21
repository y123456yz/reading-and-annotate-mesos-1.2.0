#!/usr/bin/env bash

prefix=/usr/local
exec_prefix=${prefix}

usage() {
  echo "Usage: mesos-stop-cluster.sh [-h] [-s]"
  echo " -h          display this message"
  echo " -s          use sudo to stop mesos-master and mesos-agent"
  if test ${#} -gt 0; then
    echo
    echo "${@}"
  fi
  exit 1
}

while getopts "hs" opt
do
  case ${opt} in
    h) usage ;;
    s) export DEPLOY_WITH_SUDO=1 ;;
    *) usage "Invalid option: -${OPTARG}" ;;
  esac
done

${exec_prefix}/sbin/mesos-stop-agents.sh && sleep 1 && ${exec_prefix}/sbin/mesos-stop-masters.sh && {
  # TODO(chengwei): see mesos-start-cluster.sh
  echo "Everything's stopped!"
}
