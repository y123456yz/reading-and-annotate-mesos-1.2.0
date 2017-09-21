#!/usr/bin/env bash

prefix=/usr/local
exec_prefix=${prefix}

deploy_dir=${prefix}/etc/mesos

# Increase the default number of open file descriptors.
ulimit -n 8192

PROGRAM=${1}

shift # Remove PROGRAM from the argument list (since we pass ${@} below).

test -e ${deploy_dir}/${PROGRAM}-env.sh && \
. ${deploy_dir}/${PROGRAM}-env.sh

nohup ${exec_prefix}/sbin/${PROGRAM} ${@} </dev/null >/dev/null 2>&1 &
