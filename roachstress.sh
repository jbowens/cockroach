#!/bin/bash
set -euxo pipefail

sha=$(git rev-parse HEAD)

if [ ! -f workload.$sha ]; then
	./build/builder.sh mkrelease amd64-linux-gnu bin/workload
	mv -f bin.docker_amd64/workload workload.$sha
fi

if [ ! -f cockroach.$sha ]; then
	git clean -xffd ./pkg
	./build/builder.sh mkrelease amd64-linux-gnu
	mv cockroach-linux-2.6.32-gnu-amd64 cockroach.$sha
fi

export GCE_PROJECT=andrei-jepsen
TEST=restore2TB/nodes=10
time caffeinate -- roachtest run "${TEST}" --port 8080 --user jackson --parallelism 100 --debug --count 800 --cpu-quota 1600 --workload workload.${sha} --cockroach ./cockroach.$sha --artifacts artifacts.$sha
