#!/usr/bin/env bash

set -a
. .env
set +a

set -e -o pipefail

[ -d "${NS3_HOME}/data" ] || ln -s "$(realpath data)" "${NS3_HOME}/data"

for param in "${@}"; do
    export SIM_ID=$(basename $param | cut -d. -f1)
    export SIM_PARAMS="$param"
    export JENA_FUSEKI_HOME="${JENA_FUSEKI_HOME}"
    export JENA_FUSEKI_IMAGE="${JENA_FUSEKI_IMAGE}"
    if [[ "${SIM_ID}" =~ baseline ]]; then
        export SIM_DISCARD_RATIO_FOR_CENTRALIZED_CASE=1.0
    else
        export SIM_DISCARD_RATIO_FOR_CENTRALIZED_CASE=
    fi
    logfile="data/archive/${SIM_ID}.log"
    dbfolder="${JENA_FUSEKI_HOME}/databases/DB_${SIM_ID}"
    if [ -f "$logfile" ]; then
        echo "ERROR: ${logfile@Q} already exists! Back it up first!"
	exit 1
    fi
    if [ -d "$dbfolder" ]; then
        echo "ERROR: ${dbfolder@Q} already exists! Back it up first!"
	exit 1
    fi
    echo "SIM_ID=${SIM_ID@Q}, SIM_PARAMS=${SIM_PARAMS@Q}, writing logs to ${logfile}"
    (cd ${NS3_HOME} && ./ns3 run "${SIM_NAME}") > "${logfile}"
    [[ "${SIM_ID}" =~ baseline ]] || mv "${JENA_FUSEKI_HOME}/databases/DB2" "$dbfolder"
    mkdir -p "${JENA_FUSEKI_HOME}/databases/DB2"
done
