#!/bin/bash


DATA_DIR=$1
WORKSPACE_DIR="/tmp/"
LDBCPP_DIR=/home/aprat/projects/LDBCpp/trunk
LDBCPPBUILD_DIR=$LDBCPP_DIR/build
DRIVER_DIR=./ldbc_driver
#WORKLOAD=com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiWorkload
WORKLOAD=com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload


SPARKSEE_LICENSE=$(cat sparksee.cfg | grep "license" | cut -d'=' -f 2)


if [[ -z "$LDBCPP_DIR" ]] || [[ -z "$LDBCPPBUILD_DIR" ]] || [[ -z "$DATA_DIR" ]]; then
  echo "LDBCPP PROJECT or DATA_DIR VARIABLES NOT SET "
  exit
fi

if [[ -z "$SPARKSEE_LICENSE" ]]; then
  echo "MISSING SPARKSEE LICENSE"
  exit
fi

rm -r $WORKSPACE_DIR/validation_set
mkdir -p $WORKSPACE_DIR/validation_set
cp -r $DATA_DIR/social_network/string_date/* $WORKSPACE_DIR/validation_set
cp -r $DATA_DIR/substitution_parameters/* $WORKSPACE_DIR/validation_set
cp -r $DATA_DIR/validation_params.csv $WORKSPACE_DIR/validation_set
cp -r $DATA_DIR/updates/* $WORKSPACE_DIR/validation_set


#cp -r $DATA_DIR/* $WORKSPACE_DIR/validation_set

WORKLOAD_PROPERTIES_FILE=$2

$LDBCPP_DIR/data/load_data.sh $WORKSPACE_DIR/validation_set/ 1 1 $LDBCPP_DIR $LDBCPPBUILD_DIR

$LDBCPPBUILD_DIR/server -q remote --threads 1 -t shortestjobfirst > run_validation.server &

java -cp $DRIVER_DIR/target/jeeves-0.3-SNAPSHOT.jar com.ldbc.driver.Client -P $WORKLOAD_PROPERTIES_FILE > run_validation.driver &

DRIVER_PID=$!
wait $DRIVER_PID
python $LDBCPP_DIR/data/shutdownServer.py
