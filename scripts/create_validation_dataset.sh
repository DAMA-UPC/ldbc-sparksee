#!/bin/bash


DATA_DIR=$1
WORKSPACE_DIR="/tmp/"
LDBCPP_DIR=/home/aprat/projects/LDBCpp/trunk
LDBCPPBUILD_DIR=$LDBCPP_DIR/build
DRIVER_DIR=./ldbc_driver
OUTPUT_DIR=./output_validation
#WORKLOAD=com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload
WORKLOAD=com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiWorkload


SPARKSEE_LICENSE=$(cat sparksee.cfg | grep "license" | cut -d'=' -f 2)


if [[ -z "$LDBCPP_DIR" ]] || [[ -z "$LDBCPPBUILD_DIR" ]] || [[ -z "$DATA_DIR" ]]; then
  echo "LDBCPP PROJECT or DATA_DIR VARIABLES NOT SET "
  exit
fi

if [[ -z "$SPARKSEE_LICENSE" ]]; then
  echo "MISSING SPARKSEE LICENSE"
  exit
fi


mkdir -p $OUTPUT_DIR
rm -r $WORKSPACE_DIR/validation_set
mkdir -p $WORKSPACE_DIR/validation_set
cp -r $DATA_DIR/social_network/* $WORKSPACE_DIR/validation_set
cp -r $DATA_DIR/substitution_parameters/* $WORKSPACE_DIR/validation_set

UPDATES_DATA=$(cat $WORKSPACE_DIR/validation_set/updateStream.properties)

WORKLOAD_PROPERTIES_FILE=./ldbc_driver/configuration/ldbc/snb/bi/ldbc_snb_bi_SF-0001.properties
#WORKLOAD_PROPERTIES_FILE=./ldbc_snb_interactive.properties
WORKLOAD_PROPERTIES=$(cat $WORKLOAD_PROPERTIES_FILE)

echo "
# -------------------------------------
# -------------------------------------
# ----- LDBC Driver Configuration -----
# -------------------------------------
# -------------------------------------

# ***********************
# *** driver defaults ***
# ***********************

# status display interval (intermittently show status during benchmark execution)
# INTEGER (seconds)
# COMMAND: -s/--status
status=1

# thread pool size to use for executing operation handlers
# INTEGER
# COMMAND: -tc/--thread_count
thread_count=1

# name of the benchmark run
# STRING
# COMMAND: -nm/--name
name=LDBC-SNB

# path specifying where to write the benchmark results file
# STRING
# COMMAND: -rd/--results_dir
# results_dir=

# create a csv file containing simple data about the execution of every operation in the workload
# BOOLEAN
# COMMAND: -rl/--results_log
results_log=false

# time unit to use for measuring performance metrics (e.g., query response time)
# ENUM ([NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS, MINUTES])
# COMMAND: -tu/--time_unit
time_unit=MILLISECONDS

# used to 'compress'/'stretch' durations between operation start times to increase/decrease benchmark load
# e.g. 2.0 = run benchmark 2x slower, 0.1 = run benchmark 10x faster
# DOUBLE
# COMMAND: -tcr/--time_compression_ratio
time_compression_ratio=0.001

# NOT USED AT PRESENT - reserved for distributed driver mode
# specifies the addresses of other driver processes, so they can find each other
# LIST (e.g., peer1|peer2|peer3)
# COMMAND: -pids/--peer_identifiers
peer_identifiers=

# enable validation that will check if the provided database implementation is correct
# parameter value specifies where to find the validation parameters file
# STRING
# COMMAND: -vdb/--validate_database
# validate_database=

# generate validation parameters file for validating correctness of database implementations
# parameter values specify: (1) where to create the validation parameters file (2) how many validation parameters to generate
# STRING|INTEGER (e.g., validation_parameters.csv|1000)
# COMMAND: -cvp/--create_validation_parameters
create_validation_parameters=$WORKSPACE_DIR/validation_set/validation_params.csv|3000

# enable validation that will check if the provided workload implementation is correct
# BOOLEAN
# COMMAND: -vw/--validate_workload
validate_workload=false

# calculate & display workload statistics (operation mix, etc.)
# BOOLEAN
# COMMAND: -stats/--workload_statistics
workload_statistics=false

# sleep duration (ms) injected into busy wait loops (to reduce CPU consumption)
# LONG (milliseconds)
# COMMAND: -sw/--spinner_wait_duration
spinner_wait_duration=0

# print help string - usage instructions
# BOOLEAN
# COMMAND: -help
help=false

# executes operations as fast as possible, ignoring their scheduled start times
# BOOLEAN
# COMMAND: -ignore_scheduled_start_times
ignore_scheduled_start_times=false

# ***************************************************************
# *** the following should be set by workload implementations ***
# ***************************************************************

# fully qualified class name of the Workload (class) implementation to execute
# STRING (e.g., com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload)
# COMMAND: -w/--workload
workload=$WORKLOAD

# number of operations to generate during benchmark execution
# LONG
# COMMAND: -oc/--operation_count
operation_count=10000

# ************************************************************************************
# *** the following should be set by vendor implementations for specific workloads ***
# ************************************************************************************

# fully qualified class name of the Db (class) implementation to execute
# STRING (e.g., com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb)
# COMMAND: -db/--database
database=com.ldbc.driver.sparksee.workloads.ldbc.snb.interactive.db.RemoteDb

ldbc.snb.interactive.parameters_dir=$WORKSPACE_DIR/validation_set/
ldbc.snb.bi.parameters_dir=$WORKSPACE_DIR/validation_set/
ldbc.snb.interactive.short_read_dissipation=0.2
ldbc.snb.interactive.updates_dir=$WORKSPACE_DIR/validation_set/
$WORKLOAD_PROPERTIES 
$UPDATES_DATA
# NOTE: Sparksee was used to create the parameters
" > $WORKSPACE_DIR/readwrite_sparksee--ldbc_driver_config--validation_parameter_creation.properties


echo " # -------------------------------------
# -------------------------------------
# ----- LDBC Driver Configuration -----
# -------------------------------------
# -------------------------------------

# TODO: uncomment this and point it to where it wants to be pointed
database=com.ldbc.driver.sparksee.workloads.ldbc.snb.interactive.db.RemoteDb

# TODO: uncomment this and point it to where it wants to be pointed
validate_database=$WORKSPACE_DIR/validation_set/validation_params.csv

# TODO: uncomment this and point it to where it wants to be pointed
ldbc.snb.interactive.parameters_dir=$WORKSPACE_DIR/validation_set
ldbc.snb.bi.parameters_dir=$WORKSPACE_DIR/validation_set

# TODO: uncomment this and point it to where it wants to be pointed
# path specifying where to write the benchmark results file
# STRING
# COMMAND: -rd/--results_dir
# results_dir=

# ***********************
# *** driver defaults ***
# ***********************

# status display interval (intermittently show status during benchmark execution)
# INTEGER (seconds)
# COMMAND: -s/--status
status=1

# thread pool size to use for executing operation handlers
# INTEGER
# COMMAND: -tc/--thread_count
thread_count=1

# name of the benchmark run
# STRING
# COMMAND: -nm/--name
name=LDBC-SNB

# path specifying where to write the benchmark results file
# STRING
# COMMAND: -rd/--results_dir
results_dir=

# create a csv file containing simple data about the execution of every operation in the workload
# BOOLEAN
# COMMAND: -rl/--results_log
results_log=false

# time unit to use for measuring performance metrics (e.g., query response time)
# ENUM ([NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS, MINUTES])
# COMMAND: -tu/--time_unit
time_unit=MILLISECONDS

# used to 'compress'/'stretch' durations between operation start times to increase/decrease benchmark load
# e.g. 2.0 = run benchmark 2x slower, 0.1 = run benchmark 10x faster
# DOUBLE
# COMMAND: -tcr/--time_compression_ratio
time_compression_ratio=0.001

# NOT USED AT PRESENT - reserved for distributed driver mode
# specifies the addresses of other driver processes, so they can find each other
# LIST (e.g., peer1|peer2|peer3)
# COMMAND: -pids/--peer_identifiers
peer_identifiers=

# generate validation parameters file for validating correctness of database implementations
# parameter values specify: (1) where to create the validation parameters file (2) how many validation parameters to generate
# STRING|INTEGER (e.g., validation_parameters.csv|1000)
# COMMAND: -cvp/--create_validation_parameters
# create_validation_parameters=

# enable validation that will check if the provided workload implementation is correct
# BOOLEAN
# COMMAND: -vw/--validate_workload
validate_workload=true

# calculate & display workload statistics (operation mix, etc.)
# BOOLEAN
# COMMAND: -stats/--workload_statistics
workload_statistics=false

# sleep duration (ms) injected into busy wait loops (to reduce CPU consumption)
# LONG (milliseconds)
# COMMAND: -sw/--spinner_wait_duration
spinner_wait_duration=0

# print help string - usage instructions
# BOOLEAN
# COMMAND: -help
help=false

# executes operations as fast as possible, ignoring their scheduled start times
# BOOLEAN
# COMMAND: -ignore_scheduled_start_times
ignore_scheduled_start_times=false

# ***************************************************************
# *** the following should be set by workload implementations ***
# ***************************************************************

# fully qualified class name of the Workload (class) implementation to execute
# STRING (e.g., com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload)
# COMMAND: -w/--workload
workload=$WORKLOAD

# number of operations to generate during benchmark execution
# LONG
# COMMAND: -oc/--operation_count
operation_count=100000
$WORKLOAD_PROPERTIES 
$UPDATES_DATA
" > $WORKSPACE_DIR/readwrite_sparksee--ldbc_driver_config--db_validation.properties

cp $LDBCPP_DIR/scripts/load_data.sh ./
./load_data.sh $WORKSPACE_DIR/validation_set/ 1 1 $LDBCPP_DIR $LDBCPPBUILD_DIR

$LDBCPPBUILD_DIR/server -q remote --threads 1 -t shortestjobfirst > create_validation.server &

java -cp $DRIVER_DIR/target/jeeves-0.3-SNAPSHOT.jar com.ldbc.driver.Client -P $WORKSPACE_DIR/readwrite_sparksee--ldbc_driver_config--validation_parameter_creation.properties > create_validation.driver &

DRIVER_PID=$!
wait $DRIVER_PID
python $LDBCPP_DIR/scripts/shutdownServer.py

cp $WORKSPACE_DIR/*.properties $OUTPUT_DIR
cp -r $WORKSPACE_DIR/validation_set $OUTPUT_DIR
CURRENT=$(pwd)
cd $OUTPUT_DIR
tar cvfz readwrite_sparksee--validation_set.tar.gz validation_set
rm -r validation_set
cd $CURRENT

