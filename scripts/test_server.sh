#!/bin/bash


#DEFAULT VARIABLES

# DATABASE VARIABLES
DATABASE_REPOSITORY_DIR=
DATABASE_WORKSPACE_DIR=
IMAGE_NAME=snb.gdb

# SERVER VARIABLES
SERVER_DIR=
SERVER_THREAD_STRATEGY="doublebufferencore"
SERVER_THREAD_STRATEGY="shortestjobfirst"
SERVER_N_THREADS="1"

# DRIVER VARIABLES
DRIVER_DIR=
DRIVER_N_THREADS="1"
DRIVER_TCR=1.0
DRIVER_IGNORE_TCR=
DRIVER_N_WARMUP_OPERATIONS=1000
DRIVER_N_OPERATIONS=1000
DRIVER_WORKLOAD=com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload
DRIVER_PARAMETERS_DIR_OPTION=ldbc.snb.interactive.parameters_dir
DRIVER_DATABASE_CONNECTOR=com.ldbc.driver.sparksee.workloads.ldbc.snb.interactive.db.RemoteDb
SPARKSEE_HOST="localhost"

# options parsing
while [[ $# > 0 ]]
do
	key="$1"

	case $key in
		-h|--help)
			HELP=YES
			;;
		-r|--repository)
			DATABASE_REPOSITORY_DIR="$2"
			shift # past argument
			;;
		-s|--server)
			SERVER_DIR="$2"
			shift # past argument
			;;
		-d|--driver)
			DRIVER_DIR="$2"
			shift # past argument
			;;
		-w|--workspace)
			DATABASE_WORKSPACE_DIR="$2"
			shift # past argument
			;;
		-st|--serverthreads)
			SERVER_N_THREADS="$2"
			shift # past argument
			;;
		-dt|--driverthreads)
			DRIVER_N_THREADS="$2"
			shift # past argument
			;;
		-sf|--scalefactor)
			SCALE_FACTOR="$2"
			shift # past argument
			;;
		-m|--maxspeed)
			DRIVER_IGNORE_TCR="-ignore_scheduled_start_times true"
			;;
		-t|--tag)
			TAG="$2"
			shift # past argument
			;;
		-o|--operations)
			DRIVER_N_OPERATIONS="$2"
			shift # past argument
			;;
		-wo|--warmupoperations)
			DRIVER_N_WARMUP_OPERATIONS="$2"
			shift # past argument
			;;
		-tcr|--tcratio)
			DRIVER_TCR="$2"
			shift # past argument
			;;
		-str|--strategy)
			SERVER_THREAD_STRATEGY="$2"
			shift # past argument
			;;
		-f|--driverworkloadfile)
			DRIVER_WORKLOAD_FILE="$2"
			shift # past argument
			;;
		-b|--bi)
			BI_WORKLOAD="yes"
			;;
		-p|--perf)
			PERF_FILE="$2"
			shift # past argument
			;;
		-hs|--host)
			SPARKSEE_HOST="$2"
			shift # past argument
			;;
		-ns|--noserver)
			NO_SERVER="yes"
			;;
		-nd|--nodriver)
			NO_DRIVER="yes"
			;;
		-rp|--results-path)
			RESULTS_PATH="$2"
      shift # past argument
			;;
	esac
	shift
done

if [[ ! -z $HELP ]]
then
	echo " Options: "
	echo " -r|--repository <path to where the datasets are stored> "
	echo " -s|--server <path to the LDBCpp root folder> "
	echo " -d|--driver <path to the driver root folder> "
	echo " -w|--workspace <path to the workspace used to execute the benchmark> "
	echo " -sf|--scalefactor <the scale factor to run> "
	echo " -st|--serverthreads <the number of threads in the server> "
	echo " -dt|--driverthreads <the number of threads in the driver> "
	echo " -m|--maxspeed the driver runs at max speed by ignoring scheduled times "
	echo " -tcr|--tcratio the tcr of the run"
	echo " -t|--tag the tag identifying the sparksee version"
	echo " -o|--operations the number of operations to run"
	echo " -wo|--warmupoperations the number of warmup operations to run"
	echo " -f|--driverworkloadfile path to the workload file" 
	echo " -b|--bi execute bi workload"
	echo " -p|--perf execute with perf stat with the counters specified in file"
	echo " -rp|--results-path path to the results repository" 
	exit
fi

if [[ -z "$DATABASE_REPOSITORY_DIR" ]]; then
  echo "-r/--repository not set"
  exit
fi

if [[ -z "$SERVER_DIR" ]]; then
  echo "-s/--server not set"
  exit
fi

if [[ -z "$DRIVER_DIR" ]]; then
  echo "-d/--driver not set"
  exit
fi

if [[ -z "$DATABASE_WORKSPACE_DIR" ]]; then
  echo "-w/--workspace not set"
  exit
fi

if [[ -z "$SERVER_N_THREADS" ]]; then
  echo "-st/--serverthreads not set"
  exit
fi

if [[ -z "$DRIVER_N_THREADS" ]]; then
  echo "-dt/--driverthreads not set"
  exit
fi

if [[ -z "$IMAGE_NAME" ]]; then
  echo "IMAGE_NAME not set"
  exit
fi

if [[ -z "$TAG" ]]; then
  echo "-t/--tag not set"
  exit
fi

if [[ -z "$SCALE_FACTOR" ]]; then
  echo "-sf/--scalefactor not set"
  exit
fi

if [[ -z "$DRIVER_WORKLOAD_FILE" ]]; then
  echo "-f/--driverworkloadfile not set"
  exit
fi

# Setting the final driver configuration
if [[ ! -z "$BI_WORKLOAD" ]]; then
	DRIVER_WORKLOAD=com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiWorkload
	DRIVER_PARAMETERS_DIR_OPTION=ldbc.snb.bi.parameters_dir
fi

OUTPUT_FILE_BASE_NAME=execution_${SCALE_FACTOR}_${SERVER_N_THREADS}_${DRIVER_N_THREADS}

######################## PRINTING BENCHMARK CONFIG ##################################
rm -f ${OUTPUT_FILE_BASE_NAME}.config
echo "scale_factor=$SCALE_FACTOR" >> ${OUTPUT_FILE_BASE_NAME}.config
echo "tag=$TAG" >> ${OUTPUT_FILE_BASE_NAME}.config
echo "driver_threads=$DRIVER_N_THREADS" >> ${OUTPUT_FILE_BASE_NAME}.config
echo "server_threads=$SERVER_N_THREADS" >> ${OUTPUT_FILE_BASE_NAME}.config
echo "warmup_ops=$DRIVER_N_WARMUP_OPERATIONS" >> ${OUTPUT_FILE_BASE_NAME}.config
echo "ops=$DRIVER_N_OPERATIONS" >> ${OUTPUT_FILE_BASE_NAME}.config
echo "tcr=$DRIVER_TCR" >> ${OUTPUT_FILE_BASE_NAME}.config
echo "ignore_tcr=$DRIVER_IGNORE_TCR" >> ${OUTPUT_FILE_BASE_NAME}.config
echo "workload=$DRIVER_WORKLOAD" >> ${OUTPUT_FILE_BASE_NAME}.config 


DATABASE_SOURCE_DIR=$DATABASE_REPOSITORY_DIR/$SCALE_FACTOR/$TAG
DRIVER_WORKLOAD_OPTS="-w $DRIVER_WORKLOAD -db $DRIVER_DATABASE_CONNECTOR -p $DRIVER_PARAMETERS_DIR_OPTION
$DATABASE_SOURCE_DIR/substitution_parameters -p ldbc.snb.interactive.data_dir $DATABASE_SOURCE_DIR/social_network -p
ldbc.snb.interactive.updates_dir $DATABASE_SOURCE_DIR/social_network "
DRIVER_OPTS="-s 2 -tc $DRIVER_N_THREADS -nm LDBC -rd $RESULTS_PATH -tu MILLISECONDS -tcr $DRIVER_TCR -sw 1
$DRIVER_IGNORE_TCR"

####################################################################################


if [[ -a $DATABASE_WORKSPACE_DIR/$TAG/$IMAGE_NAME ]]
then
	rm -f $DATABASE_WORKSPACE_DIR/$TAG/$IMAGE_NAME*
fi

if [[ -z $DATABASE_SOURCE_DIR/$TAG/$IMAGE_NAME  ]]
then
	echo "SCALE FACTOR IS NOT LOADED"
	echo "LOAD IT FIRST"
	exit
fi


if [[ -z $NO_SERVER ]]
then
	echo "COPYING IMAGE $DATABASE_SOURCE_DIR/$IMAGE_NAME to $DATABASE_WORKSPACE_DIR/$IMAGE_NAME"

	cp -rf $DATABASE_SOURCE_DIR/$IMAGE_NAME $DATABASE_WORKSPACE_DIR/$IMAGE_NAME
	
	echo "EXECUTING SERVER WITH $SERVER_N_THREADS"
	
  rm sparksee.log

  if [[ -z $NO_DRIVER ]]
  then
    $SERVER_DIR/build/server -q remote -t $SERVER_THREAD_STRATEGY --threads $SERVER_N_THREADS --database $DATABASE_WORKSPACE_DIR/$IMAGE_NAME &> ${OUTPUT_FILE_BASE_NAME}.server &
  else
    $SERVER_DIR/build/server -q remote -t $SERVER_THREAD_STRATEGY --threads $SERVER_N_THREADS --database $DATABASE_WORKSPACE_DIR/$IMAGE_NAME &> ${OUTPUT_FILE_BASE_NAME}.server
  fi
fi


####################################################################################


if [[ -z $NO_DRIVER ]]
then
python2 $SERVER_DIR/scripts/waitConnection.py $SPARKSEE_HOST
echo "EXECUTING DRIVER WORKLOAD"
echo "java -cp $DRIVER_DIR/target/jeeves-standalone.jar com.ldbc.driver.Client -wu $DRIVER_N_WARMUP_OPERATIONS -oc $DRIVER_N_OPERATIONS $DRIVER_OPTS $DRIVER_WORKLOAD_OPTS -P $DRIVER_WORKLOAD_FILE -P $DATABASE_SOURCE_DIR/social_network/updateStream.properties -p \"sparksee.host|$SPARKSEE_HOST\" &> ${OUTPUT_FILE_BASE_NAME}.driver "
java -cp $DRIVER_DIR/target/jeeves-standalone.jar com.ldbc.driver.Client -wu $DRIVER_N_WARMUP_OPERATIONS -oc $DRIVER_N_OPERATIONS $DRIVER_OPTS $DRIVER_WORKLOAD_OPTS -P $DRIVER_WORKLOAD_FILE -P $DATABASE_SOURCE_DIR/social_network/updateStream.properties -p "sparksee.host|$SPARKSEE_HOST" &> ${OUTPUT_FILE_BASE_NAME}.driver 
fi

if [[ -z $NO_DRIVER ]]
then
	mv $RESULTS_PATH/LDBC-results_log.csv ${OUTPUT_FILE_BASE_NAME}.log
	python2 $SERVER_DIR/scripts/shutdownServer.py $SPARKSEE_HOST
	mkdir -p $RESULTS_PATH/$SCALE_FACTOR/$TAG/
	mv -f ${OUTPUT_FILE_BASE_NAME}.driver $RESULTS_PATH/$SCALE_FACTOR/$TAG/
	mv -f ${OUTPUT_FILE_BASE_NAME}.log $RESULTS_PATH/$SCALE_FACTOR/$TAG/
	mv -f ${OUTPUT_FILE_BASE_NAME}.config $RESULTS_PATH/$SCALE_FACTOR/$TAG/
fi

if [[ -z $NO_SERVER ]]
then
	mkdir -p $RESULTS_PATH/$SCALE_FACTOR/$TAG/
  cp sparksee.cfg $RESULTS_PATH/$SCALE_FACTOR/$TAG/${OUTPUT_FILE_BASE_NAME}.sparksee.cfg
  mv sparksee.log $RESULTS_PATH/$SCALE_FACTOR/$TAG/${OUTPUT_FILE_BASE_NAME}.sparksee.log
	mv -f ${OUTPUT_FILE_BASE_NAME}.server $RESULTS_PATH/$SCALE_FACTOR/$TAG/
fi

exit 0;
