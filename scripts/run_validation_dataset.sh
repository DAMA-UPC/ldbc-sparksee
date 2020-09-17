#!/bin/bash


DRIVER_WORKLOAD=com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload
DRIVER_DATABASE_CONNECTOR=com.ldbc.driver.sparksee.workloads.ldbc.snb.interactive.db.RemoteDb
WORKLOAD=com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload
SPARKSEE_HOST="localhost"

while [[ $# > 0 ]]
do
	key="$1"

	case $key in
		-h|--help)
			HELP=YES
			;;
		-w|--workspace)
			WORKSPACE_DIR=$2
			shift # past argument
			;;
		-l|--ldbcpp)
			LDBCPP_DIR=$2
			LDBCPPBUILD_DIR=$LDBCPP_DIR/build
			shift # past argument
			;;
		-d|--dataset)
			DATA_DIR=$2
			shift # past argument
			;;
		-f|--file)
			WORKLOAD_PROPERTIES_FILE=$2
			shift # past argument
			;;
		-dd|--driverdir)
			DRIVER_DIR=$2
			shift # past argument
			;;

	esac
	shift
done

if [[ ! -z $HELP ]]
then
	echo "-w|--w the workspace used to execute the validation"
	echo "-l|--ldbcpp the ldbc project folder"
	echo "-d|--dataset the path to the validation dataset"
	echo "-f|--file the path to the file with validation config"
	echo "-dd|--driverdir the path to the driver"
	exit
fi


SPARKSEE_LICENSE=$(cat sparksee.cfg | grep "license" | cut -d'=' -f 2)

if [[ -z $DRIVER_DIR ]] || [[ -z $LDBCPP_DIR ]] || [[ -z $LDBCPPBUILD_DIR ]] || [[ -z $DATA_DIR ]] || [[ -z $WORKSPACE_DIR ]] || [[ -z $WORKLOAD_PROPERTIES_FILE ]]; then
  echo "SOME VARIABLES NOT SET. RUN with --help "
  exit
fi

if [[ -z "$SPARKSEE_LICENSE" ]]; then
  echo "MISSING SPARKSEE LICENSE"
  exit
fi

rm -r $WORKSPACE_DIR/validation_set
mkdir -p $WORKSPACE_DIR/validation_set
cp -r $DATA_DIR/* $WORKSPACE_DIR/validation_set


$LDBCPP_DIR/scripts/load_data.sh $WORKSPACE_DIR/validation_set/ 1 1 $LDBCPP_DIR $LDBCPPBUILD_DIR

$LDBCPPBUILD_DIR/server -q remote --threads 1 -t shortestjobfirst > run_validation.server &

java -cp $DRIVER_DIR/target/jeeves-standalone.jar com.ldbc.driver.Client -P $WORKLOAD_PROPERTIES_FILE -p ldbc.snb.interactive.parameters_dir $WORKSPACE_DIR/validation_set --validate_database $WORKSPACE_DIR/validation_set/validation_params.csv -w $DRIVER_WORKLOAD -db $DRIVER_DATABASE_CONNECTOR > run_validation.driver &

DRIVER_PID=$!
wait $DRIVER_PID
python $LDBCPP_DIR/scripts/shutdownServer.py $SPARKSEE_HOST
