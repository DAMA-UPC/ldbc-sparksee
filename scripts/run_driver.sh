#!/bin/bash

sf=1timestamps

IMAGE_BASE_FOLDER=/home/aprat/projects/LDBC/datasets
IMAGE_SOURCE_DIR="$IMAGE_BASE_FOLDER/$sf"
IMAGE_WORKSPACE_DIR="./"

DRIVER_TCR=0.0005
DRIVER_N_WARMUP_OPERATIONS=0
DRIVER_N_OPERATIONS=5000
DRIVER_WORKLOAD_FILE=./ldbc_snb_interactive.properties.reads
DRIVER_N_THREADS=1
DRIVER_WORKLOAD=com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload
DRIVER_DATABASE_CONNECTOR=com.ldbc.driver.sparksee.workloads.ldbc.snb.interactive.db.RemoteDb
DRIVER_OPTS="-s 2 -tc $DRIVER_N_THREADS -nm LDBC -rd results false -tu MILLISECONDS -tcr $DRIVER_TCR -sw 1
-ignore_scheduled_start_times true"
DRIVER_PARAMETERS_DIR_OPTION=ldbc.snb.interactive.parameters_dir
DRIVER_WORKLOAD_OPTS="-w $DRIVER_WORKLOAD -db $DRIVER_DATABASE_CONNECTOR -p $DRIVER_PARAMETERS_DIR_OPTION "$IMAGE_SOURCE_DIR/substitution_parameters" -p ldbc.snb.interactive.data_dir "$IMAGE_SOURCE_DIR/social_network" -p ldbc.snb.interactive.updates_dir "$IMAGE_SOURCE_DIR/social_network""

java -cp ./ldbc_driver/target/jeeves-0.3-SNAPSHOT.jar com.ldbc.driver.Client -wu $DRIVER_N_WARMUP_OPERATIONS -oc $DRIVER_N_OPERATIONS $DRIVER_OPTS $DRIVER_WORKLOAD_OPTS -P $DRIVER_WORKLOAD_FILE -P $IMAGE_SOURCE_DIR/social_network/updateStream.properties 
