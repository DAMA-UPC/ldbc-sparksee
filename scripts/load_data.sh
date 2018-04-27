#!/bin/bash

if [[ -z "$1" ]] || [[ -z "$2" ]] || [[ -z "$3" ]]; then
  echo "WRONG ARGUMENTS"
  echo "Usage load_data.sh <data_folder> <num_threads> <num_partitions>"
  exit
fi

export TZ=GMT


CURRENT_DIR=$(pwd)
DATA_DIR=$1
NUM_THREADS=$2
NUM_PARTITIONS=$3

##########################################
# WARNING: SET VARIABLES WITH FULL PATHS #
##########################################
LDBCPP_DIR=$4
LDBCPP_BUILD_DIR=$5

SPARKSEE_LICENSE=$(cat sparksee.cfg | grep "license" | cut -d'=' -f 2)

if [[ -z "$LDBCPP_DIR" ]] || [[ -z "$LDBCPP_BUILD_DIR" ]]; then
  echo "LDBCPP PROJECT VARIABLES NOT SET"
  exit
fi

if [[ -z "$SPARKSEE_LICENSE" ]]; then
  echo "MISSING SPARKSEE LICENSE"
  exit
fi

cp sparksee.cfg $DATA_DIR/
cd $DATA_DIR

$LDBCPP_DIR/scripts/sortFiles.sh comment_isLocatedIn_place_?_?.csv > temp
python2 $LDBCPP_DIR/scripts/repartitionFiles.py temp comment_isLocatedIn_place $2 $3

$LDBCPP_DIR/scripts/sortFiles.sh post_isLocatedIn_place_?_?.csv > temp
python2 $LDBCPP_DIR/scripts/repartitionFiles.py temp post_isLocatedIn_place $2 $3

$LDBCPP_DIR/scripts/sortFiles.sh person_isLocatedIn_place_?_?.csv > temp
python2 $LDBCPP_DIR/scripts/repartitionFiles.py temp person_isLocatedIn_place $2 $3

$LDBCPP_DIR/scripts/sortFiles.sh organisation_isLocatedIn_place_?_?.csv > temp
python2 $LDBCPP_DIR/scripts/repartitionFiles.py temp organisation_isLocatedIn_place $2 $3

$LDBCPP_DIR/scripts/sortFiles.sh comment_hasCreator_person_?_?.csv > temp
python2 $LDBCPP_DIR/scripts/repartitionFiles.py temp comment_hasCreator_person $2 $3

rm temp
python2 $LDBCPP_DIR/scripts/createHasMemberWithPostsCSV.py
python2 $LDBCPP_DIR/scripts/createLanguages.py

cp $LDBCPP_DIR/scripts/loader.txt loader.txt.tmp
sed -i "s/LICENSE_CODE/$SPARKSEE_LICENSE/g" loader.txt.tmp
sed -i "s#/path/to/data/#$DATA_DIR/#g" loader.txt.tmp
sed -i "s#set,partitions,1#set,partitions,$NUM_THREADS#g" loader.txt.tmp
sed -i "s#set,thread_partitions,1#set,thread_partitions,$NUM_PARTITIONS#g" loader.txt.tmp
$LDBCPP_BUILD_DIR/snbLoaderStandalone loader.txt.tmp
#$LDBCPP_DIR/scripts/scripts/load.sh $LDBCPP_DIR 
$LDBCPP_BUILD_DIR/precompute snb.gdb 4
cd $CURRENT_DIR
mv $DATA_DIR/snb.gdb* .
