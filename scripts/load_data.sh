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
LDBC_SPARKSEE=$4

echo $LDBC_SPARKSEE

if [[ ! -f "./sparksee.cfg" ]]; then
  echo "Missing sparksee.cfg file"
  exit
fi

SPARKSEE_LICENSE=$(cat sparksee.cfg | grep "license" | cut -d'=' -f 2)

if [[ -z ${LDBC_SPARKSEE} ]]; then
  echo "LDBC_SPARKSEE VARIABLE NOT SET"
  exit
fi

LDBC_SPARKSEE_BUILD_DIR=$LDBC_SPARKSEE/build

if [[ -z $SPARKSEE_LICENSE ]]; then
  echo "MISSING SPARKSEE LICENSE"
  exit
fi

cp sparksee.cfg $DATA_DIR/
cd $DATA_DIR

echo "DATA_DIR="$DATA_DIR
echo "LDBC_SPARKSEE="$LDBC_SPARKSEE
echo "LDBC_SPARKSEE_BUILD_DIR="$LDBC_SPARKSEE_BUILD_DIR

#$LDBC_SPARKSEE/scripts/sortFiles.sh comment_isLocatedIn_place_?_?.csv > temp
#python2 $LDBC_SPARKSEE/scripts/repartitionFiles.py temp comment_isLocatedIn_place $2 $3
#
#$LDBC_SPARKSEE/scripts/sortFiles.sh post_isLocatedIn_place_?_?.csv > temp
#python2 $LDBC_SPARKSEE/scripts/repartitionFiles.py temp post_isLocatedIn_place $2 $3
#
#$LDBC_SPARKSEE/scripts/sortFiles.sh person_isLocatedIn_place_?_?.csv > temp
#python2 $LDBC_SPARKSEE/scripts/repartitionFiles.py temp person_isLocatedIn_place $2 $3
#
#$LDBC_SPARKSEE/scripts/sortFiles.sh organisation_isLocatedIn_place_?_?.csv > temp
#python2 $LDBC_SPARKSEE/scripts/repartitionFiles.py temp organisation_isLocatedIn_place $2 $3
#
#$LDBC_SPARKSEE/scripts/sortFiles.sh comment_hasCreator_person_?_?.csv > temp
#python2 $LDBC_SPARKSEE/scripts/repartitionFiles.py temp comment_hasCreator_person $2 $3

rm temp
python2 $LDBC_SPARKSEE/scripts/createHasMemberWithPostsCSV.py
python2 $LDBC_SPARKSEE/scripts/createLanguages.py

#cp $LDBC_SPARKSEE/scripts/loader.txt loader.txt.tmp
#sed -i "s/LICENSE_CODE/$SPARKSEE_LICENSE/g" loader.txt.tmp
#sed -i "s#/path/to/data/#$DATA_DIR/#g" loader.txt.tmp
#sed -i "s#set,partitions,1#set,partitions,$NUM_THREADS#g" loader.txt.tmp
#sed -i "s#set,thread_partitions,1#set,thread_partitions,$NUM_PARTITIONS#g" loader.txt.tmp
#$LDBC_SPARKSEE_BUILD_DIR/snbLoaderStandalone loader.txt.tmp
$LDBC_SPARKSEE/scripts/scripts/load.sh $LDBC_SPARKSEE 
$LDBC_SPARKSEE_BUILD_DIR/precompute snb.gdb 8
cd $CURRENT_DIR
mv $DATA_DIR/snb.gdb* .
