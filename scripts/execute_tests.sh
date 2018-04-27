#!/bin/bash

rm test_results.txt
touch test_results.txt
for file in $@
do
	echo "Executing perf with following counters:"
	cat $file

	COUNTERS=""
	while read line
	do
		COUNTERS="$COUNTERS $line"
	done < $file
	
	HEADER="query"
	for counter in $COUNTERS
	do
		HEADER="$HEADER;$counter"
	done

	echo "$HEADER" >> test_results.txt

	for i in $(seq 1 14)
	do
		$LDBCPP_DIR/scripts/ldbc_snb.sh run --repository $UNISERVER_LDBCPP_REPO --workspace $UNISERVER_LDBCPP_WORKSPACE -t uniserver -str doublebufferencore -o 1000 -wo 1000 -m -sf 0001 -st 8 -dt 16 -f ldbc_driver/configuration/ldbc/snb/interactive/ldbc_snb_interactive_SF-0001.properties.$i -p $file 

		LINE=$i
		for counter in $COUNTERS
		do
		VAL=$(grep " $counter" execution_0001_8_16.server | sed 's/ \+/ /g'| cut -f 2 -d' ')
		LINE="$LINE;$VAL"
		done
		echo "$LINE" >> test_results.txt
	done
done
