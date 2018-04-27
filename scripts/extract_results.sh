#!/bin/bash


for file in $@
do
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
	
	echo "$HEADER"

	for i in {1..14}
	do
		LINE=$i
		for counter in $COUNTERS
		do
		VAL=$(grep " $counter" results_${file}.$i | sed 's/ \+/ /g'| cut -f 2 -d' ')
		LINE="$LINE;$VAL"
		done
		echo "$LINE"
	done
done
