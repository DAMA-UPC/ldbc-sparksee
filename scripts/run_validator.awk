
BEGIN { 
  FS = "|";
  average_difference=0
  num_queries=0;
  threshold = 1000;
  num_above = 0;
}

{ 
   if( $3 - $2 > threshold) {
    num_above+=1
	average_difference+=$3-$2
	print $0
   }
   num_queries+=1
}

END {
	print "% ABOVE THRESHOLD " num_above * 100 / num_queries;
	print "AVERAGE DIFFERENCE " average_difference / num_queries;
	print "NUM ABOVE THRESHOLD " num_above 
	print "NUM QUERIES " num_queries
   }
   
