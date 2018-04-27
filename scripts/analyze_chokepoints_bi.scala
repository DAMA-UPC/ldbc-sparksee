

import scala.io.Source
import scala.collection.mutable.HashMap


def jaccard( x : Array[Boolean], y : Array[Boolean] ) : Double = {
  var intersection = 0
  var union = 0
  assert(x.length == y.length )
  for( i <- 0 until x.length ) {

      union += (x(i) || y(i) match {
        case true => 1
        case _ => 0
      })

      intersection += (x(i) && y(i) match {
        case true => 1 
        case _ => 0
      })
  }
  intersection.toDouble / union.toDouble
}

//Reading queries line by line and creating for each query an array of integers. 
//Droping the first line
var queries = Source.fromFile("./chokepoints_bi.txt").getLines().drop(1).map( x => x.split(" ")).toArray
var queries_boolean : Array[Array[Boolean]] = queries.map( x => x.map(y => y match { 
  case "0" => false 
  case "1" => true 
 })).toArray

assert(queries_boolean.length == 25)

var pairs : List[(Int,Int,Double)] = List[(Int,Int,Double)]()
for( i <- 0 until queries_boolean.length ) {
  for( j <- i+1 until queries_boolean.length ) {
    pairs = pairs :+ (i+1,j+1,jaccard(queries_boolean(i),queries_boolean(j)))
  }
}

pairs = pairs.sortWith( (a,b) => (a._3 > b._3) )
println()
(0 until 10).foreach( x => println(pairs(x)) ) 

var counts : HashMap[Int,Int] = HashMap[Int,Int]()
(0 until queries_boolean(0).length).foreach( x => counts.put(x,0))

queries_boolean.foreach( x => (0 until x.length).foreach( y => {
  x(y) match {
    case true => counts.put(y,counts(y)+1)
    case _ => 
  }
} )
)

counts.toSeq.sortWith( (a,b) => (a._2 > b._2) ).foreach(println)


