
import scala.io.Source
import scala.collection._
import scala.collection.immutable.List
import scala.collection.immutable.Set
import scala.collection.immutable.Map


val nodetypes = Map[String,Set[String]](
                                "city" -> Set[String](),
                                "comment" -> Set[String](),
                                "company" -> Set[String](),
                                "country" -> Set[String](),
                                "forum" -> Set[String]("id","title","creationDate"),
                                "university" -> Set[String](),
                                "company" -> Set[String](),
                                "person" -> Set[String]("id","firstName","lastName","gender","birthday","email","speaks","browserUsed","locationIP","creationDate"),
                                "post" -> Set[String]("language","imageFile"),
                                "tag" -> Set[String]("id","name"),
                                "tagclass" -> Set[String]("id","name"),
                                "continent" -> Set[String]()
                                ) 

val abstractnodetypes = Map[String,Set[String]](
                            "organisation" -> Set[String]("id","name"),
                            "message" -> Set[String]("id","browserUsed","creationDate","locationIP","content","length"),
                            "place" -> Set[String]("id","name")
                           ) 

case class EdgeInfo( name : String, tailtype : String, headtype : String ) {
  override def toString : String = {
    s"$tailtype-$name-$headtype"
  }
}

val edgetypes = Map[EdgeInfo,Set[String]](
                EdgeInfo("containerOf","forum","post") -> Set(), 
                EdgeInfo("hasCreator","post","person") -> Set(), 
                EdgeInfo("hasCreator","comment","person") -> Set(), 
                EdgeInfo("hasInterest","person","tag") -> Set(), 
                EdgeInfo("hasMember","forum","person") -> Set("joinDate"), 
                EdgeInfo("hasModerator","forum","person") -> Set(), 
                EdgeInfo("hasTag","post","tag") -> Set(), 
                EdgeInfo("hasTag","comment","tag") -> Set(), 
                EdgeInfo("hasTag","forum","tag") -> Set(), 
                EdgeInfo("hasType","tag","tagClass") -> Set(), 
                EdgeInfo("isLocatedIn","company","country") -> Set(), 
                EdgeInfo("isLocatedIn","post","country") -> Set(), 
                EdgeInfo("isLocatedIn","comment","country") -> Set(), 
                EdgeInfo("isLocatedIn","person","city") -> Set(), 
                EdgeInfo("isLocatedIn","university","city") -> Set(), 
                EdgeInfo("isPartOf","city","country") -> Set(), 
                EdgeInfo("isPartOf","country","continent") -> Set(), 
                EdgeInfo("isSubclassOf","tagClass","tagClass") -> Set(), 
                EdgeInfo("knows","person","person") -> Set("creationDate"), 
                EdgeInfo("likes","person","post") -> Set("creationDate"), 
                EdgeInfo("likes","person","comment") -> Set(), 
                EdgeInfo("replyOf","comment","post") -> Set(), 
                EdgeInfo("replyOf","comment","comment") -> Set(), 
                EdgeInfo("studyAt","person","university") -> Set("classYear"), 
                EdgeInfo("workAt","person","company") -> Set("workFrom")
                )

val edgetypenames = edgetypes.keys.map({ case EdgeInfo(name,_,_) => name }).toSet

case class QuerySchema( nodetypes : mutable.HashMap[String,mutable.HashSet[String]], 
                        abstractnodetypes : mutable.HashMap[String,mutable.HashSet[String]],
                        edgetypes : mutable.HashMap[EdgeInfo,mutable.HashSet[String]]) {


                          override def toString() : String = {
                            val str = new StringBuilder()
                            str.append("Nodes: \n")
                            nodetypes.foreach( {case (node,attributes) => {
                              str.append(s"\t ${node.toString()}: ")
                              attributes.foreach(attr => str.append(s" $attr"))
                              str.append("\n")
                            }
                            })

                            str.append("AbstractNodes: \n")
                            abstractnodetypes.foreach( {case (node,attributes) => {
                              str.append(s"\t ${node.toString()}: ")
                              attributes.foreach(attr => str.append(s" $attr"))
                              str.append("\n")
                            }
                            })

                            str.append("Edges: \n")
                            edgetypes.foreach( {case (edge,attributes) => {
                              str.append(s"\t ${edge.toString()}: ")
                              attributes.foreach(attr => str.append(s" $attr"))
                              str.append("\n")
                            }
                            })
                            str.toString()
                        }
                        }


object Query extends Enumeration {
  type Query = Value
  val COMPLEX1 = Value("COMPLEX1") 
  val COMPLEX2 = Value("COMPLEX2") 
  val COMPLEX3 = Value("COMPLEX3")
  val COMPLEX4 = Value("COMPLEX4")
  val COMPLEX5 = Value("COMPLEX5")
  val COMPLEX6 = Value("COMPLEX6")
  val COMPLEX7 = Value("COMPLEX7")
  val COMPLEX8 = Value("COMPLEX8")
  val COMPLEX9 = Value("COMPLEX9")
  val COMPLEX10 = Value("COMPLEX10")
  val COMPLEX11 = Value("COMPLEX11")
  val COMPLEX12 = Value("COMPLEX12")
  val COMPLEX13 = Value("COMPLEX13")
  val COMPLEX14 = Value("COMPLEX14")
  val SHORT1 = Value("SHORT1")
  val SHORT2 = Value("SHORT2")
  val SHORT3 = Value("SHORT3")
  val SHORT4 = Value("SHORT4")
  val SHORT5 = Value("SHORT5")
  val SHORT6 = Value("SHORT6")
  val SHORT7 = Value("SHORT7")
  val UPDATE1 = Value("UPDATE1") 
  val UPDATE2 = Value("UPDATE2")
  val UPDATE3 = Value("UPDATE3")
  val UPDATE4 = Value("UPDATE4")
  val UPDATE5 = Value("UPDATE5")
  val UPDATE6 = Value("UPDATE6")
  val UPDATE7 = Value("UPDATE7")
  val UPDATE8 = Value("UPDATE8") 
  val NOQUERY = Value("NOQUERY")
}


def getQuery( line : String ) : Query.Query = {
  line match {
    case x if x.startsWith("QUERY1:") => Query.COMPLEX1
    case x if x.startsWith("QUERY2:") => Query.COMPLEX2
    case x if x.startsWith("QUERY3:") => Query.COMPLEX3
    case x if x.startsWith("QUERY4:") => Query.COMPLEX4
    case x if x.startsWith("QUERY5:") => Query.COMPLEX5
    case x if x.startsWith("QUERY6:") => Query.COMPLEX6
    case x if x.startsWith("QUERY7:") => Query.COMPLEX7
    case x if x.startsWith("QUERY8:") => Query.COMPLEX8
    case x if x.startsWith("QUERY9:") => Query.COMPLEX9
    case x if x.startsWith("QUERY10:") => Query.COMPLEX10
    case x if x.startsWith("QUERY11:") => Query.COMPLEX11
    case x if x.startsWith("QUERY12:") => Query.COMPLEX12
    case x if x.startsWith("QUERY13:") => Query.COMPLEX13
    case x if x.startsWith("QUERY14:") => Query.COMPLEX14
    /*case x if x.startsWith("UPDATE QUERY1:") => Query.UPDATE1
    case x if x.startsWith("UPDATE QUERY2:") => Query.UPDATE2
    case x if x.startsWith("UPDATE QUERY3:") => Query.UPDATE3
    case x if x.startsWith("UPDATE QUERY4:") => Query.UPDATE4
    case x if x.startsWith("UPDATE QUERY5:") => Query.UPDATE5
    case x if x.startsWith("UPDATE QUERY6:") => Query.UPDATE6
    case x if x.startsWith("UPDATE QUERY7:") => Query.UPDATE7
    case x if x.startsWith("UPDATE QUERY8:") => Query.UPDATE8*/
    case x if x.startsWith("SHORT QUERY1:") => Query.SHORT1
    case x if x.startsWith("SHORT QUERY2:") => Query.SHORT2
    case x if x.startsWith("SHORT QUERY3:") => Query.SHORT3
    case x if x.startsWith("SHORT QUERY4:") => Query.SHORT4
    case x if x.startsWith("SHORT QUERY5:") => Query.SHORT5
    case x if x.startsWith("SHORT QUERY6:") => Query.SHORT6
    case x if x.startsWith("SHORT QUERY7:") => Query.SHORT7
    case _ => Query.NOQUERY
  }
}

var currentquery : Query.Query = Query.NOQUERY
val queryschemas = new mutable.HashMap[Query.Query,QuerySchema]()

queryschemas.put(Query.COMPLEX1,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.COMPLEX2,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.COMPLEX3,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.COMPLEX4,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.COMPLEX5,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.COMPLEX6,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.COMPLEX7,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.COMPLEX8,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.COMPLEX9,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.COMPLEX10,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.COMPLEX11,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.COMPLEX12,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.COMPLEX13,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.COMPLEX14,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.SHORT1,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.SHORT2,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.SHORT3,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.SHORT4,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.SHORT5,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.SHORT6,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.SHORT7,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.UPDATE1,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.UPDATE2,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.UPDATE3,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.UPDATE4,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.UPDATE5,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.UPDATE6,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.UPDATE7,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))
queryschemas.put(Query.UPDATE8,new QuerySchema( new mutable.HashMap(), new mutable.HashMap(), new mutable.HashMap() ))

def isSparkseeEdgeType(str : String) : Boolean = {
  str match {
    case "postHasCreator" => true
    case "commentHasCreator" => true
    case "postHasTag" => true
    case "commentHasTag" => true
    case "hasMemberWithPosts" => true
    case _ => false
  }
}

def isEdgeType( str : String ) : Boolean = {
  if(str.contains("-")) {
    true
  } else if(edgetypenames.contains(str)) {
    true
  }  else if(isSparkseeEdgeType(str)) {
    true
  } else {
    false
  }
}

def extractEdgeInfo( etype : String ) : EdgeInfo = {
  val terms = etype.split("-")
  val (ename,etail,ehead) : (String,String,String) = terms.length match {
    case 1 => (etype,"","")
    case 2 => (terms(1),terms(0),"")
    case 3 => (terms(1),terms(0),terms(2))
  }
  new EdgeInfo(ename,etail,ehead)
}

def processTypes( line : String ) { 
  val types = line.split(":")(1).split(" ").filter( s => !s.isEmpty() )
  val querynodetypes = types.filter( t => !isEdgeType(t) && !abstractnodetypes.keySet.contains(t) )
  val queryabstractnodetypes = types.filter( t => abstractnodetypes.keySet.contains(t) )
  val queryedgetypes = types.filter( t => isEdgeType(t) )
  val queryschema : QuerySchema = queryschemas.get(currentquery).get

  for( t <- querynodetypes ) {
    if(!queryschema.nodetypes.keySet.contains(t)) {
      queryschema.nodetypes.put(t,new mutable.HashSet())
    }
  }

  for( t <- queryabstractnodetypes ) {
    if(!queryschema.abstractnodetypes.keySet.contains(t)) {
      queryschema.abstractnodetypes.put(t,new mutable.HashSet())
    }
  }

  for( t <- queryedgetypes ) {
    val edge = extractEdgeInfo(t)
    if(!queryschema.edgetypes.keySet.contains(edge)) {
      queryschema.edgetypes.put(edge,new mutable.HashSet())
    }
  }
}
 
def processAttributes( line : String ) {
  val attributes = line.split(" ").filter( s => s.contains("t:") ).map( s => s.split(":")(1) )
  val entityAttributePairs = attributes.map( s => {
    val entry =s.split(raw"\|") 
    (entry(0).replace("\\",""),entry(1))
  })
  val queryschema : QuerySchema = queryschemas.get(currentquery).get
  for(pair <- entityAttributePairs) {
    val (entity,attribute) = pair
    if(!isEdgeType(entity) && !abstractnodetypes.keySet.contains(entity)) {
      if(!queryschema.nodetypes.keySet.contains(entity)) {
        queryschema.nodetypes.put(entity,new mutable.HashSet())
      }
      queryschema.nodetypes.get(entity).get += attribute
    } else if(abstractnodetypes.keySet.contains(entity)) {
      if(!queryschema.abstractnodetypes.keySet.contains(entity)) {
        queryschema.abstractnodetypes.put(entity,new mutable.HashSet())
      }
      queryschema.abstractnodetypes.get(entity).get += attribute
    } else if(isEdgeType(entity)) {
      val edge = extractEdgeInfo(entity)
      if(!queryschema.edgetypes.keySet.contains(edge)) {
        queryschema.edgetypes.put(edge,new mutable.HashSet())
      }
      queryschema.edgetypes.get(edge).get += attribute
    }
  }
}

for(line <- Source.fromFile("/home/aprat/tmp/results/0001/test/execution_0001_1_1.server").getLines()) {
  line match {
    case x if x.startsWith("Types") && currentquery != Query.NOQUERY => processTypes(line)
    case x if x.startsWith("Attributes") && currentquery != Query.NOQUERY => processAttributes(line)
    case x if x.startsWith("EXIT") && currentquery != Query.NOQUERY => Unit
    case x if x.contains("QUERY") => currentquery = getQuery(line)
    case _ => Unit
  }
}


/********** TRANSFORMERS *************/

def transformLanguage( schema : QuerySchema ) : QuerySchema = {
  schema.nodetypes.remove("language")
  val removed = schema.edgetypes.remove(new EdgeInfo("speaks","person",""))
  if(!removed.isEmpty) {
    if(!schema.nodetypes.contains("person")) {
      schema.nodetypes.put("person", new mutable.HashSet[String])
    }
    schema.nodetypes.get("person").get.add("speaks")
  }
  schema
}

def transformEmail( schema : QuerySchema ) : QuerySchema = {
  schema.nodetypes.remove("emailaddress")
  val removed = schema.edgetypes.remove(new EdgeInfo("email","person",""))
  if(!removed.isEmpty) {
    if(!schema.nodetypes.contains("person")) {
      schema.nodetypes.put("person", new mutable.HashSet[String])
    }
    schema.nodetypes.get("person").get.add("email")
  }
  schema
}

def promote( ntype : String ) : String = {
  ntype match {
    case "post" => "message"
    case "comment" => "message"
    case "country" => "place"
    case "city" => "place"
    case "continent" => "place"
    case "university" => "organisation"
    case "company" => "organisation"
    case _ => ntype
  }
}

def transformPromoteTypes( schema : QuerySchema ) : QuerySchema = {
  schema.nodetypes.foreach({case (nodetype,attributes) => {
    val nodeAttributes = nodetypes.get(nodetype).get
    val toPromote = new mutable.HashSet[String]()
    val supertype = promote(nodetype)
    if(supertype != nodetype) {
      if(!schema.abstractnodetypes.contains(supertype)) {
        schema.abstractnodetypes.put(supertype,new mutable.HashSet[String]())
      }
      val supertypeAttributes = abstractnodetypes.get(supertype).get
      for(attribute <- attributes) {
        if(!nodeAttributes.contains(attribute) && supertypeAttributes.contains(attribute)) {
          toPromote.add(attribute)
        }
      }
      attributes --= toPromote
      schema.abstractnodetypes.get(supertype).get ++= toPromote
    }
  }
  })
  schema
}

def findEdge( edgeInfo : EdgeInfo ) : EdgeInfo = {
  var candidates = new mutable.HashSet[EdgeInfo]()
  for( edgetype <- edgetypes.keySet ) {
    if(edgetype.name == edgeInfo.name) {
      candidates.add(edgetype)
    }
  }
  if(candidates.size == 1) {
    candidates.toList(0)
  } else {
    candidates = candidates.filter( candidateedge => {candidateedge.tailtype == edgeInfo.tailtype || candidateedge.headtype == edgeInfo.tailtype })
    if(candidates.size == 1) {
      candidates.toList(0)
    } else {
      edgeInfo
    }
  }
}

def transformEdges(schema : QuerySchema) : QuerySchema = {
  val queryEdges = new mutable.HashMap[EdgeInfo,mutable.HashSet[String]]
  for ( (edge,attributes) <- schema.edgetypes ) {
    val candidateEdge = findEdge(edge)
    if(!queryEdges.contains(candidateEdge)) {
      queryEdges.put(candidateEdge,new mutable.HashSet[String]())
    }
    val edgeAttributes = queryEdges.get(candidateEdge).get
    edgeAttributes ++= attributes
  }
  schema.edgetypes.clear
  schema.edgetypes ++= queryEdges
  schema
}

def transformSparkseeEdges( schema : QuerySchema ) : QuerySchema = {
  val queryEdges = new mutable.HashMap[EdgeInfo,mutable.HashSet[String]]
  for ( (edge,attributes) <- schema.edgetypes ) {
    if(edge.name == "hasMemberWithPosts") {
      queryEdges.put(new EdgeInfo("hasMember","forum","person"),attributes)
    } else if(edge.name == "postHasCreator"){
      queryEdges.put(new EdgeInfo("hasCreator","post","person"),attributes)
    } else if(edge.name == "postHasTag"){
      queryEdges.put(new EdgeInfo("hasTag","post","tag"),attributes)
    } else if(edge.name == "commentHasTag"){
      queryEdges.put(new EdgeInfo("hasTag","comment","tag"),attributes)
    } else if(edge.name == "commentHasCreator"){
      queryEdges.put(new EdgeInfo("hasCreator","comment","person"),attributes)
    } else if(edge.name == "likes" && ((edge.tailtype == "post") || (edge.headtype == "post" )) ){
      queryEdges.put(new EdgeInfo("likes","person","post"),attributes)
    } else if(edge.name == "likes" && ((edge.tailtype == "comment") || (edge.headtype == "comment")) ){
      queryEdges.put(new EdgeInfo("likes","person","comment"),attributes)
    } else {
      queryEdges.put(edge,attributes)
    }
  }
  schema.edgetypes.clear
  schema.edgetypes ++= queryEdges
  schema
}

val workloadnodetypes = new mutable.HashMap[String,mutable.HashSet[String]]()
val workloadabstractnodetypes = new mutable.HashMap[String,mutable.HashSet[String]]()
val workloadedgetypes = new mutable.HashMap[EdgeInfo,mutable.HashSet[String]]()
val warningsworkload = new mutable.HashSet[String]()

def addNodeAttributesToWorkload( query : Query.Query, attributes : Set[String], workloadattributes : mutable.HashSet[String], ntype : String, queryattributes : mutable.HashSet[String] ) = {
  for( attribute <- queryattributes ) {
    if(attributes.contains(attribute)) {
      workloadattributes.add(attribute)
    } else {
      warningsworkload.add(s"On query $query Attribute $attribute for node $ntype does not exist") 
    }
  }
}

def addNodeTypeToWorkload( query : Query.Query, nodetypes : Map[String,Set[String]], workloadnodetypes : mutable.HashMap[String,mutable.HashSet[String]], ntype : String, attributes : mutable.HashSet[String] ) : Unit = {
  if(nodetypes.keySet.contains(ntype)) {
    if(!workloadnodetypes.keySet.contains(ntype)) {
      workloadnodetypes.put(ntype,new mutable.HashSet[String]())
    }
    addNodeAttributesToWorkload(query, nodetypes.get(ntype).get,workloadnodetypes.get(ntype).get,ntype,attributes)
  } else {
    warningsworkload.add(s"On query $query Nodetype $ntype does not exist")
  } 
}

def addEdgeTypeToWorkload( query : Query.Query, edgetypes : Map[EdgeInfo,Set[String]], workloadedgetypes : mutable.HashMap[EdgeInfo,mutable.HashSet[String]], edgetype : EdgeInfo, attributes : mutable.HashSet[String] ) : Unit = {
  if(edgetypes.keySet.contains(edgetype)) {
    if(!workloadedgetypes.keySet.contains(edgetype)) {
      workloadedgetypes.put(edgetype,new mutable.HashSet[String]())
    }
    addNodeAttributesToWorkload(query,edgetypes.get(edgetype).get,workloadedgetypes.get(edgetype).get,edgetype.toString,attributes)
  } else {
    warningsworkload.add(s"On query $query EdgeType $edgetype does not exist")
  } 
}

queryschemas.foreach( { case (query,schema) => {
  transformLanguage(schema)
  transformEmail(schema)
  transformPromoteTypes(schema)
  transformEdges(schema)
  transformSparkseeEdges(schema)
}
})

for((query, schema) <- queryschemas) {
  for((nodetype,attributes) <- schema.nodetypes) {
    addNodeTypeToWorkload(query, nodetypes,workloadnodetypes,nodetype,attributes)
  }
  for((nodetype,attributes) <- schema.abstractnodetypes) {
    addNodeTypeToWorkload(query, abstractnodetypes, workloadabstractnodetypes,nodetype,attributes)
  }
  for((edgetype,attributes) <- schema.edgetypes) {
    addEdgeTypeToWorkload(query, edgetypes, workloadedgetypes,edgetype,attributes)
  }
}

val workloadschema = new QuerySchema(workloadnodetypes,workloadabstractnodetypes,workloadedgetypes)

val warningsschema = new mutable.HashSet[String]()
for( (nodetype,attributes) <- nodetypes) {
  if(!workloadschema.nodetypes.contains(nodetype)) {
    warningsschema.add("Could not find Node Type: "+nodetype+" in the workload")
  } else {
    val workloadattributes = workloadschema.nodetypes.get(nodetype).get
    for(attribute <- attributes) {
      if(!workloadattributes.contains(attribute)) {
        warningsschema.add("Could not find Attribute "+attribute+" of Node Type: "+nodetype+" in the workload")
      }
    }
  }
}

for( (edgetype,attributes) <- edgetypes) {
  if(!workloadschema.edgetypes.contains(edgetype)) {
    warningsschema.add("Could not find Edge Type: "+edgetype+" in the workload")
  } else {
    val workloadattributes = workloadschema.edgetypes.get(edgetype).get
    for(attribute <- attributes) {
      if(!workloadattributes.contains(attribute)) {
        warningsschema.add("Could not find Attribute "+attribute+" of Edge Type: "+edgetype+" in the workload")
      }
    }
  }
}

queryschemas.foreach( { case (query,schema) => {println(query);println(schema)}})
println("WORKLOAD")
println(workloadschema.toString)
println("WARNINGS WORKLOAD")
warningsworkload.foreach( msg => println("WARNING: "+msg))
println("WARNINGS SCHEMA")
warningsschema.foreach( msg => println("WARNING: "+msg))
