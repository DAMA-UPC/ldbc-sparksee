package com.sparsity.tojson;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import com.sparsity.sparksee.gdb.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;

class ToJson {
    public static class Arguments {
        @Parameter(names = {"-i","--input"}, description = "Path to the database file", required = true)
        public String filename;

        @Parameter(names = {"-b","--begin"}, description = "The begin date dd/mm/yy", required = true)
        public String beginDate;

        @Parameter(names = {"-e","--end"}, description = "The end date dd/mm/yy", required = true)
        public String endDate;
    }


    public static class LDBCTypes {

        public int personType;
        public int personIdType;
        public int personFirstNameType;
        public int personLastNameType;

        public int placeType;
        public int placeNameType;

        public int isLocatedInType;
        public int isPartOfType;

        public int postType;
        public int postIdType;
        public int postCreationDateType;
        public int postImageFileType;

        public int postHasCreator;

        public int commentType;
        public int commentIdType;
        public int commentCreationDateType;

        public int commentHasCreator;

        public int replyOfType;

        public int tagType;
        public int tagNameType;

        public int hasTagType;

        public int likesType;
        public int likesCreationDateType;


        public LDBCTypes(Graph graph) {
            this.personType = graph.findType("person");
            this.personIdType = graph.findAttribute(personType, "id");
            this.personFirstNameType = graph.findAttribute(personType, "firstName");
            this.personLastNameType = graph.findAttribute(personType, "lastName");

            this.isLocatedInType = graph.findType("isLocatedIn");
            this.isPartOfType = graph.findType("isPartOf");

            this.placeType = graph.findType("place");
            this.placeNameType = graph.findAttribute(placeType,"name");

            this.postType = graph.findType("post");
            this.postIdType = graph.findAttribute(postType, "id");
            this.postCreationDateType = graph.findAttribute(postType,"creationDate");
            this.postImageFileType = graph.findAttribute(postType,"imageFile");

            this.postHasCreator = graph.findType("postHasCreator");

            this.commentType = graph.findType("comment");
            this.commentIdType = graph.findAttribute(commentType, "id");
            this.commentCreationDateType = graph.findAttribute(commentType,"creationDate");

            this.commentHasCreator = graph.findType("commentHasCreator");

            this.replyOfType = graph.findType("replyOf");

            this.tagType = graph.findType("tag");
            this.tagNameType = graph.findAttribute(tagType,"name");

            this.hasTagType = graph.findType("hasTag");

            this.likesType = graph.findType("likes");
            this.likesCreationDateType = graph.findAttribute(likesType,"creationDate");

        }

    }

    public static Person getPerson(Graph graph, LDBCTypes ldbc, long person) {

        Value value = new Value();
        graph.getAttribute(person,ldbc.personIdType,value);
        long idPerson = value.getLong();
        graph.getAttribute(person,ldbc.personFirstNameType,value);
        String firstName = value.getString();
        graph.getAttribute(person,ldbc.personLastNameType,value);
        String lastName = value.getString();

        Objects places = graph.neighbors(person,ldbc.isLocatedInType,EdgesDirection.Outgoing);
        long city = places.any();
        places.close();
        graph.getAttribute(city,ldbc.placeNameType,value);
        String cityName = value.getString();

        Objects countries = graph.neighbors(city,ldbc.isPartOfType,EdgesDirection.Outgoing);
        long country = countries.any();
        countries.close();
        graph.getAttribute(country,ldbc.placeNameType,value);
        String countryName = value.getString();

        return new Person(idPerson, countryName, cityName, firstName, lastName);
    }

    public static Document getPost(Graph graph, LDBCTypes ldbc, long document) {


        Value value = new Value();
        graph.getAttribute(document, ldbc.postIdType, value);
        long postId = value.getLong();
        graph.getAttribute(document,ldbc.postCreationDateType,value);
        long creationDate = value.getTimestamp();
        Objects tags = graph.neighbors(document,ldbc.hasTagType,EdgesDirection.Outgoing);
        String [] tagArray = new String[(int)tags.count()];
        int index = 0;
        for(long tag : tags) {
            graph.getAttribute(tag,ldbc.tagNameType,value);
            String tagName = value.getString();
            tagArray[index] = tagName;
            index++;
        }
        tags.close();

        Objects countries = graph.neighbors(document,ldbc.isLocatedInType, EdgesDirection.Outgoing);
        long country = countries.any();
        graph.getAttribute(country,ldbc.placeNameType, value);
        String countryName = value.getString();
        countries.close();

        Objects creators = graph.neighbors(document,ldbc.postHasCreator, EdgesDirection.Outgoing);
        Person creator = getPerson(graph, ldbc, creators.any());
        creators.close();

        return new Document(postId, creationDate, null, Document.Type.post, tagArray, creator, countryName);


    }

    public static Document getComment(Graph graph, LDBCTypes ldbc, long document, boolean outputParent) {

        Value value = new Value();
        graph.getAttribute(document, ldbc.commentIdType, value);
        long commentId = value.getLong();
        graph.getAttribute(document,ldbc.commentCreationDateType,value);
        long creationDate = value.getTimestamp();
        Objects tags = graph.neighbors(document,ldbc.hasTagType,EdgesDirection.Outgoing);
        String [] tagArray = new String[(int)tags.count()];
        int index = 0;
        for(long tag : tags) {
            graph.getAttribute(tag,ldbc.tagNameType,value);
            String tagName = value.getString();
            tagArray[index] = tagName;
            index++;
        }
        tags.close();
        Objects countries = graph.neighbors(document,ldbc.isLocatedInType, EdgesDirection.Outgoing);
        long country = countries.any();
        graph.getAttribute(country,ldbc.placeNameType, value);
        String countryName = value.getString();
        countries.close();

        Objects creators = graph.neighbors(document,ldbc.commentHasCreator, EdgesDirection.Outgoing);
        Person creator = getPerson(graph, ldbc, creators.any());
        creators.close();

        Document parent = null;
        if(outputParent) {
            Objects replied = graph.neighbors(document, ldbc.replyOfType, EdgesDirection.Outgoing);
            long parentId = replied.any();
            replied.close();
            if(graph.getObjectType(parentId) == ldbc.postType) {
                parent = getPost(graph, ldbc, parentId);
            } else {
                parent = getComment(graph, ldbc, parentId, false);
            }
        }
        return new Document(commentId, creationDate, parent, Document.Type.comment, tagArray, creator, countryName);
    }

    public static Document getLikes(Graph graph, LDBCTypes ldbc, long document) {

        Value value = new Value();
        graph.getAttribute(document,ldbc.likesCreationDateType,value);
        long creationDate = value.getTimestamp();

        Person creator = getPerson(graph, ldbc, graph.getEdgeData(document).getTail());
        long messageId = graph.getEdgeData(document).getHead();
        Document parent = null;
        if(graph.getObjectType(messageId) == ldbc.postType) {
            parent = getPost(graph,ldbc,messageId);
        } else {
            parent = getComment(graph,ldbc,messageId,false);
        }

        return new Document(-1, creationDate, parent, Document.Type.like, null, creator, creator.country);
    }


    public static void main(String[] argv) {

        Arguments arguments = new Arguments();
        new JCommander(arguments,argv);

        LocalDateTime beginDate = LocalDateTime.of(2000+Integer.parseInt(arguments.beginDate.substring(6,8)),
                Integer.parseInt(arguments.beginDate.substring(3,5)),
                Integer.parseInt(arguments.beginDate.substring(0,2)), 0, 0
                );

        LocalDateTime endDate = LocalDateTime.of(2000+Integer.parseInt(arguments.endDate.substring(6,8)),
                Integer.parseInt(arguments.endDate.substring(3,5)),
                Integer.parseInt(arguments.endDate.substring(0,2)), 0, 0
        );

        SparkseeConfig sparkseeConfig = new SparkseeConfig();
        sparkseeConfig.setLicense("RNZ53-GHPS9-6WZE1-KWP18");
        Sparksee sparksee = new Sparksee(sparkseeConfig);
        Database database = null;
        try {
            database = sparksee.open(arguments.filename,false);
        } catch(FileNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        Session session = database.newSession();
        Graph graph = session.getGraph();

        Gson gson = new Gson();
        LDBCTypes ldbc = new LDBCTypes(graph);

        Value beginValue = new Value();
        long begin = beginDate.toEpochSecond(ZoneOffset.ofHours(0))*1000;
        beginValue.setTimestamp(begin);
        Value endValue = new Value();
        long end = endDate.toEpochSecond(ZoneOffset.ofHours(0))*1000;
        endValue.setTimestamp(end);
        Objects posts = graph.select(ldbc.postCreationDateType, Condition.Between, beginValue, endValue);
        try {
            Value value = new Value();
            FileWriter postsFile = new FileWriter(new File("./posts.json"));
            ObjectsIterator it = posts.iterator();
            while(it.hasNext()) {
                long post = it.next();
                graph.getAttribute(post,ldbc.postImageFileType,value);
                if(value.isNull()) {
                    Document document = getPost(graph, ldbc, post);
                    String json = gson.toJson(document) + "\n";
                    postsFile.write(json);
                }
            }
            it.close();
            postsFile.close();
        }catch ( IOException e ) {
            e.printStackTrace();
            System.exit(-1);
        }
        posts.close();

        Objects comments = graph.select(ldbc.commentCreationDateType, Condition.Between, beginValue, endValue);
        try {
            FileWriter commentsFile = new FileWriter(new File("./comments.json"));
            for(Long comment : comments) {
                commentsFile.write(gson.toJson(getComment(graph,ldbc,comment, true))+"\n");
            }
            commentsFile.close();
        }catch ( IOException e ) {
            e.printStackTrace();
            System.exit(-1);
        }
        comments.close();

        Objects likes = graph.select(ldbc.likesCreationDateType, Condition.Between, beginValue, endValue);
        try {
            FileWriter likesFile = new FileWriter(new File("./likes.json"));
            for(Long like : likes) {
                likesFile.write(gson.toJson(getLikes(graph,ldbc,like))+"\n");
            }
            likesFile.close();
        }catch ( IOException e ) {
            e.printStackTrace();
            System.exit(-1);
        }
        likes.close();


        session.close();
        database.close();
        return;
    }
}