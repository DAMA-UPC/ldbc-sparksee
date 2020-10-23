package com.ldbc.driver.sparksee.workloads.ldbc.snb.interactive.db;



import com.ldbc.driver.*;
//import com.ldbc.driver.workloads.ldbc.snb.bi.*;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.List;
import java.util.ArrayList;

public class LDBCppQueryTranslator {
    
    private static final String ALL_MATCH  = "\"([^\"]+)\""; //Match all except "
    private static final String LIST_MATCH = "(\\[[^\\]]+\\]|\"\")"; //Match all except ] or an empty list

    public static String translate(LdbcQuery1 query ) {
        return String.format("{ \"query%d\" : [{\"id\" : \"%d\", \"name\": \"%s\", \"limit\": \"%d\"}] }", 1, query.personId(), query.firstName(), query.limit());
    }

    public static ArrayList<LdbcQuery1Result> translateToQuery1(String json) {
        ArrayList<LdbcQuery1Result> result = new ArrayList<LdbcQuery1Result>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":" + ALL_MATCH + ",\"lastName\":" + ALL_MATCH + 
            ",\"birthday\":" + ALL_MATCH + ",\"creationDate\":"+ ALL_MATCH + ",\"gender\":" + ALL_MATCH +
            ",\"browserUsed\":"+ ALL_MATCH + ",\"locationIP\":" + ALL_MATCH + ",\"emails\":" + LIST_MATCH +
            ",\"languages\":" + LIST_MATCH + ",\"Place\":\\{\"name\":" + ALL_MATCH + "\\},\"studyAt\":" + LIST_MATCH +
            ",\"workAt\":" + LIST_MATCH +"\\},\"distance\":" + ALL_MATCH + "\\}");
        Pattern emailPattern = Pattern.compile(ALL_MATCH);
        Pattern languagePattern = Pattern.compile(ALL_MATCH);
        Pattern studyPattern = Pattern.compile("\\{\"University\":\\{\"name\":" + ALL_MATCH + "\\}" +
            ",\"classYear\":" + ALL_MATCH + ",\"City\":\\{\"name\":" + ALL_MATCH +"\\}\\}");
        Pattern workPattern = Pattern.compile("\\{\"Company\":\\{\"name\":" + ALL_MATCH + "\\}" +
            ",\"workFrom\":" + ALL_MATCH + ",\"Country\":\\{\"name\":" + ALL_MATCH +"\\}\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            ArrayList<String> emails = new ArrayList<String>();
            ArrayList<String> languages = new ArrayList<String>();
            ArrayList<List<Object>> universities = new ArrayList<List<Object>>();
            List<List<Object>> companies = new ArrayList<List<Object>>();
            Long personId = new Long(m.group(1));
            String lastName = m.group(2);
            Long birthday = new Long(m.group(3));
            Long creationDate = new Long(m.group(4));
            String gender = m.group(5);
            String browser = m.group(6);
            String ip = m.group(7);
            Matcher emailMatcher = emailPattern.matcher(m.group(8));
            Matcher languageMatcher = languagePattern.matcher(m.group(9));
            String city = m.group(10);
            Matcher studyMatcher = studyPattern.matcher(m.group(11));
            Matcher workMatcher = workPattern.matcher(m.group(12));
            Integer distance = new Integer(m.group(13));
            while (emailMatcher.find()) {
                emails.add(emailMatcher.group(1));
            }
            while (languageMatcher.find()) {
                languages.add(languageMatcher.group(1));
            }
            while (studyMatcher.find()) {
                List<Object> studyAt = new ArrayList<Object>();
                studyAt.add(studyMatcher.group(1));
                studyAt.add(studyMatcher.group(2));
                studyAt.add(studyMatcher.group(3));
                universities.add(studyAt);
            }
            while (workMatcher.find()) {
                List<Object> workAt = new ArrayList<Object>();
                workAt.add(workMatcher.group(1));
                workAt.add(workMatcher.group(2));
                workAt.add(workMatcher.group(3));
                companies.add(workAt);
            }           
            result.add(new LdbcQuery1Result(personId, lastName, distance, birthday, creationDate, gender, browser, ip,
                  emails, languages, city, universities, companies));
        }
        return result;
    }

    public static String translate(LdbcQuery2 query ) {
        return String.format("{ \"query%d\" : [{\"id\" : \"%d\", \"date\": \"%s\", \"limit\": \"%d\"}] }", 2, query.personId(), query.maxDate().getTime(), query.limit());
    }

    public static ArrayList<LdbcQuery2Result> translateToQuery2(String json) {
        ArrayList<LdbcQuery2Result> result = new ArrayList<LdbcQuery2Result>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":" + ALL_MATCH + ",\"firstName\":" + ALL_MATCH + 
            ",\"lastName\":" + ALL_MATCH + "\\},\"Message\":\\{\"id\":" + ALL_MATCH + ",\"content\":" + ALL_MATCH +
            ",\"creationDate\":" + ALL_MATCH + "\\}\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long creatorId = new Long(m.group(1));
            String firstName = m.group(2);
            String lastName = m.group(3);
            Long commentId = new Long(m.group(4));
            String content = m.group(5);
            Long creationDate = new Long(m.group(6));
            result.add(new LdbcQuery2Result(creatorId, firstName, lastName, commentId, content, creationDate)); 
        }
        return result;
    }

    public static String translate(LdbcQuery3 query ) {
        return String.format("{ \"query%d\" : [{\"id\" : \"%d\", \"country1\": \"%s\", \"country2\": \"%s\", \"date\": \"%d\", \"duration\": \"%d\", \"limit\": \"%d\"}] }", 3, query.personId(), query.countryXName(), query.countryYName(),  query.startDate().getTime(), query.durationDays(), query.limit());
    }

    public static ArrayList<LdbcQuery3Result> translateToQuery3(String json) {
        ArrayList<LdbcQuery3Result> result = new ArrayList<LdbcQuery3Result>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":" + ALL_MATCH + 
            ",\"firstName\":"+ ALL_MATCH + ",\"lastName\":" + ALL_MATCH + "\\}" +
            ",\"countx\":" + ALL_MATCH + ",\"county\":" + ALL_MATCH + 
            ",\"count\":" + ALL_MATCH + "\\}"); 
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long personId = new Long(m.group(1));
            String firstName = m.group(2);
            String lastName = m.group(3);
            Integer country1 = new Integer(m.group(4));
            Integer country2 = new Integer(m.group(5));
            Integer total = new Integer(m.group(6));
            result.add(new LdbcQuery3Result(personId, firstName, lastName, country1, country2, total));
        }
        return result;
    }


    public static String translate(LdbcQuery4 query ) {
        return String.format("{ \"query%d\" : [{\"id\" : \"%d\", \"date\": \"%s\", \"days\": \"%d\", \"limit\": \"%d\"}] }", 4, query.personId(), query.startDate().getTime(), query.durationDays(), query.limit());
    }

    public static ArrayList<LdbcQuery4Result> translateToQuery4(String json) {
        ArrayList<LdbcQuery4Result> result = new ArrayList<LdbcQuery4Result>();
        Pattern pattern = Pattern.compile("\\{\"Tag\":\\{\"name\":" + ALL_MATCH + "\\},\"count\":" + ALL_MATCH + "\\}"); 
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            String name = m.group(1);
            Integer count = new Integer(m.group(2));
            result.add(new LdbcQuery4Result(name, count));
        }
        return result;
    }


    public static String translate(LdbcQuery5 query ) {
        return String.format("{ \"query%d\" : [{\"id\" : \"%d\", \"date\": \"%s\", \"limit\": \"%d\"}] }", 5, query.personId(), query.minDate().getTime(), query.limit());
    }

    public static ArrayList<LdbcQuery5Result> translateToQuery5(String json) {
        ArrayList<LdbcQuery5Result> result = new ArrayList<LdbcQuery5Result>();
        Pattern pattern = Pattern.compile("\\{\"Forum\":\\{\"title\":" + ALL_MATCH + "\\},\"count\":" + ALL_MATCH + "\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            String title = m.group(1);
            Integer count = new Integer(m.group(2));
            result.add(new LdbcQuery5Result(title, count)); 
        }
        return result;
    }

    public static String translate(LdbcQuery6 query ) {
        return String.format("{ \"query%d\" : [{\"id\" : \"%d\", \"tag\": \"%s\", \"limit\": \"%d\"}] }", 6, query.personId(), query.tagName(), query.limit());
    }

    public static ArrayList<LdbcQuery6Result> translateToQuery6(String json) {
        ArrayList<LdbcQuery6Result> result = new ArrayList<LdbcQuery6Result>();
        Pattern pattern = Pattern.compile("\\{\"Tag\":\\{\"name\":" + ALL_MATCH + "\\},\"count\":" + ALL_MATCH + "\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            String name = m.group(1);
            Integer count = new Integer(m.group(2));
            result.add(new LdbcQuery6Result(name, count)); 
        }
        return result;
    }

    public static String translate(LdbcQuery7 query ) {
        return String.format("{ \"query%d\" : [{\"id\" : \"%d\", \"limit\": \"%d\"}] }", 7, query.personId(), query.limit());
    }

    public static ArrayList<LdbcQuery7Result> translateToQuery7(String json) {
        ArrayList<LdbcQuery7Result> result = new ArrayList<LdbcQuery7Result>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":" + ALL_MATCH + ",\"firstName\":" + ALL_MATCH + 
            ",\"lastName\":" + ALL_MATCH + "\\},\"Like\":\\{\"creationDate\":" + ALL_MATCH + 
            "\\},\"Message\":\\{\"id\":" + ALL_MATCH + ",\"content\":" + ALL_MATCH + "\\},\"latency\":" + ALL_MATCH +
            ",\"isNew\":" + ALL_MATCH + "\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long creatorId = new Long(m.group(1));
            String firstName = m.group(2);
            String lastName = m.group(3);
            Long creationDate = new Long(m.group(4));
            Long commentId = new Long(m.group(5));
            String content = m.group(6);
            Integer latency = new Integer(m.group(7));
            Boolean isNew = new Boolean(m.group(8));
            result.add(new LdbcQuery7Result(creatorId, firstName, lastName, creationDate, commentId, content, latency, isNew)); 
        }
        return result;
    }

    public static String translate(LdbcQuery8 query ) {
        return String.format("{ \"query%d\" : [{\"id\" : \"%d\", \"limit\": \"%d\"}] }", 8, query.personId(), query.limit());
    }

    public static ArrayList<LdbcQuery8Result> translateToQuery8(String json) {
        ArrayList<LdbcQuery8Result> result = new ArrayList<LdbcQuery8Result>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":" + ALL_MATCH + ",\"firstName\":" + ALL_MATCH + 
            ",\"lastName\":" + ALL_MATCH + "\\},\"Comment\":\\{\"creationDate\":" + ALL_MATCH + ",\"id\":" + ALL_MATCH +
            ",\"content\":" + ALL_MATCH + "\\}\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long creatorId = new Long(m.group(1));
            String firstName = m.group(2);
            String lastName = m.group(3);
            Long creationDate = new Long(m.group(4));
            Long commentId = new Long(m.group(5));
            String content = m.group(6);
            result.add(new LdbcQuery8Result(creatorId, firstName, lastName, creationDate, commentId, content)); 
        }
        return result;
    }

   
    public static String translate(LdbcQuery9 query ) {
        return String.format("{ \"query%d\" : [{\"id\" : \"%d\", \"date\": \"%s\", \"limit\": \"%d\"}] }", 9, query.personId(), query.maxDate().getTime(), query.limit());
    }

    public static ArrayList<LdbcQuery9Result> translateToQuery9(String json) {
        ArrayList<LdbcQuery9Result> result = new ArrayList<LdbcQuery9Result>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":" + ALL_MATCH + ",\"firstName\":" + ALL_MATCH + 
            ",\"lastName\":" + ALL_MATCH + "\\},\"Message\":\\{\"id\":" + ALL_MATCH + ",\"content\":" + ALL_MATCH +
            ",\"creationDate\":" + ALL_MATCH + "\\}\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long creatorId = new Long(m.group(1));
            String firstName = m.group(2);
            String lastName = m.group(3);
            Long commentId = new Long(m.group(4));
            String content = m.group(5);
            Long creationDate = new Long(m.group(6));
            result.add(new LdbcQuery9Result(creatorId, firstName, lastName, commentId, content, creationDate)); 
        }
        return result;
    }
     
    public static String translate(LdbcQuery10 query ) {
        return String.format("{ \"query%d\" : [{\"id\" : \"%d\", \"month\": \"%d\", \"limit\": \"%d\"}] }", 10, query.personId(), query.month(), query.limit());
    }

    public static ArrayList<LdbcQuery10Result> translateToQuery10(String json) {
        ArrayList<LdbcQuery10Result> result = new ArrayList<LdbcQuery10Result>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":" + ALL_MATCH + 
            ",\"firstName\":"+ ALL_MATCH + ",\"lastName\":" + ALL_MATCH + 
            ",\"gender\":"+ ALL_MATCH + "\\},\"similarity\":" + ALL_MATCH + 
            ",\"Place\":\\{\"name\":" + ALL_MATCH + "\\}\\}"); 
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long personId = new Long(m.group(1));
            String firstName = m.group(2);
            String lastName = m.group(3);
            String gender = m.group(4);
            Integer score = new Integer(m.group(5));
            String place = m.group(6);
            result.add(new LdbcQuery10Result(personId, firstName, lastName, score, gender, place));
        }
        return result;
    }


    public static String translate(LdbcQuery11 query ) {
        return String.format("{ \"query%d\" : [{\"id\" : \"%d\", \"country\": \"%s\", \"year\": \"%d\",\"limit\": \"%d\"}] }", 11, query.personId(), query.countryName(), query.workFromYear(), query.limit());
    }

    public static ArrayList<LdbcQuery11Result> translateToQuery11(String json) {
        ArrayList<LdbcQuery11Result> result = new ArrayList<LdbcQuery11Result>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":" + ALL_MATCH + 
            ",\"firstName\":"+ ALL_MATCH + ",\"lastName\":" + ALL_MATCH + "\\}" +
            ",\"worksAt\":\\{\"name\":"+ ALL_MATCH + ",\"worksFrom\":" + ALL_MATCH + "\\}\\}"); 
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long personId = new Long(m.group(1));
            String firstName = m.group(2);
            String lastName = m.group(3);
            String organization = m.group(4);
            Integer year = new Integer(m.group(5));
            result.add(new LdbcQuery11Result(personId, firstName, lastName, organization, year));
        }
        return result;
    }


    public static String translate(LdbcQuery12 query ) {
        return String.format("{ \"query%d\" : [{\"id\" : \"%d\", \"tagclass\": \"%s\", \"limit\": \"%d\"}] }", 12, query.personId(), query.tagClassName(), query.limit());
    }

    public static ArrayList<LdbcQuery12Result> translateToQuery12(String json) {
        ArrayList<LdbcQuery12Result> result = new ArrayList<LdbcQuery12Result>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":" + ALL_MATCH + 
            ",\"firstName\":"+ ALL_MATCH + ",\"lastName\":" + ALL_MATCH + "\\}" +
            ",\"Tags\":" + LIST_MATCH + ",\"count\":" + ALL_MATCH + "\\}");
        Pattern tagsPattern = Pattern.compile(ALL_MATCH);
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            ArrayList<String> tags = new ArrayList<String>();
            Long personId = new Long(m.group(1));
            String firstName = m.group(2);
            String lastName = m.group(3);
            Matcher tagMatcher = tagsPattern.matcher(m.group(4)); 
            Integer count = new Integer(m.group(5)); 
            while (tagMatcher.find()) {
                tags.add(tagMatcher.group(1));
            }
            result.add(new LdbcQuery12Result(personId, firstName, lastName, tags, count)); 
        }
        return result;
    }


    public static String translate(LdbcQuery13 query ) {
        return String.format("{ \"query%d\" : [{\"id1\" : \"%d\", \"id2\" : \"%d\"}] }", 13, query.person1Id(),query.person2Id());
    }

    public static LdbcQuery13Result translateToQuery13(String json) {
        LdbcQuery13Result result = null;
        Pattern pattern = Pattern.compile("\\{\"length\":" + ALL_MATCH + "\\}"); 
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Integer length = new Integer(m.group(1));
            result = new LdbcQuery13Result(length);
        }
        return result;
    }

    public static String translate(LdbcQuery14 query ) {
        return String.format("{ \"query%d\" : [{\"id1\" : \"%d\", \"id2\" : \"%d\"}] }", 14, query.person1Id(),query.person2Id());
    }

    public static ArrayList<LdbcQuery14Result> translateToQuery14(String json) {
        ArrayList<LdbcQuery14Result> result = new ArrayList<LdbcQuery14Result>();
        Pattern pattern = Pattern.compile("\\{\"path\":" + LIST_MATCH + ",\"weight\":" + ALL_MATCH + "\\}");
        Pattern pathPattern = Pattern.compile(ALL_MATCH);
        Matcher m = pattern.matcher(json);
        while (m.find()) {
           ArrayList<Long> path = new ArrayList<Long>();
           Matcher pathMatch = pathPattern.matcher(m.group(1));
           Double weight = new Double(m.group(2));
           while (pathMatch.find()) {
               path.add(new Long(pathMatch.group(1)));
           }  
           result.add(new LdbcQuery14Result(path, weight));
        }
        return result;
    }
    
    public static String translate(LdbcUpdate1AddPerson query) {
        String result = String.format("{ \"update%d\" : [{\"id\" : \"%d\", \"firstName\" : \"%s\", \"lastName\" : \"%s\"" +
            ",\"gender\" : \"%s\", \"birthday\" : \"%d\", \"date\" : \"%d\", \"ip\" : \"%s\", \"browser\" : \"%s\"" +
            ",\"cityId\" : \"%d\", \"numEmails\" : \"%d\", \"emails\" : ", 1, query.personId(), query.personFirstName(),
            query.personLastName(), query.gender(), query.birthday().getTime(), query.creationDate().getTime(), 
            query.locationIp(), query.browserUsed(), query.cityId(), query.emails().size());
        if (query.emails().size() == 0) {
            result += "\"\"";
        } else {
            result += ("[\"" + query.emails().get(0) + "\"");
            for (int i = 1; i < query.emails().size(); ++i) {
                result += (", \"" + query.emails().get(i) + "\"");
            }
            result += "]";
        }
        result += String.format(", \"numLanguages\" : \"%d\", \"languages\" : ", query.languages().size());
        if (query.languages().size() == 0) {
            result += "\"\"";
        } else {
            result += ("[\"" + query.languages().get(0) + "\"");
            for (int i = 1; i < query.languages().size(); ++i) {
                result += (", \"" + query.languages().get(i) + "\"");
            }
            result += "]";
        }
        result += String.format(", \"numInterests\" : \"%d\", \"interests\" : ", query.tagIds().size());
        if (query.tagIds().size() == 0) {
            result += "\"\"";
        } else {
            result += ("[\"" + query.tagIds().get(0) + "\"");
            for (int i = 1; i < query.tagIds().size(); ++i) {
                result += (", \"" + query.tagIds().get(i) + "\"");
            }
            result += "]";
        }
        result += String.format(", \"numWorkAt\" : \"%d\", \"workAt\" : ", query.workAt().size());
        if (query.workAt().size() == 0) {
            result += "\"\"";
        } else {
            result += ("[{\"Company\":{\"id\":\"" + query.workAt().get(0).organizationId() + "\", \"year\":\""+ query.workAt().get(0).year()  +"\"}}");
            for (int i = 1; i < query.workAt().size(); ++i) {
                result += (",{\"Company\":{\"id\":" + query.workAt().get(i).organizationId() + ", \"year\":\""+ query.workAt().get(i).year()  +"\"}}");
            }
            result += "]";
        }
        result += String.format(", \"numStudyAt\" : \"%d\", \"studyAt\" : ", query.studyAt().size());
        if (query.studyAt().size() == 0) {
            result += "\"\"";
        } else {
            result += ("[{\"University\": {\"id\":\"" + query.studyAt().get(0).organizationId() + "\", \"year\":\""+ query.studyAt().get(0).year()  +"\"}}");
            for (int i = 1; i < query.studyAt().size(); ++i) {
                result += (",{\"University\": {\"id\":" + query.studyAt().get(i).organizationId() + ", \"year\":\""+ query.studyAt().get(i).year()  +"\"}}");
            }
            result += "]";
        }
        result += "}]}";
        return result;
    }

    public static String translate(LdbcUpdate2AddPostLike query) {
        return String.format("{ \"update%d\" : [{\"id\" : \"%d\", \"postId\": \"%d\", \"date\": \"%d\"}] }", 2, query.personId(), query.postId(), query.creationDate().getTime());
    }
    public static String translate(LdbcUpdate3AddCommentLike query) {
        return String.format("{ \"update%d\" : [{\"id\" : \"%d\", \"commentId\": \"%d\", \"date\": \"%d\"}] }", 3, query.personId(), query.commentId(), query.creationDate().getTime());
    }

    public static String translate(LdbcUpdate4AddForum query) {
        String result = String.format("{ \"update%d\" : [{\"id\" : \"%d\", \"title\" : \"%s\", \"date\" : \"%d\"" +
            ", \"personId\" : \"%d\", \"numTags\": \"%d\", \"tags\" : ", 4, query.forumId(), query.forumTitle(), 
            query.creationDate().getTime(), query.moderatorPersonId(), query.tagIds().size());
        if (query.tagIds().size() == 0) {
            result += "\"\"";
        } else {
            result += ("[\"" + query.tagIds().get(0) + "\"");
            for (int i = 1; i < query.tagIds().size(); ++i) {
                result += (", \"" + query.tagIds().get(i) + "\"");
            }
            result += "]";
        }
        result += String.format("}]}");
        return result;
    }


    public static String translate(LdbcUpdate5AddForumMembership query) {
        return String.format("{ \"update%d\" : [{\"id\" : \"%d\", \"forumId\": \"%d\", \"date\": \"%d\"}] }", 5, query.personId(), query.forumId(), query.joinDate().getTime());
    }


    public static String translate(LdbcUpdate6AddPost query) {
        String result = String.format("{ \"update%d\" : [{\"id\" : \"%d\", \"image\" : \"%s\", \"date\" : \"%d\"" +
            ", \"ip\" : \"%s\", \"browser\" : \"%s\", \"language\" : \"%s\", \"content\" : \"%s\", " + 
            "\"length\" : \"%d\",\"creatorId\": \"%d\", \"forumId\": \"%d\", \"locationId\" : \"%d\", \"numTags\": \"%d\", \"tags\" : ", 6, query.postId(), query.imageFile(), 
            query.creationDate().getTime(), query.locationIp(), query.browserUsed(), query.language(), 
            query.content(), query.length(), query.authorPersonId(), query.forumId(), query.countryId(), query.tagIds().size());
        if (query.tagIds().size() == 0) {
            result += "\"\"";
        } else {
            result += ("[\"" + query.tagIds().get(0) + "\"");
            for (int i = 1; i < query.tagIds().size(); ++i) {
                result += (", \"" + query.tagIds().get(i) + "\"");
            }
            result += "]";
        }
        result += String.format("}]}");
        return result;
    }


    public static String translate(LdbcUpdate7AddComment query) {
        String result = String.format("{ \"update%d\" : [{\"id\" : \"%d\", \"date\" : \"%d\"" +
            ", \"ip\" : \"%s\", \"browser\" : \"%s\", \"content\" : \"%s\", " + 
            "\"length\" : \"%d\", \"creatorId\": \"%d\", \"locationId\" : \"%d\", " +
            "\"replyPost\" : \"%d\", \"replyComment\" : \"%d\"," +
            "\"numTags\": \"%d\", \"tags\" : ", 7, query.commentId(), query.creationDate().getTime(), 
            query.locationIp(), query.browserUsed(), query.content(), query.length(), query.authorPersonId(),
            query.countryId(), query.replyToPostId(), query.replyToCommentId(), query.tagIds().size());
        if (query.tagIds().size() == 0) {
            result += "\"\"";
        } else {
            result += ("[\"" + query.tagIds().get(0) + "\"");
            for (int i = 1; i < query.tagIds().size(); ++i) {
                result += (", \"" + query.tagIds().get(i) + "\"");
            }
            result += "]";
        }
        result += String.format("}]}");
        return result;
    }


    public static String translate(LdbcUpdate8AddFriendship query) {
        return String.format("{ \"update%d\" : [{\"id1\" : \"%d\", \"id2\": \"%d\", \"date\": \"%d\"}] }", 8, query.person1Id(), query.person2Id(), query.creationDate().getTime());
    }


    public static String translate(LdbcShortQuery1PersonProfile query) {
        return String.format("{ \"short%d\" : [{\"id\" : \"%d\"}] }", 1, query.personId());
    }

    public static LdbcShortQuery1PersonProfileResult translateToShort1(String json) {
        LdbcShortQuery1PersonProfileResult result = null;
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"firstName\":"+ ALL_MATCH + 
            ",\"lastName\":" + ALL_MATCH + ",\"birthday\":" + ALL_MATCH + ",\"locationIP\":" + ALL_MATCH +
            ",\"browserUsed\":" + ALL_MATCH + ",\"cityId\":" + ALL_MATCH + ",\"gender\":" + ALL_MATCH +",\"creationDate\":" + ALL_MATCH +"\\}\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            ArrayList<String> tags = new ArrayList<String>();
            String firstName = m.group(1);
            String lastName = m.group(2);
            Long birthday = new Long(m.group(3));
            String ip = m.group(4);
            String browser = m.group(5);
            Long city = new Long(m.group(6)); 
            String gender = m.group(7);
            Long creationDate =new Long(m.group(8));
            result = new LdbcShortQuery1PersonProfileResult(firstName, lastName, birthday, ip, browser, city, gender, creationDate);
        }
        return result;
    } 

    public static String translate(LdbcShortQuery2PersonPosts query) {
        return String.format("{ \"short%d\" : [{\"id\" : \"%d\"}] }", 2, query.personId());
    }

    public static ArrayList<LdbcShortQuery2PersonPostsResult> translateToShort2(String json) {
        ArrayList<LdbcShortQuery2PersonPostsResult> result = new ArrayList<LdbcShortQuery2PersonPostsResult>();
        Pattern pattern = Pattern.compile("\\{\"Message\":\\{\"id\":" + ALL_MATCH + ",\"content\":" + ALL_MATCH +
            ",\"creationDate\":" + ALL_MATCH +"\\},"+
            "\"Post\":\\{\"id\":" + ALL_MATCH + "\\},"+
            "\"Person\":\\{\"id\":" + ALL_MATCH +
            ",\"firstName\":" + ALL_MATCH +
            ",\"lastName\":" + ALL_MATCH +
            "\\}\\}"); 
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long id = new Long(m.group(1));
            String content = m.group(2);
            Long creationDate = new Long(m.group(3));
            Long originalId = new Long(m.group(4));
            Long originalAuthorId = new Long(m.group(5));
            String originalFirstName = m.group(6);
            String originalLastName = m.group(7);
            result.add(new LdbcShortQuery2PersonPostsResult(id, content, creationDate, originalId, originalAuthorId, originalFirstName, originalLastName));
        }
        return result;
    } 

    public static String translate(LdbcShortQuery3PersonFriends query) {
        return String.format("{ \"short%d\" : [{\"id\" : \"%d\"}] }", 3, query.personId());
    }

    public static ArrayList<LdbcShortQuery3PersonFriendsResult> translateToShort3(String json) {
        ArrayList<LdbcShortQuery3PersonFriendsResult> result = new ArrayList<LdbcShortQuery3PersonFriendsResult>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":" + ALL_MATCH + ",\"firstName\":" + ALL_MATCH +
          ",\"lastName\":" + ALL_MATCH + 
          "\\},\"Knows\":\\{\"creationDate\":"+ ALL_MATCH + "\\}\\}"); 
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long id = new Long(m.group(1));
            String firstName = m.group(2);
            String lastName = m.group(3);
            Long friendshipCreationDate = new Long(m.group(4));
            result.add(new LdbcShortQuery3PersonFriendsResult(id, firstName, lastName, friendshipCreationDate));
        }
        return result;
    } 

    public static String translate(LdbcShortQuery4MessageContent query) {
        return String.format("{ \"short%d\" : [{\"id\" : \"%d\"}] }", 4, query.messageId());
    }

    public static LdbcShortQuery4MessageContentResult translateToShort4(String json) {
        LdbcShortQuery4MessageContentResult result = null;
        Pattern pattern = Pattern.compile("\\{\"Message\":\\{\"content\":" + ALL_MATCH +",\"creationDate\":" + ALL_MATCH +"\\}\\}"); 
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            String content = m.group(1);
            Long creationDate = new Long(m.group(2));
            result = new LdbcShortQuery4MessageContentResult(content, creationDate);
        }
        return result;
    } 

    public static String translate(LdbcShortQuery5MessageCreator query) {
        return String.format("{ \"short%d\" : [{\"id\" : \"%d\"}] }", 5, query.messageId());
    }

    public static LdbcShortQuery5MessageCreatorResult translateToShort5(String json) {
        LdbcShortQuery5MessageCreatorResult result = null;
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":" + ALL_MATCH + ",\"firstName\":" + ALL_MATCH +
          ",\"lastName\":" + ALL_MATCH + "\\}\\}"); 
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long id = new Long(m.group(1));
            String firstName = m.group(2);
            String lastName = m.group(3);
            result = new LdbcShortQuery5MessageCreatorResult(id, firstName, lastName);
        }
        return result;
    }

    public static int byteArrayToInt(byte[] b, int offset)
    {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value += (b[offset+i] & 0x000000FF) << (8*i);
        }
        return value;
    }

    public static int byteArrayToLong(byte[] b, int offset)
    {
        int value = 0;
        for (int i = 0; i < 8; i++) {
            value += (b[offset+i] & 0x00000000000000FF) << (8*i);
        }
        return value;
    }

    /*public static LdbcShortQuery5MessageCreatorResult translateToShort5(byte[] data) {
        LdbcShortQuery5MessageCreatorResult result = null;
        int position = 0;
        int numResults = byteArrayToInt(data,position);
        position += 4;
        for(int i = 0; i < numResults; ++i) {
            long id = byteArrayToLong(data,position);
            position+=8;
            int length = byteArrayToInt(data,position);
            position+=4;
            String firstName = new String(data,position,length);
            position+=length;
            length = byteArrayToInt(data,position);
            position+=4;
            String lastName = new String(data,position,length);
            position+=length;
            result = new LdbcShortQuery5MessageCreatorResult(id, firstName, lastName);
        }
        return result;
    }*/

    public static String translate(LdbcShortQuery6MessageForum query) {
        return String.format("{ \"short%d\" : [{\"id\" : \"%d\"}] }", 6, query.messageId());
    }

    public static LdbcShortQuery6MessageForumResult translateToShort6(String json) {
        LdbcShortQuery6MessageForumResult result = null;
        Pattern pattern = Pattern.compile("\\{\"Forum\":\\{\"id\":" + ALL_MATCH + ",\"title\":" + ALL_MATCH + "\\}" + 
            ",\"Person\":\\{\"id\":" + ALL_MATCH + ",\"firstName\":" + ALL_MATCH + ",\"lastName\":" + ALL_MATCH +"\\}\\}"); 
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long forumId = new Long(m.group(1));
            String title = m.group(2);
            Long personId = new Long(m.group(3));
            String firstName = m.group(4);
            String lastName = m.group(5);
            result = new LdbcShortQuery6MessageForumResult(forumId, title, personId, firstName, lastName);
        }
        return result;
    } 

    public static String translate(LdbcShortQuery7MessageReplies query) {
        return String.format("{ \"short%d\" : [{\"id\" : \"%d\"}] }", 7, query.messageId());
    }

    public static ArrayList<LdbcShortQuery7MessageRepliesResult> translateToShort7(String json) {
        ArrayList<LdbcShortQuery7MessageRepliesResult> result = new ArrayList<LdbcShortQuery7MessageRepliesResult>();
        Pattern pattern = Pattern.compile("\\{\"Comment\":\\{\"id\":" + ALL_MATCH + 
            ",\"content\":" + ALL_MATCH + 
            ",\"creationDate\":" + ALL_MATCH + "\\},"+
            "\"Person\":\\{\"id\":" + ALL_MATCH + 
            ",\"firstName\":" + ALL_MATCH + 
            ",\"lastName\":" + ALL_MATCH + 
            "\\}"+
            ",\"Known\":"+ ALL_MATCH +
            "\\}"); 
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long id = new Long(m.group(1));
            String content = m.group(2);
            Long creationDate = new Long(m.group(3));
            Long personId = new Long(m.group(4));
            String personFirstName = m.group(5);
            String personLastName = m.group(6);
            Boolean known = new Boolean(m.group(7));
            result.add(new LdbcShortQuery7MessageRepliesResult(id, content, creationDate, personId, personFirstName, personLastName, known));
        }
        return result;
    }

    /*

    public static String translate(LdbcSnbBiQuery1PostingSummary query ) {
        return String.format("{ \"bi%d\" : [{\"date\" : \"%d\"}] }", 1, query.date());
    }

    public static ArrayList<LdbcSnbBiQuery1PostingSummaryResult> translateToSnbBiQuery1(String json) {
        ArrayList<LdbcSnbBiQuery1PostingSummaryResult> result = new ArrayList<LdbcSnbBiQuery1PostingSummaryResult>();
        Pattern pattern = Pattern.compile("\\{\"Group\":\\{\"year\":"+ALL_MATCH+",\"isReply\":"+ALL_MATCH+
                ",\"category\":"+ALL_MATCH+",\"count\":"+ ALL_MATCH+",\"avg\":"+ALL_MATCH+",\"sum\":"+ ALL_MATCH+
                ",\"per\":"+ ALL_MATCH+"\\}\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Integer year = new Integer(m.group(1));
            Boolean isReply =  new Boolean(m.group(2));
            Integer category = new Integer(m.group(3));
            Integer count = new Integer(m.group(4));
            Integer avg = new Integer(m.group(5));
            Integer sum = new Integer(m.group(6));
            Float per = new Float(m.group(7));
            result.add(new LdbcSnbBiQuery1PostingSummaryResult(year,isReply, category, count, avg, sum, per));
        }
        return result;
    }

    public static String translate(LdbcSnbBiQuery2TopTags query ) {
        String result = String.format("{ \"bi%d\" : [{\"date1\" : \"%d\", \"date2\" : \"%d\", \"country1\" : \"%s\", " +
                        "\"country2\" : \"%s\", \"limit\" : \"%d\"}]}", 2, query.startDate(), query.endDate(), query
                        .country1()
                , query.country2(), query.limit());
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery2TopTagsResult> translateToSnbBiQuery2(String json) {
        ArrayList<LdbcSnbBiQuery2TopTagsResult> result = new ArrayList<LdbcSnbBiQuery2TopTagsResult>();
        Pattern pattern = Pattern.compile("\\{\"Group\":\\{\"country\":"+ALL_MATCH+",\"month\":"+ALL_MATCH+
                ",\"gender\":"+ALL_MATCH+",\"age_group\":"+ALL_MATCH+",\"tag_name\":"+ALL_MATCH+
                ",\"count\":"+ALL_MATCH+"\\}\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            String country = new String(m.group(1));
            Integer month = new Integer(m.group(2));
            String gender =  new String(m.group(3));
            Integer age_group = new Integer(m.group(4));
            String tag_name = new String(m.group(5));
            Integer count = new Integer(m.group(6));
            result.add(new LdbcSnbBiQuery2TopTagsResult(country, month, gender, age_group, tag_name, count));
        }
        return result;
    }

    public static String translate(LdbcSnbBiQuery3TagEvolution query )  {
        String result = String.format("{\"bi%d\" : [{\"year\" : \"%d\", \"month\" : \"%d\", \"limit\" : \"%d\"}]}", 3,
                query.year(), query.month(), query.limit());
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery3TagEvolutionResult> translateToSnbBiQuery3(String json) {
        ArrayList<LdbcSnbBiQuery3TagEvolutionResult> result = new ArrayList<LdbcSnbBiQuery3TagEvolutionResult>();
        Pattern pattern = Pattern.compile("\\{\"Tag\":\\{\"name\":"+ALL_MATCH+"\\},\"countInterval1\":"+ALL_MATCH+"," +
                        "\"countInterval2\":"+ALL_MATCH+",\"diff\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            String name = new String(m.group(1));
            Integer count1 = new Integer(m.group(2));
            Integer count2 = new Integer(m.group(3));
            Integer diff = new Integer(m.group(4));
            result.add(new LdbcSnbBiQuery3TagEvolutionResult(name, count1, count2, diff));
        }
        return result;
    }

    public static String translate(LdbcSnbBiQuery4PopularCountryTopics query )  {
        String result = String.format("{ \"bi%d\" : [{\"tagClass\" : \"%s\", \"country\" : \"%s\", \"limit\" : " +
                "\"%d\"}]} ", 4, query
                .tagClass(), query.country(), query.limit() );
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery4PopularCountryTopicsResult> translateToSnbBiQuery4(String json) {
        ArrayList<LdbcSnbBiQuery4PopularCountryTopicsResult> result = new ArrayList<LdbcSnbBiQuery4PopularCountryTopicsResult>();
        Pattern pattern = Pattern.compile("\\{\"Forum\":\\{\"id\":"+ALL_MATCH+",\"title\":"+ALL_MATCH+",\"creationDate\":"+ALL_MATCH+",\"moderator\":"+ALL_MATCH+",\"count\":"+ALL_MATCH+"\\}\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long id = new Long(m.group(1));
            String title = new String(m.group(2));
            Long creationDate = new Long(m.group(3));
            Long moderator = new Long(m.group(4));
            Integer count = new Integer(m.group(5));
            result.add(new LdbcSnbBiQuery4PopularCountryTopicsResult(id, title, creationDate, moderator, count));
        }
        return result;
    }

    public static String translate(LdbcSnbBiQuery5TopCountryPosters query )  {
        String result = String.format("{ \"bi%d\" : [{\"country\" : \"%s\", \"limit\" : \"%d\"}]} ", 5, query.country
                (), query.limit() );
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery5TopCountryPostersResult> translateToSnbBiQuery5(String json) {
        ArrayList<LdbcSnbBiQuery5TopCountryPostersResult> result = new ArrayList<LdbcSnbBiQuery5TopCountryPostersResult>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":"+ALL_MATCH+",\"firstName\":"+ALL_MATCH+"," +
                        "\"lastName\":"+ALL_MATCH+",\"creationDate\":"+ALL_MATCH+"\\},\"count\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long id = new Long(m.group(1));
            String firstName = new String(m.group(2));
            String lastName = new String(m.group(3));
            Long creationDate = new Long(m.group(4));
            Integer count = new Integer(m.group(5));
            result.add(new LdbcSnbBiQuery5TopCountryPostersResult(id, firstName, lastName, creationDate, count));
        }
        return result;
    }

    public static String translate(LdbcSnbBiQuery6ActivePosters query )  {
        String result = String.format("{ \"bi%d\" : [{\"tag\" : \"%s\", \"limit\" : \"%d\"}]} ", 6, query.tag(), query
                .limit() );
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery6ActivePostersResult> translateToSnbBiQuery6(String json) {
        ArrayList<LdbcSnbBiQuery6ActivePostersResult> result = new ArrayList<LdbcSnbBiQuery6ActivePostersResult>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":"+ALL_MATCH+"\\},\"replyCount\":"+ALL_MATCH+",\"likeCount\":"+ALL_MATCH+",\"postCount\":"+ALL_MATCH+",\"score\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long id = new Long(m.group(1));
            Integer replyCount = new Integer(m.group(2));
            Integer likeCount = new Integer(m.group(3));
            Integer postCount = new Integer(m.group(4));
            Integer score = new Integer(m.group(5));
            result.add(new LdbcSnbBiQuery6ActivePostersResult(id, replyCount, likeCount, postCount, score));
        }
        return result;
    }

    public static String translate( LdbcSnbBiQuery7AuthoritativeUsers query )  {
        String result = String.format("{ \"bi%d\" : [{\"tag\" : \"%s\", \"limit\" : \"%d\"}]} ", 7, query.tag(), query
                .limit() );
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery7AuthoritativeUsersResult> translateToSnbBiQuery7(String json) {
        ArrayList<LdbcSnbBiQuery7AuthoritativeUsersResult> result = new ArrayList<LdbcSnbBiQuery7AuthoritativeUsersResult>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":"+ALL_MATCH+"\\},\"score\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long id = new Long(m.group(1));
            Integer score = new Integer(m.group(2));
            result.add(new LdbcSnbBiQuery7AuthoritativeUsersResult(id, score));
        }
        return result;
    }

    public static String translate( LdbcSnbBiQuery8RelatedTopics query )  {
        String result = String.format("{ \"bi%d\" : [{\"tag\" : \"%s\", \"limit\" : \"%d\"}]} ", 8, query.tag(), query
                .limit() );
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery8RelatedTopicsResult> translateToSnbBiQuery8(String json) {
        ArrayList<LdbcSnbBiQuery8RelatedTopicsResult> result = new ArrayList<LdbcSnbBiQuery8RelatedTopicsResult>();
        Pattern pattern = Pattern.compile("\\{\"Tag\":\\{\"name\":"+ALL_MATCH+"\\},\"count\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            String name= new String(m.group(1));
            Integer count = new Integer(m.group(2));
            result.add(new LdbcSnbBiQuery8RelatedTopicsResult(name,count));
        }
        return result;
    }

    public static String translate( LdbcSnbBiQuery9RelatedForums query )  {
        String result = String.format("{\"bi%d\" : [{\"tagClass1\" : \"%s\", \"tagClass2\" : \"%s\", \"threshold\" : " +
                "\"%d\", \"limit\": \"%d\"}]}", 9, query.tagClass1(), query.tagClass2(), query.threshold(), query.limit());
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery9RelatedForumsResult> translateToSnbBiQuery9(String json) {
        ArrayList<LdbcSnbBiQuery9RelatedForumsResult> result = new ArrayList<LdbcSnbBiQuery9RelatedForumsResult>();
        Pattern pattern = Pattern.compile("\\{\"Forum\":\\{\"id\":"+ALL_MATCH+"\\},\"count1\":"+ALL_MATCH+",\"count2\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long id = new Long(m.group(1));
            Integer count1 = new Integer(m.group(2));
            Integer count2 = new Integer(m.group(3));
            result.add(new LdbcSnbBiQuery9RelatedForumsResult(id,count1, count2));
        }
        return result;
    }

    public static String translate( LdbcSnbBiQuery10TagPerson query )  {
        String result = String.format("{ \"bi%d\" : [{\"tag\" : \"%s\", \"date\" : \"%d\", \"limit\" : \"%d\"}]} ", 10,
                query.tag(), query
                .date(), query.limit() );
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery10TagPersonResult> translateToSnbBiQuery10(String json) {
        ArrayList<LdbcSnbBiQuery10TagPersonResult> result = new ArrayList<LdbcSnbBiQuery10TagPersonResult>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":"+ALL_MATCH+"\\},\"score\":"+ALL_MATCH+",\"friendsScore\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long id = new Long(m.group(1));
            Integer score = new Integer(m.group(2));
            Integer friendScore = new Integer(m.group(3));
            result.add(new LdbcSnbBiQuery10TagPersonResult(id, score, friendScore));
        }
        return result;
    }

    public static String translate( LdbcSnbBiQuery11UnrelatedReplies query )  {
        String result = String.format("{ \"bi%d\" : [{\"country\" : \"%s\", \"blacklist\" : [", 11, query.country());
        String firstWord = query.blacklist().get(0);
        result += String.format("\"%s\"", firstWord);
        for(int i = 1; i < query.blacklist().size(); ++i ) {
            String word = query.blacklist().get(i);
            result += String.format(",\"%s\"", word);
        }
        result += String.format("], \"blacklistSize\" : \"%d\", \"limit\" : \"%d\" }]} ", query.blacklist().size(), query.limit());
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery11UnrelatedRepliesResult> translateToSnbBiQuery11(String json) {
        ArrayList<LdbcSnbBiQuery11UnrelatedRepliesResult> result = new ArrayList<LdbcSnbBiQuery11UnrelatedRepliesResult>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":"+ALL_MATCH+"\\},\"Tag\":\\{\"name\":"+ALL_MATCH+"\\},\"countLikes\":"+ALL_MATCH+",\"countReplies\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long id = new Long(m.group(1));
            String name = new String(m.group(2));
            Integer countLikes = new Integer(m.group(3));
            Integer countReplies = new Integer(m.group(4));
            result.add(new LdbcSnbBiQuery11UnrelatedRepliesResult(id, name, countLikes, countReplies));
        }
        return result;
    }

    public static String translate( LdbcSnbBiQuery12TrendingPosts query )  {
        String result = String.format("{ \"bi%d\" : [{\"date\" : \"%s\", \"likeThreshold\" : \"%d\", \"limit\" : " +
                "\"%d\"}]}", 12, query.date(), query.likeThreshold(), query.limit());
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery12TrendingPostsResult> translateToSnbBiQuery12(String json) {
        ArrayList<LdbcSnbBiQuery12TrendingPostsResult> result = new ArrayList<LdbcSnbBiQuery12TrendingPostsResult>();
        Pattern pattern = Pattern.compile("\\{\"Post\":\\{\"id\":"+ALL_MATCH+",\"creationDate\":"+ALL_MATCH+"\\},\"Person\":\\{\"firstName\":"+ALL_MATCH+",\"lastName\":"+ALL_MATCH+"\\},\"likeCount\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long postId = new Long(m.group(1));
            Long postCreationDate = new Long(m.group(2));
            String personFirstName = new String(m.group(3));
            String personLastName = new String(m.group(4));
            Integer likeCount = new Integer(m.group(5));
            result.add(new LdbcSnbBiQuery12TrendingPostsResult(postId, postCreationDate, personFirstName,
                    personLastName, likeCount));
        }
        return result;
    }

    public static String translate( LdbcSnbBiQuery13PopularMonthlyTags query )  {
        String result = String.format("{ \"bi%d\" : [{\"country\" : \"%s\", \"limit\" : \"%d\"}]}", 13, query.country(), query.limit());
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery13PopularMonthlyTagsResult> translateToSnbBiQuery13(String json) {
        ArrayList<LdbcSnbBiQuery13PopularMonthlyTagsResult> result = new ArrayList<LdbcSnbBiQuery13PopularMonthlyTagsResult>();
        Pattern pattern = Pattern.compile("\\{\"year\":"+ALL_MATCH+",\"month\":"+ALL_MATCH+",\"tags\":"+LIST_MATCH+"\\}");
        Pattern tagPattern = Pattern.compile("\\{\"popularity\":"+ALL_MATCH+",\"name\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Integer year = new Integer(m.group(1));
            Integer month = new Integer(m.group(2));
            ArrayList<LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity> tags = new ArrayList<LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity>();
            Matcher tagMatcher = tagPattern.matcher(m.group(3));
            while(tagMatcher.find()) {
                tags.add(new LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity(new String(tagMatcher.group(2)), new Integer(tagMatcher.group(1))));
            }
            result.add(new LdbcSnbBiQuery13PopularMonthlyTagsResult(year, month, tags));
        }
        return result;
    }

    public static String translate( LdbcSnbBiQuery14TopThreadInitiators query )  {
        String result = String.format("{ \"bi%d\" : [{\"begin\" : \"%s\",\"end\" : \"%s\",\"limit\" : \"%d\"}]}", 14,
                query.startDate(), query.endDate(), query.limit());
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery14TopThreadInitiatorsResult> translateToSnbBiQuery14(String json) {
        ArrayList<LdbcSnbBiQuery14TopThreadInitiatorsResult> result = new ArrayList<LdbcSnbBiQuery14TopThreadInitiatorsResult>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":"+ALL_MATCH+",\"firstName\":"+ALL_MATCH+",\"lastName\":"+ALL_MATCH+"\\},\"threadCount\":"+ALL_MATCH+",\"messageCount\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long id = new Long(m.group(1));
            String firstName = new String(m.group(2));
            String lastName = new String(m.group(3));
            Integer threadCount = new Integer(m.group(4));
            Integer messageCount = new Integer(m.group(5));
            result.add(new LdbcSnbBiQuery14TopThreadInitiatorsResult(id,firstName, lastName, threadCount, messageCount
            ));
        }
        return result;
    }

    public static String translate( LdbcSnbBiQuery15SocialNormals query )  {
        String result = String.format("{ \"bi%d\" : [{\"country\" : \"%s\", \"limit\":\"%d\"}]}", 15, query.country(), query.limit());
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery15SocialNormalsResult> translateToSnbBiQuery15(String json) {
        ArrayList<LdbcSnbBiQuery15SocialNormalsResult> result = new ArrayList<LdbcSnbBiQuery15SocialNormalsResult>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":"+ALL_MATCH+"\\},\"count\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long id = new Long(m.group(1));
            Integer count = new Integer(m.group(2));
            result.add(new LdbcSnbBiQuery15SocialNormalsResult(id,count));
        }
        return result;
    }

    public static String translate( LdbcSnbBiQuery16ExpertsInSocialCircle query )  {
        String result = String.format("{ \"bi%d\" : [{\"person\":\"%s\", \"country\" : \"%s\", \"tagClass\":\"%s\", " +
                        "\"min\":\"%d\", \"max\":\"%d\", \"limit\":\"%d\"}]}",
                16, query.personId(), query.country(), query.tagClass(), query.minPathDistance(), query.maxPathDistance(),
                query.limit());
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery16ExpertsInSocialCircleResult> translateToSnbBiQuery16(String json) {
        ArrayList<LdbcSnbBiQuery16ExpertsInSocialCircleResult> result = new ArrayList<LdbcSnbBiQuery16ExpertsInSocialCircleResult>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":"+ALL_MATCH+"\\},\"Tag\":\\{\"name\":"+ALL_MATCH+"\\},\"count\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long id = new Long(m.group(1));
            String name = new String(m.group(2));
            Integer count = new Integer(m.group(3));
            result.add(new LdbcSnbBiQuery16ExpertsInSocialCircleResult(id, name, count));
        }
        return result;
    }

    public static String translate( LdbcSnbBiQuery17FriendshipTriangles query )  {
        String result = String.format("{ \"bi%d\" : [{\"country\" : \"%s\", \"limit\":\"%d\"}]}", 17, query.country(), 1);
        return result;
    }

    public static LdbcSnbBiQuery17FriendshipTrianglesResult translateToSnbBiQuery17(String json) {
        ArrayList<LdbcSnbBiQuery17FriendshipTrianglesResult> result = new ArrayList<LdbcSnbBiQuery17FriendshipTrianglesResult>();
        Pattern pattern = Pattern.compile("\\{\"count\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Integer count = new Integer(m.group(1));
            result.add(new LdbcSnbBiQuery17FriendshipTrianglesResult(count));
        }
        return result.get(0);
    }

    public static String translate( LdbcSnbBiQuery18PersonPostCounts query )  {
        StringBuilder languages = new StringBuilder();
        languages.append("[");
        languages.append("\""+query.languages().get(0)+"\"");
        for(int i = 1; i < query.languages().size(); ++i) {
            languages.append(",\""+query.languages().get(i)+"\"");
        }
        languages.append("]");
        String result = String.format("{ \"bi%d\" : [{\"date\" : \"%d\", \"numLanguages\" : \"%d\", \"languages\" : " +
                        "%s, \"threshold\" : \"%d\",\"limit\":\"%d\"}]}", 18,
                query.date(), query.languages().size(), languages.toString(), query.lengthThreshold(), query.limit());
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery18PersonPostCountsResult> translateToSnbBiQuery18(String json) {
        ArrayList<LdbcSnbBiQuery18PersonPostCountsResult> result = new ArrayList<LdbcSnbBiQuery18PersonPostCountsResult>();
        Pattern pattern = Pattern.compile("\\{\"postCount\":"+ALL_MATCH+",\"personCount\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Integer count = new Integer(m.group(1));
            Integer personCount = new Integer(m.group(2));
            result.add(new LdbcSnbBiQuery18PersonPostCountsResult(count,personCount));
        }
        return result;
    }


    public static String translate( LdbcSnbBiQuery19StrangerInteraction query )  {
        String result = String.format("{ \"bi%d\" : [{\"date\" : \"%s\", \"tagClass1\":\"%s\", \"tagClass2\":\"%s\", \"limit\":\"%d\"}]}", 19, query.date(), query.tagClass1(), query.tagClass2(), query.limit());
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery19StrangerInteractionResult> translateToSnbBiQuery19(String json) {
        ArrayList<LdbcSnbBiQuery19StrangerInteractionResult> result = new ArrayList<LdbcSnbBiQuery19StrangerInteractionResult>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":"+ALL_MATCH+"\\},\"strangersCount\":"+ALL_MATCH+",\"messageCount\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long id = new Long(m.group(1));
            Integer strangersCount = new Integer(m.group(2));
            Integer messageCount = new Integer(m.group(3));
            result.add(new LdbcSnbBiQuery19StrangerInteractionResult(id,strangersCount, messageCount));
        }
        return result;
    }

    public static String translate(LdbcSnbBiQuery20HighLevelTopics query ) {
        String result = String.format("{ \"bi%d\" : [{\"classes\" : ", 20);
        result += String.format("[");
        String s = query.tagClasses().get(0);
        if( s != null ) {
            result += String.format("\"%s\"",s);
            for (int i = 1; i < query.tagClasses().size(); ++i) {
                result += String.format(", \"%s\"",query.tagClasses().get(i));
            }
        }
        result += String.format("],");
        result += String.format("\"numClasses\":\"%d\",\"limit\" : \"%d\" }] }", query.tagClasses().size(), query.limit());
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery20HighLevelTopicsResult> translateToSnbBiQuery20(String json) {
        ArrayList<LdbcSnbBiQuery20HighLevelTopicsResult> result = new ArrayList<LdbcSnbBiQuery20HighLevelTopicsResult>();
        Pattern pattern = Pattern.compile("\\{\"TagClass\":\\{\"name\":"+ALL_MATCH+"\\},\"postCount\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            String className = new String(m.group(1));
            Integer count = new Integer(m.group(2));
            result.add(new LdbcSnbBiQuery20HighLevelTopicsResult(className, count));
        }
        return result;
    }


    public static String translate( LdbcSnbBiQuery21Zombies query )  {
        String result = String.format("{ \"bi%d\" : [{\"date\" : \"%s\", \"country\":\"%s\", \"limit\":\"%d\"}]}", 21, query.endDate(), query.country(), query.limit());
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery21ZombiesResult> translateToSnbBiQuery21(String json) {
        ArrayList<LdbcSnbBiQuery21ZombiesResult> result = new ArrayList<LdbcSnbBiQuery21ZombiesResult>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id\":"+ALL_MATCH+"\\},\"zombieScore\":"+ALL_MATCH+",\"zombieCount\":"+ALL_MATCH+",\"likeCount\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long id = new Long(m.group(1));
            Double zombieScore = new Double(m.group(2));
            Integer zombieCount = new Integer(m.group(3));
            Integer likeCount = new Integer(m.group(4));
            result.add(new LdbcSnbBiQuery21ZombiesResult(id,zombieCount, likeCount, zombieScore));
        }
        return result;
    }

    public static String translate( LdbcSnbBiQuery22InternationalDialog query )  {
        String result = String.format("{ \"bi%d\" : [{\"country1\":\"%s\",\"country2\":\"%s\", \"limit\":\"%d\"}]}",
                22, query.country1(), query.country2(), query.limit());
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery22InternationalDialogResult> translateToSnbBiQuery22(String json) {
        ArrayList<LdbcSnbBiQuery22InternationalDialogResult> result = new ArrayList<LdbcSnbBiQuery22InternationalDialogResult>();
        Pattern pattern = Pattern.compile("\\{\"Person\":\\{\"id1\":"+ALL_MATCH+",\"id2\":"+ALL_MATCH+"\\}," +
                "\"City\":\\{\"name\":"+ALL_MATCH+"\\},\"score\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            Long id1 = new Long(m.group(1));
            Long id2 = new Long(m.group(2));
            String cityName = new String(m.group(3));
            Integer score = new Integer(m.group(4));
            result.add(new LdbcSnbBiQuery22InternationalDialogResult(id1,id2, cityName,score));
        }
        return result;
    }

    public static String translate( LdbcSnbBiQuery23HolidayDestinations query )  {
        String result = String.format("{ \"bi%d\" : [{\"country\":\"%s\", \"limit\":\"%d\"}]}", 23, query.country(), query.limit());
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery23HolidayDestinationsResult> translateToSnbBiQuery23(String json) {
        ArrayList<LdbcSnbBiQuery23HolidayDestinationsResult> result = new ArrayList<LdbcSnbBiQuery23HolidayDestinationsResult>();
        Pattern pattern = Pattern.compile("\\{\"Country\":\\{\"name\":"+ALL_MATCH+"\\},\"month\":"+ALL_MATCH+",\"messageCount\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            String name = new String(m.group(1));
            Integer month = new Integer(m.group(2));
            Integer messageCount = new Integer(m.group(3));
            result.add(new LdbcSnbBiQuery23HolidayDestinationsResult(messageCount, name, month));
        }
        return result;
    }

    public static String translate( LdbcSnbBiQuery24MessagesByTopic query )  {
        String result = String.format("{ \"bi%d\" : [{\"tagClass\":\"%s\", \"limit\":\"%d\"}]}", 24, query.tagClass(), query.limit());
        return result;
    }

    public static ArrayList<LdbcSnbBiQuery24MessagesByTopicResult> translateToSnbBiQuery24(String json) {
        ArrayList<LdbcSnbBiQuery24MessagesByTopicResult> result = new ArrayList<LdbcSnbBiQuery24MessagesByTopicResult>();
        Pattern pattern = Pattern.compile("\\{\"Continent\":\\{\"name\":"+ALL_MATCH+"\\},\"messageCount\":"+ALL_MATCH+",\"likeCount\":"+ALL_MATCH+",\"year\":"+ALL_MATCH+",\"month\":"+ALL_MATCH+"\\}");
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            String name = new String(m.group(1));
            Integer messageCount = new Integer(m.group(2));
            Integer likeCount = new Integer(m.group(3));
            Integer year = new Integer(m.group(4));
            Integer month = new Integer(m.group(5));
            result.add(new LdbcSnbBiQuery24MessagesByTopicResult( messageCount, likeCount, year, month, name));
        }
        return result;
    }

    public static String translate(LdbcSnbBiQuery25WeightedPaths query ) {
        return String.format("{ \"bi%d\" : [{\"id1\" : \"%d\", \"id2\" : \"%d\", \"startDate\" : \"%s\", " +
                        "\"endDate\" : \"%s\"}] }", 25, query.person1Id(),query.person2Id(), query.startDate(), query
                .endDate());
    }

    public static ArrayList<LdbcSnbBiQuery25WeightedPathsResult> translateToSnbBiQuery25(String json) {
        ArrayList<LdbcSnbBiQuery25WeightedPathsResult> result = new ArrayList<LdbcSnbBiQuery25WeightedPathsResult>();
        Pattern pattern = Pattern.compile("\\{\"path\":" + LIST_MATCH + ",\"weight\":" + ALL_MATCH + "\\}");
        Pattern pathPattern = Pattern.compile(ALL_MATCH);
        Matcher m = pattern.matcher(json);
        while (m.find()) {
            ArrayList<Long> path = new ArrayList<Long>();
            Matcher pathMatch = pathPattern.matcher(m.group(1));
            Double weight = new Double(m.group(2));
            while (pathMatch.find()) {
                path.add(new Long(pathMatch.group(1)));
            }
            result.add(new LdbcSnbBiQuery25WeightedPathsResult(path,weight));
        }
        return result;
    }
    */


};

