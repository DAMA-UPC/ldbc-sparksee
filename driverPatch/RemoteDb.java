package com.ldbc.driver.sparksee.workloads.ldbc.snb.interactive.db;

import com.ldbc.driver.*;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.bi.*;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.net.Socket;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringEscapeUtils;

/**
 * @author <a href="http://www.sparsity-technologies.com">Sparsity Technologies</a>
 */
public class RemoteDb extends Db {

    private static final Integer CODE_OK    =  0;
    private static final Integer CODE_ERROR = -1;
    
    
    private static RemoteDBConnectionState remoteDBConnectionState;
    private static String host;
    private static Integer port;

    @Override
    protected void onInit(Map<String, String> properties, LoggingService lService) throws DbException {
        remoteDBConnectionState = new RemoteDBConnectionState();
        //port = Integer.parseInt(properties.get("sparksee.port"));
        //host = "147.83.35.200";
        host = properties.get("sparksee.host");
        port = 9998;
        
        registerOperationHandler(LdbcQuery1.class,  LdbcQuery1Sparksee.class);
        registerOperationHandler(LdbcQuery2.class,  LdbcQuery2Sparksee.class);
        registerOperationHandler(LdbcQuery3.class,  LdbcQuery3Sparksee.class);
        registerOperationHandler(LdbcQuery4.class,  LdbcQuery4Sparksee.class);
        registerOperationHandler(LdbcQuery5.class,  LdbcQuery5Sparksee.class);
        registerOperationHandler(LdbcQuery6.class,  LdbcQuery6Sparksee.class);
        registerOperationHandler(LdbcQuery7.class,  LdbcQuery7Sparksee.class);
        registerOperationHandler(LdbcQuery8.class,  LdbcQuery8Sparksee.class);
        registerOperationHandler(LdbcQuery9.class,  LdbcQuery9Sparksee.class);
        registerOperationHandler(LdbcQuery10.class, LdbcQuery10Sparksee.class);
        registerOperationHandler(LdbcQuery11.class, LdbcQuery11Sparksee.class);
        registerOperationHandler(LdbcQuery12.class, LdbcQuery12Sparksee.class);
        registerOperationHandler(LdbcQuery13.class, LdbcQuery13Sparksee.class);
        registerOperationHandler(LdbcQuery14.class, LdbcQuery14Sparksee.class);
        
        
        registerOperationHandler(LdbcUpdate1AddPerson.class,          LdbcUpdate1AddPersonSparksee.class);
        registerOperationHandler(LdbcUpdate2AddPostLike.class,        LdbcUpdate2AddPostLikeSparksee.class);
        registerOperationHandler(LdbcUpdate3AddCommentLike.class,     LdbcUpdate3AddCommentLikeSparksee.class);
        registerOperationHandler(LdbcUpdate4AddForum.class,           LdbcUpdate4AddForumSparksee.class);
        registerOperationHandler(LdbcUpdate5AddForumMembership.class, LdbcUpdate5AddForumMembershipSparksee.class);
        registerOperationHandler(LdbcUpdate6AddPost.class,            LdbcUpdate6AddPostSparksee.class);
        registerOperationHandler(LdbcUpdate7AddComment.class,         LdbcUpdate7AddCommentSparksee.class);
        registerOperationHandler(LdbcUpdate8AddFriendship.class,      LdbcUpdate8AddFriendshipSparksee.class);

        registerOperationHandler(LdbcShortQuery1PersonProfile.class,  LdbcShortQuery1PersonProfileSparksee.class);
        registerOperationHandler(LdbcShortQuery2PersonPosts.class,  LdbcShortQuery2PersonPostsSparksee.class);
        registerOperationHandler(LdbcShortQuery3PersonFriends.class,  LdbcShortQuery3PersonFriendsSparksee.class);
        registerOperationHandler(LdbcShortQuery4MessageContent.class,  LdbcShortQuery4MessageContentSparksee.class);
        registerOperationHandler(LdbcShortQuery5MessageCreator.class,  LdbcShortQuery5MessageCreatorSparksee.class);
        registerOperationHandler(LdbcShortQuery6MessageForum.class,  LdbcShortQuery6MessageForumSparksee.class);
        registerOperationHandler(LdbcShortQuery7MessageReplies.class,  LdbcShortQuery7MessageRepliesSparksee.class);

        registerOperationHandler(LdbcSnbBiQuery1PostingSummary.class,  LdbcSnbBiQuery1Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery2TopTags.class,  LdbcSnbBiQuery2Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery3TagEvolution.class,  LdbcSnbBiQuery3Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery4PopularCountryTopics.class,  LdbcSnbBiQuery4Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery5TopCountryPosters.class,  LdbcSnbBiQuery5Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery6ActivePosters.class,  LdbcSnbBiQuery6Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery7AuthoritativeUsers.class,  LdbcSnbBiQuery7Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery8RelatedTopics.class,  LdbcSnbBiQuery8Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery9RelatedForums.class,  LdbcSnbBiQuery9Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery10TagPerson.class,  LdbcSnbBiQuery10Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery11UnrelatedReplies.class,  LdbcSnbBiQuery11Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery12TrendingPosts.class,  LdbcSnbBiQuery12Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery13PopularMonthlyTags.class,  LdbcSnbBiQuery13Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery14TopThreadInitiators.class,  LdbcSnbBiQuery14Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery15SocialNormals.class,  LdbcSnbBiQuery15Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery16ExpertsInSocialCircle.class,  LdbcSnbBiQuery16Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery17FriendshipTriangles.class,  LdbcSnbBiQuery17Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery18PersonPostCounts.class,  LdbcSnbBiQuery18Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery19StrangerInteraction.class,  LdbcSnbBiQuery19Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery20HighLevelTopics.class,  LdbcSnbBiQuery20Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery21Zombies.class,  LdbcSnbBiQuery21Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery22InternationalDialog.class,  LdbcSnbBiQuery22Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery23HolidayDestinations.class,  LdbcSnbBiQuery23Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery24MessagesByTopic.class,  LdbcSnbBiQuery24Sparksee.class);
        registerOperationHandler(LdbcSnbBiQuery25WeightedPaths.class,  LdbcSnbBiQuery25Sparksee.class);
    }
    
    @Override
    protected void onClose() throws IOException {
        remoteDBConnectionState.close();
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return remoteDBConnectionState;
    }

    public class RemoteDBConnectionState extends DbConnectionState {

        private RemoteDBConnectionState() {
        }

        @Override
        public void close() {
        }
    }

    private static String socketProcessing(String json) throws Exception {
        long start = System.nanoTime();
            Socket socket = new Socket(host, port);
        //System.out.println("TIME TO CREATE SOCKET: "+(System.nanoTime() - start )/1000000.0f);
        start = System.nanoTime();
            PrintWriter writestream = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
        //System.out.println("TIME TO CREATE PRINTER: "+(System.nanoTime() - start )/1000000.0f);
        start = System.nanoTime();
            writestream.println(json);
        //System.out.println("TIME TO WRITE STREAM: "+(System.nanoTime() - start )/1000000.0f);
        start = System.nanoTime();
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String result = "";
            char[] data = new char[8192];
            int read = 0;
            do {
                String output = new String(data, 0, read);
                result += output;
                read = in.read(data);
            } while (read != -1);
        //System.out.println("TIME READ RESULT: "+(System.nanoTime() - start )/1000000.0f);
            writestream.close();
            in.close();
            socket.close();
	    return StringEscapeUtils.unescapeJava(result);
    }

    private static byte[] socketProcessingByteArray(String json) throws Exception {
        long start = System.nanoTime();
        Socket socket = new Socket(host, port);
        //System.out.println("TIME TO CREATE SOCKET: "+(System.nanoTime() - start )/1000000.0f);
        start = System.nanoTime();
        PrintWriter writestream = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
        //System.out.println("TIME TO CREATE PRINTER: "+(System.nanoTime() - start )/1000000.0f);
        start = System.nanoTime();
        writestream.println(json);
        //System.out.println("TIME TO WRITE STREAM: "+(System.nanoTime() - start )/1000000.0f);
        start = System.nanoTime();
        InputStream in = socket.getInputStream();
        //System.out.println("TIME GET INPUT STREAM: "+(System.nanoTime() - start )/1000000.0f);
        start = System.nanoTime();
        byte[] data = new byte[8192];
        ByteArrayOutputStream ret = new ByteArrayOutputStream();
        int read = 0;
        while((read = in.read(data)) != -1) {
            ret.write(data,0,read);
        }
        writestream.close();
        in.close();
        socket.close();
        //System.out.println("TIME READ DATA: "+(System.nanoTime() - start )/1000000.0f);
        return ret.toByteArray();
    }

    public static class MockList<T> extends ArrayList<T> {
      MockList(String json) {
          this.json = json;
      }

      @Override
      public String toString() {return json;}

      private String json;
    }


    public static class LdbcQuery1Sparksee implements OperationHandler<LdbcQuery1, RemoteDBConnectionState> {
        
        @Override
       	public void executeOperation(LdbcQuery1 operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
		try {
			String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
			resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToQuery1(result), operation);
		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
			resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToQuery1(result), operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcQuery2Sparksee implements OperationHandler<LdbcQuery2, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcQuery2 operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToQuery2(result), operation);
		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
			resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToQuery2(result), operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcQuery3Sparksee implements OperationHandler<LdbcQuery3, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcQuery3 operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
		try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToQuery3(result), operation);
		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
			resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToQuery3(result), operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcQuery4Sparksee implements OperationHandler<LdbcQuery4, RemoteDBConnectionState> {
        
        @Override
        public void executeOperation(LdbcQuery4 operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
		try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToQuery4(result), operation);

		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
            resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToQuery4(result), operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcQuery5Sparksee implements OperationHandler<LdbcQuery5, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcQuery5 operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try {
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToQuery5(result), operation);
		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
            resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToQuery5(result), operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcQuery6Sparksee implements OperationHandler<LdbcQuery6, RemoteDBConnectionState> {
        
        @Override
        public void executeOperation(LdbcQuery6 operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try {
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToQuery6(result), operation);
		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
            resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToQuery6(result), operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcQuery7Sparksee implements OperationHandler<LdbcQuery7, RemoteDBConnectionState> {
        
        @Override
        public void executeOperation(LdbcQuery7 operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
		ArrayList<LdbcQuery7Result> res = LDBCppQueryTranslator.translateToQuery7(result);
            resultReporter.report(res.size(), res , operation);

		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
			resultReporter.report(CODE_ERROR, new ArrayList<LdbcQuery7Result>() , operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcQuery8Sparksee implements OperationHandler<LdbcQuery8, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcQuery8 operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToQuery8(result), operation);

		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
			resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToQuery8(result), operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcQuery9Sparksee implements OperationHandler<LdbcQuery9, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcQuery9 operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToQuery9(result), operation);

		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
			resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToQuery9(result), operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcQuery10Sparksee implements OperationHandler<LdbcQuery10, RemoteDBConnectionState> {
        
        @Override
        public void executeOperation(LdbcQuery10 operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToQuery10(result), operation);
		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
			resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToQuery10(result), operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcQuery11Sparksee implements OperationHandler<LdbcQuery11, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcQuery11 operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToQuery11(result), operation);
		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
			resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToQuery11(result), operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcQuery12Sparksee implements OperationHandler<LdbcQuery12, RemoteDBConnectionState> {
         
        @Override
        public void executeOperation(LdbcQuery12 operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToQuery12(result), operation);

		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
            resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToQuery12(result), operation);
			throw new DbException();
		}
        }
    }
    
    public static class LdbcQuery13Sparksee implements OperationHandler<LdbcQuery13, RemoteDBConnectionState> {
        
        @Override
        public void executeOperation(LdbcQuery13 operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToQuery13(result), operation);
		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
            resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToQuery13(result), operation);
			throw new DbException();
		}
        }
    }
    
    public static class LdbcQuery14Sparksee implements OperationHandler<LdbcQuery14, RemoteDBConnectionState> {
        
        @Override
        public void executeOperation(LdbcQuery14 operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToQuery14(result), operation);
		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
            resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToQuery14(result), operation);
			throw new DbException();
		}
        }
    }
    
    
    public static class LdbcUpdate1AddPersonSparksee implements OperationHandler<LdbcUpdate1AddPerson, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate1AddPerson operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK,  LdbcNoResult.INSTANCE , operation); 
		} catch(Exception e){
            e.printStackTrace();
            resultReporter.report(CODE_ERROR,  LdbcNoResult.INSTANCE , operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcUpdate2AddPostLikeSparksee implements OperationHandler<LdbcUpdate2AddPostLike, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate2AddPostLike operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try{
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK,  LdbcNoResult.INSTANCE , operation);
            } catch(Exception e){
                e.printStackTrace();
                resultReporter.report(CODE_ERROR,  LdbcNoResult.INSTANCE , operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcUpdate3AddCommentLikeSparksee implements OperationHandler<LdbcUpdate3AddCommentLike, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate3AddCommentLike operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK,  LdbcNoResult.INSTANCE , operation); 

		} catch(Exception e){
            e.printStackTrace();
			resultReporter.report(CODE_ERROR,  LdbcNoResult.INSTANCE , operation);
			throw new DbException();
		}

        }
    }

    public static class LdbcUpdate4AddForumSparksee implements OperationHandler<LdbcUpdate4AddForum, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate4AddForum operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK,  LdbcNoResult.INSTANCE , operation); 

		} catch(Exception e){
            e.printStackTrace();
			resultReporter.report(CODE_ERROR,  LdbcNoResult.INSTANCE , operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcUpdate5AddForumMembershipSparksee implements OperationHandler<LdbcUpdate5AddForumMembership, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate5AddForumMembership operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK,  LdbcNoResult.INSTANCE , operation);
		} catch(Exception e){
            e.printStackTrace();
			resultReporter.report(CODE_ERROR,  LdbcNoResult.INSTANCE , operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcUpdate6AddPostSparksee implements OperationHandler<LdbcUpdate6AddPost, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate6AddPost operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK,  LdbcNoResult.INSTANCE , operation);
		} catch(Exception e){
            e.printStackTrace();
			resultReporter.report(CODE_ERROR,  LdbcNoResult.INSTANCE , operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcUpdate7AddCommentSparksee implements OperationHandler<LdbcUpdate7AddComment, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate7AddComment operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK,  LdbcNoResult.INSTANCE , operation);
		} catch(Exception e){
            e.printStackTrace();
			resultReporter.report(CODE_ERROR,  LdbcNoResult.INSTANCE , operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcUpdate8AddFriendshipSparksee implements OperationHandler<LdbcUpdate8AddFriendship, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate8AddFriendship operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK,  LdbcNoResult.INSTANCE , operation);
		} catch(Exception e){
            e.printStackTrace();
			resultReporter.report(CODE_ERROR,  LdbcNoResult.INSTANCE , operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcShortQuery1PersonProfileSparksee implements OperationHandler<LdbcShortQuery1PersonProfile, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery1PersonProfile operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToShort1(result), operation);
		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
            resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToShort1(result), operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcShortQuery2PersonPostsSparksee implements OperationHandler<LdbcShortQuery2PersonPosts, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery2PersonPosts operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
	try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToShort2(result), operation);
		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
            resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToShort2(result), operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcShortQuery3PersonFriendsSparksee implements OperationHandler<LdbcShortQuery3PersonFriends, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery3PersonFriends operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
	try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToShort3(result), operation);
		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
            resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToShort3(result), operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcShortQuery4MessageContentSparksee implements OperationHandler<LdbcShortQuery4MessageContent, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery4MessageContent operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToShort4(result), operation);
		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
            resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToShort4(result), operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcShortQuery5MessageCreatorSparksee implements OperationHandler<LdbcShortQuery5MessageCreator, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery5MessageCreator operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            long start = System.nanoTime();
    //        byte[] result = socketProcessingByteArray(LDBCppQueryTranslator.translate(operation));
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            //System.out.println("Time query + com:"+(System.nanoTime() - start ) / 1000000.0f);
            start = System.nanoTime();
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToShort5(result), operation);
            //System.out.println("Time parse results:"+(System.nanoTime() - start )/ 1000000.0f);
		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
			resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToShort5(result), operation);
            //LdbcShortQuery5MessageCreatorResult result = new LdbcShortQuery5MessageCreatorResult(0, "", "");
            //resultReporter.report(CODE_ERROR, result, operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcShortQuery6MessageForumSparksee implements OperationHandler<LdbcShortQuery6MessageForum, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery6MessageForum operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToShort6(result), operation);
		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
			resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToShort6(result), operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcShortQuery7MessageRepliesSparksee implements OperationHandler<LdbcShortQuery7MessageReplies, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery7MessageReplies operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
		try{
            String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
            resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToShort7(result), operation);
		} catch(Exception e){
			System.err.println(e.getMessage());
			String result = "{\"ERROR\":\"ERROR\"}";
			resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToShort7(result), operation);
			throw new DbException();
		}
        }
    }

    public static class LdbcSnbBiQuery1Sparksee implements OperationHandler<LdbcSnbBiQuery1PostingSummary, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery1PostingSummary operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery1(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery1(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery2Sparksee implements OperationHandler<LdbcSnbBiQuery2TopTags, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery2TopTags operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery2(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery2(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery3Sparksee implements OperationHandler<LdbcSnbBiQuery3TagEvolution, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery3TagEvolution operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery3(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery3(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery4Sparksee implements OperationHandler<LdbcSnbBiQuery4PopularCountryTopics, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery4PopularCountryTopics operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery4(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery4(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery5Sparksee implements OperationHandler<LdbcSnbBiQuery5TopCountryPosters, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery5TopCountryPosters operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery5(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery5(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery6Sparksee implements OperationHandler<LdbcSnbBiQuery6ActivePosters, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery6ActivePosters operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery6(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery6(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery7Sparksee implements OperationHandler<LdbcSnbBiQuery7AuthoritativeUsers, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery7AuthoritativeUsers operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery7(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery7(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery8Sparksee implements OperationHandler<LdbcSnbBiQuery8RelatedTopics, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery8RelatedTopics operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery8(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery8(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery9Sparksee implements OperationHandler<LdbcSnbBiQuery9RelatedForums, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery9RelatedForums operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery9(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery9(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery10Sparksee implements OperationHandler<LdbcSnbBiQuery10TagPerson, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery10TagPerson operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery10(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery10(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery11Sparksee implements OperationHandler<LdbcSnbBiQuery11UnrelatedReplies, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery11UnrelatedReplies operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery11(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery11(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery12Sparksee implements OperationHandler<LdbcSnbBiQuery12TrendingPosts, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery12TrendingPosts operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery12(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery12(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery13Sparksee implements OperationHandler<LdbcSnbBiQuery13PopularMonthlyTags, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery13PopularMonthlyTags operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery13(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery13(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery14Sparksee implements OperationHandler<LdbcSnbBiQuery14TopThreadInitiators, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery14TopThreadInitiators operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery14(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery14(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery15Sparksee implements OperationHandler<LdbcSnbBiQuery15SocialNormals, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery15SocialNormals operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery15(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery15(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery16Sparksee implements OperationHandler<LdbcSnbBiQuery16ExpertsInSocialCircle, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery16ExpertsInSocialCircle operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery16(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery16(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery17Sparksee implements OperationHandler<LdbcSnbBiQuery17FriendshipTriangles, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery17FriendshipTriangles operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery17(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery17(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery18Sparksee implements OperationHandler<LdbcSnbBiQuery18PersonPostCounts, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery18PersonPostCounts operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery18(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery18(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery19Sparksee implements OperationHandler<LdbcSnbBiQuery19StrangerInteraction, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery19StrangerInteraction operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery19(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery19(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery20Sparksee implements OperationHandler<LdbcSnbBiQuery20HighLevelTopics, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery20HighLevelTopics operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery20(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery20(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery21Sparksee implements OperationHandler<LdbcSnbBiQuery21Zombies, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery21Zombies operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery21(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery21(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery22Sparksee implements OperationHandler<LdbcSnbBiQuery22InternationalDialog, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery22InternationalDialog operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery22(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery22(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery23Sparksee implements OperationHandler<LdbcSnbBiQuery23HolidayDestinations, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery23HolidayDestinations operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery23(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery23(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery24Sparksee implements OperationHandler<LdbcSnbBiQuery24MessagesByTopic, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery24MessagesByTopic operation, RemoteDBConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery24(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery24(result), operation);
                throw new DbException();
            }
        }
    }

    public static class LdbcSnbBiQuery25Sparksee implements OperationHandler<LdbcSnbBiQuery25WeightedPaths, RemoteDBConnectionState> {

        @Override
        public void executeOperation(LdbcSnbBiQuery25WeightedPaths operation, RemoteDBConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            try {
                String result = socketProcessing(LDBCppQueryTranslator.translate(operation));
                resultReporter.report(CODE_OK, LDBCppQueryTranslator.translateToSnbBiQuery25(result), operation);
            } catch(Exception e){
                System.err.println(e.getMessage());
                String result = "{\"ERROR\":\"ERROR\"}";
                resultReporter.report(CODE_ERROR, LDBCppQueryTranslator.translateToSnbBiQuery25(result), operation);
                throw new DbException();
            }
        }
    }
}
