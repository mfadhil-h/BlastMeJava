package com.blastme.messaging.smsbulk.transceiver;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.json.JSONObject;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.DataSource;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.RedisPooler;
import com.blastme.messaging.toolpooler.Tool;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import io.lettuce.core.api.sync.RedisCommands;

public class TransceiverTelinCPAASX {
	private static Logger logger;	

	// GET parameter: https://AC8e7108b2e619832e97b5a3fcaf402290:4b7a5bc000682b815e3f96a5835ab7f8@telin.restcomm.com/restcomm/2012-04-24/Accounts/AC8e7108b2e619832e97b5a3fcaf402290/SMS/Messages -d "To=628119692829" -d "From=622125070001" -d "Body=Test Thank you"
	//private static String url = "https://AC8e7108b2e619832e97b5a3fcaf402290:4b7a5bc000682b815e3f96a5835ab7f8@telin.restcomm.com/restcomm/2012-04-24/Accounts/AC8e7108b2e619832e97b5a3fcaf402290/SMS/Messages";
//	private static String requeueName = "REQUEUE_TRANSMISSION";
	//private static String queueTransceiverCommand = "COMMAND_TELIN_TRANSCEIVER";
	
	private static Tool tool;
	
//	private TransactionRedisPooler transactionRedisPooler;
//	private SaveDBRedisTrxDataPooler saveDBRedisTrxDataPooler;
	
	private static RedisPooler redisPooler;
	private static RedisCommands<String, String> redisCommand;
//	private static int redisExpiry = 7 * 24 * 60 * 60; // 7 days from last operation
	
	private RabbitMQPooler rabbitMqPooler;
	private com.rabbitmq.client.Connection connectionRabbit;
	private Channel channelRabbit;
	
	//private final static ExecutorService execCommand = Executors.newFixedThreadPool(1);
	private HashMap<String, String> mapUrlTelin = new HashMap<String, String>();

	public TransceiverTelinCPAASX() {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("TRANSCEIVER_TELINCPAAS");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate Tool
		tool = new Tool();
		
		// Initiate transactionRedisPooler
//		transactionRedisPooler = new TransactionRedisPooler();
		
		// Initiate RabbitMQPooler
		rabbitMqPooler = new RabbitMQPooler();
		connectionRabbit = rabbitMqPooler.getConnection();
		channelRabbit = rabbitMqPooler.getChannel(connectionRabbit);
		
		// Initiate RedisPooler
		redisPooler = new RedisPooler();
		redisCommand = redisPooler.redisInitiateConnection();

		loadTelinURLMap();
		
		//execCommand.execute(new TransceiverCommand());
		
		// Initiate SaveDBRedisTrxData
//		saveDBRedisTrxDataPooler = new SaveDBRedisTrxDataPooler();
		
		LoggingPooler.doLog(logger, "INFO", "TransceiverTelinCPAAS", "TransceiverTelinCPAAS", false, false, false, "", "Starting Transceiver TransceiverTelinCPAAS ...", null);	
	}
	
	private void loadTelinURLMap(){
		// Load url map telin ke redis
		Connection connection = null;
		PreparedStatement statement = null;
	    ResultSet resultSet = null;

		try{
            BasicDataSource bds = DataSource.getInstance().getBds();
            connection = bds.getConnection();

            statement = connection.prepareStatement("select api_username, telin_url from transceiver_telin_property where is_active = true");
            resultSet = statement.executeQuery();
            
            while(resultSet.next()){
            	JSONObject jsonDetail = new JSONObject();

            	String apiUserName = resultSet.getString("api_username");
            	String telinUrl = resultSet.getString("telin_url");
            	
            	jsonDetail.put("apiUserName", apiUserName.trim());
            	jsonDetail.put("telinUrl", telinUrl);
            	
            	// Write into redis
            	String redisKey = "telintransceiver-" + apiUserName.trim();
            	String redisVal = jsonDetail.toString();
            	
            	// Put into mapUrlTelin
            	mapUrlTelin.put(apiUserName.trim(), redisVal);
            	
            	redisPooler.redisSet(redisCommand, redisKey, redisVal);
        		LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS", "loadTelinURLMap", false, false, false, "", 
        				"Load apiUserName: " + apiUserName + " - value: " + redisVal + " into REDIS.", null);	
            }			
		} catch (Exception e) {
			e.printStackTrace();
    		LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS", "loadTelinURLMap", true, false, false, "", 
    				"Failed to load transceiver TELIN property to REDIS. Error occured", e);	
		} finally {
			try {
				statement.close();
				resultSet.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	//"To=628119692829" -d "From=622125070001" -d "Body=Test Thank you
	private List<NameValuePair> composeParameter(String messageId, String from, String msisdn, String message){
		List<NameValuePair> listParams = new ArrayList<NameValuePair>();

		try{
			// Development - Remove it for production
			//from = "622125070001";
			//from = "MITSUBISHI";
						
			listParams.add(new BasicNameValuePair("From", from));
			listParams.add(new BasicNameValuePair("To", msisdn));
			listParams.add(new BasicNameValuePair("Body", message));
		} catch (Exception e) {
			e.printStackTrace();
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS", "composeGetParameter", true, false, false, messageId, 
					"Failed to compose GET parameter message.", e);
		}
		
		return listParams;
	}
	
	private String getTelinURL(String apiUserName){
		String theURL = "";
		
//		String redisKey = "telintransceiver-" + apiUserName.trim();
//		String redisVal = redisPooler.redisGet(redisCommand, redisKey);
//		
//		if(redisVal == null){
//			redisVal = "";
//		} else {
//			JSONObject jsonTelin = new JSONObject(redisVal);
//			theURL = jsonTelin.getString("telinUrl");
//		}
		
		// Override ambil dari mapUrlTelin
		if (mapUrlTelin.containsKey(apiUserName) == true) {
			JSONObject jsonTelin = new JSONObject(mapUrlTelin.get(apiUserName));
			theURL = jsonTelin.getString("telinUrl");
		}
		
		return theURL;
	}

	private void processQueueData(String queueName, String message){
		System.out.println("Incoming Message: " + message);
//		jsonQueuedMessage.put("messageId", messageId);
//		jsonQueuedMessage.put("msisdn", msisdn);
//		jsonQueuedMessage.put("message", theSMS);
//		jsonQueuedMessage.put("vendorSenderId", vendorSenderId);
//		jsonQueuedMessage.put("telecomId", telecomId);
//		jsonQueuedMessage.put("vendorParameter", vendorParameter);
//		jsonQueuedMessage.put("routedVendorId", routedVendorId);	
		
		try{
			JSONObject jsonMessage = new JSONObject(message);
			
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS - " + queueName, "processQueueData", false, false, false, "", 
					"jsonMessage: " + jsonMessage.toString(), null);
			
			String messageId = jsonMessage.getString("messageId");
			String msisdn = jsonMessage.getString("msisdn");
			String theSMS = jsonMessage.getString("message");
			String vendorSenderId = jsonMessage.getString("vendorSenderId");
//			String telecomId = jsonMessage.getString("telecomId");
//			String vendorParameter = jsonMessage.getString("vendorParameter");
//			String routedVendorId = jsonMessage.getString("routedVendorId");
			String apiUserName = jsonMessage.getString("apiUserName");
			
			String telinUrl = getTelinURL(apiUserName);
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS - " + queueName, "processQueueData", false, false, false, messageId, 
					"telinUrl: " + telinUrl, null);

			List<NameValuePair> listParameter = composeParameter(messageId, vendorSenderId, msisdn.trim(), theSMS.trim());
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS - " + queueName, "processQueueData", false, false, false, messageId, 
					"getParameter: " + listParameter.toString(), null);
			
			if(telinUrl.trim().length() > 0){
				//HashMap<String, String> mapHit = tool.hitHTTPSPostFormUrl(messageId, url, listParameter, null);
				HashMap<String, String> mapHit = tool.hitHTTPSPostFormUrl(messageId, telinUrl, listParameter, null);
				LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS - " + queueName, "processQueueData", false, false, false, messageId, 
						"mapHit: " + tool.convertHashMapToJSON(mapHit).toString(), null);
			} else {
				LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS - " + queueName, "processQueueData", false, false, false, messageId, 
						"FAILED to hit TELIN CPAAS. URL for the API UserName is NOT DEFINED.", null);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS", "processQueueData", true, false, false, "", 
					"Failed to process the data from queue " + queueName + ". Error occured.", e);			
		}	
	}
	
	private void readQueue(String queueName){
		try{
			channelRabbit.queueDeclare(queueName, true, false, false, null);
			channelRabbit.basicQos(50);
			
			LoggingPooler.doLog(logger, "INFO", "TransceiverTelinCPAAS - " + queueName, "readQueue - " + queueName, false, false, true, "", 
					"Reading queue " + queueName + " is ready, waiting for message ... ", null);
			
			Consumer consumer = new DefaultConsumer(channelRabbit) {
			      @Override
			      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
			          throws IOException {
			        String message = new String(body, "UTF-8");
			        
					try{
						LoggingPooler.doLog(logger, "INFO", "TransceiverTelinCPAAS - " + queueName, "readQueue - " + queueName, false, false, true, "", 
								"Receive message: " + message, null);

						// Process the message from queue
				        processQueueData(queueName, message);
//						Runnable theHit = new hitTelin(queueName, message);
//						
//						Thread thread = new Thread(theHit);
//						thread.start();
						
					} finally {
						LoggingPooler.doLog(logger, "INFO", "TransceiverTelinCPAAS - " + queueName, "readQueue - " + queueName, false, false, true, "", 
								"Done processing message: " + message, null);

						channelRabbit.basicAck(envelope.getDeliveryTag(), false);
					}
			      }
			};

			boolean autoAck = false; // If not finally exectued well, no ack to rabbitmq, message not gone
			channelRabbit.basicConsume(queueName, autoAck, consumer);
		} catch (Exception e){
			LoggingPooler.doLog(logger, "INFO", "TransceiverJatisMobile", "readQueue - " + queueName, true, false, false, "", 
					"Failed to access queue " + queueName, e);
		}
	}
	
	public static void main(String[] args) {
		//String queueTelin = "TELIN01";
		String queueTelin = args[0];
		
		TransceiverTelinCPAASX cpass = new TransceiverTelinCPAASX();
		
		System.out.println("Starting Transceiver TELIN by reading queue " + queueTelin);
		cpass.readQueue(queueTelin);		
	}
	
//	private static class hitTelin implements Runnable {
//		private String theQueueName;
//		private String theMessage;
//		
//		public hitTelin(String queueName, String message) {
//			theQueueName = queueName;
//			theMessage = message;
//		}
//
//		//"To=628119692829" -d "From=622125070001" -d "Body=Test Thank you
//		private List<NameValuePair> composeParameter(String messageId, String from, String msisdn, String message){
//			List<NameValuePair> listParams = new ArrayList<NameValuePair>();
//
//			try{
//				// Development - Remove it for production
//				//from = "622125070001";
//				//from = "MITSUBISHI";
//							
//				listParams.add(new BasicNameValuePair("From", from));
//				listParams.add(new BasicNameValuePair("To", msisdn));
//				listParams.add(new BasicNameValuePair("Body", message));
//			} catch (Exception e) {
//				e.printStackTrace();
//				LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS", "composeGetParameter", true, false, false, messageId, 
//						"Failed to compose GET parameter message.", e);
//			}
//			
//			return listParams;
//		}
//		
//		private String getTelinURL(String apiUserName){
//			String theURL = "";
//			
//			String redisKey = "telintransceiver-" + apiUserName.trim();
//			String redisVal = redisPooler.redisGet(redisCommand, redisKey);
//			
//			if(redisVal == null){
//				redisVal = "";
//			} else {
//				JSONObject jsonTelin = new JSONObject(redisVal);
//				theURL = jsonTelin.getString("telinUrl");
//			}
//			
//			return theURL;
//		}
//
//		private void processQueueData(){
//			System.out.println("Incoming Message: " + theMessage);
////			jsonQueuedMessage.put("messageId", messageId);
////			jsonQueuedMessage.put("msisdn", msisdn);
////			jsonQueuedMessage.put("message", theSMS);
////			jsonQueuedMessage.put("vendorSenderId", vendorSenderId);
////			jsonQueuedMessage.put("telecomId", telecomId);
////			jsonQueuedMessage.put("vendorParameter", vendorParameter);
////			jsonQueuedMessage.put("routedVendorId", routedVendorId);	
//			
//			try{
//				JSONObject jsonMessage = new JSONObject(theMessage);
//				
//				LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS - " + theQueueName, "processQueueData", false, false, false, "", 
//						"jsonMessage: " + jsonMessage.toString(), null);
//				
//				String messageId = jsonMessage.getString("messageId");
//				String msisdn = jsonMessage.getString("msisdn");
//				String theSMS = jsonMessage.getString("message");
//				String vendorSenderId = jsonMessage.getString("vendorSenderId");
////				String telecomId = jsonMessage.getString("telecomId");
////				String vendorParameter = jsonMessage.getString("vendorParameter");
////				String routedVendorId = jsonMessage.getString("routedVendorId");
//				String apiUserName = jsonMessage.getString("apiUserName");
//				
//				String telinUrl = getTelinURL(apiUserName);
//				LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS - " + theQueueName, "processQueueData", false, false, false, messageId, 
//						"telinUrl: " + telinUrl, null);
//
//				List<NameValuePair> listParameter = composeParameter(messageId, vendorSenderId, msisdn.trim(), theSMS.trim());
//				LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS - " + theQueueName, "processQueueData", false, false, false, messageId, 
//						"getParameter: " + listParameter.toString(), null);
//				
//				if(telinUrl.trim().length() > 0){
//					//HashMap<String, String> mapHit = tool.hitHTTPSPostFormUrl(messageId, url, listParameter, null);
//					HashMap<String, String> mapHit = tool.hitHTTPSPostFormUrl(messageId, telinUrl, listParameter, null);
//					LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS - " + theQueueName, "processQueueData", false, false, false, messageId, 
//							"mapHit: " + tool.convertHashMapToJSON(mapHit).toString(), null);
//				} else {
//					LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS - " + theQueueName, "processQueueData", false, false, false, messageId, 
//							"FAILED to hit TELIN CPAAS. URL for the API UserName is NOT DEFINED.", null);
//				}
//				
//			} catch (Exception e) {
//				e.printStackTrace();
//				LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS", "processQueueData", true, false, false, "", 
//						"Failed to process the data from queue " + theQueueName + ". Error occured.", e);			
//			}	
//		}		
//
//		@Override
//		public void run() {
//			processQueueData();
//		}
//	}
	

}
