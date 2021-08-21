package com.blastme.messaging.smsbulk.transceiver;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;

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
import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import io.lettuce.core.api.sync.RedisCommands;

public class TransceiverTelinCPAASPCUCLIENTWithThrottling {
	private static Logger logger;	

	private static Tool tool;
	
	private static RedisPooler redisPooler;
	private static RedisCommands<String, String> redisCommand;
	
	private RabbitMQPooler rabbitMqPooler;
	private com.rabbitmq.client.Connection connectionRabbit;
	private Channel channelRabbit;
	
	public TransceiverTelinCPAASPCUCLIENTWithThrottling() {
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
		
		// Initiate RabbitMQPooler
		rabbitMqPooler = new RabbitMQPooler();
		connectionRabbit = rabbitMqPooler.getConnection();
		channelRabbit = rabbitMqPooler.getChannel(connectionRabbit);
		
		// Initiate RedisPooler
		redisPooler = new RedisPooler();
		redisCommand = redisPooler.redisInitiateConnection();

		loadTelinURLMap();
		
		LoggingPooler.doLog(logger, "INFO", "TransceiverTelinCPAAS", "TransceiverTelinCPAAS", false, false, false, "", "Starting Transceiver TransceiverTelinCPAAS ...", null);	
	}
	
	private void loadTelinURLMap(){ 
		
		// -- THIS FUNCTION IS ONLY CALLED ONCE, THUS ALL DB CONNECTION NEED TO BE CLOSED AT THE END OF FUNCTION
				
        BasicDataSource bds = DataSource.getInstance().getBds();
		Connection connection = null;
		Statement statement = null;
	    ResultSet resultSet = null;

		try{
            connection = bds.getConnection();

            statement = connection.createStatement();
            String query = "select api_username, telin_url from transceiver_telin_property where is_active = true";
            
            resultSet = statement.executeQuery(query);
            
            while(resultSet.next()){
            	JSONObject jsonDetail = new JSONObject();

            	String apiUserName = resultSet.getString("api_username");
            	String telinUrl = resultSet.getString("telin_url");
            	
            	// New update that telinUrl is now using jatis.neuapix.com instad of telin.restcomm.com
            	telinUrl = telinUrl.replace("telin.restcomm.com", "jatis.neuapix.com");
            	
            	jsonDetail.put("apiUserName", apiUserName.trim());
            	jsonDetail.put("telinUrl", telinUrl);
            	
            	// Write into redis
            	String redisKey = "telintransceiver-" + apiUserName.trim();
            	String redisVal = jsonDetail.toString();
            	
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
				connection.close();
				bds.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void readQueue(String queueName, int tps){
		try{
			channelRabbit.queueDeclare(queueName, true, false, false, null);
			channelRabbit.basicQos(50);
			
			// Guava rateLimiter
			RateLimiter rateLimiter = RateLimiter.create(tps);
			
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

						// Process the messages from queue threadlimiter
						rateLimiter.acquire();
						LoggingPooler.doLog(logger, "INFO", "TransceiverTelinCPAAS - " + queueName, "readQueue - " + queueName, false, false, true, "", 
								"Hitting Telin: " + message, null);
						
						Runnable theHit = new hitTelin(queueName, message);
						
						Thread thread = new Thread(theHit);
						thread.start();
						
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
		String requestedTps = args[1];
		int theTPS = Integer.parseInt(requestedTps);
		
		TransceiverTelinCPAASPCUCLIENTWithThrottling cpass = new TransceiverTelinCPAASPCUCLIENTWithThrottling();
		
		System.out.println("Starting Transceiver TELIN by reading queue " + queueTelin);
		cpass.readQueue(queueTelin, theTPS);		
	}
	
	
	
	
	// -- Hit TELIN thread! -- //
	private class hitTelin implements Runnable {
		private String theQueueName;
		private String theMessage;
		
		public hitTelin(String queueName, String message) {
			theQueueName = queueName;
			theMessage = message;
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

		// API USERNAME: pintarclients
		private String getTelinURL(){
			String theURL = "";
			String apiUserName = "pintarclients";
			
			String redisKey = "telintransceiver-" + apiUserName.trim();
			String redisVal = redisPooler.redisGet(redisCommand, redisKey);
			
			if(redisVal == null){
				redisVal = "";
			} else {
				JSONObject jsonTelin = new JSONObject(redisVal);
				theURL = jsonTelin.getString("telinUrl");
			}
			
			return theURL;
		}

		private void processQueueData(){
			System.out.println("Thread Incoming Message: " + theMessage);
						
//			jsonQueuedMessage.put("messageId", messageId);
//			jsonQueuedMessage.put("msisdn", msisdn);
//			jsonQueuedMessage.put("message", theSMS);
//			jsonQueuedMessage.put("vendorSenderId", vendorSenderId);
//			jsonQueuedMessage.put("telecomId", telecomId);
//			jsonQueuedMessage.put("vendorParameter", vendorParameter);
//			jsonQueuedMessage.put("routedVendorId", routedVendorId);	
			
			try{
				JSONObject jsonMessage = new JSONObject(theMessage);
				
				LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS - " + theQueueName, "processQueueData", false, false, false, "", 
						"jsonMessage: " + jsonMessage.toString(), null);
				
				String messageId = jsonMessage.getString("messageId");
				String msisdn = jsonMessage.getString("msisdn");
				String theSMS = jsonMessage.getString("message");
				String vendorSenderId = jsonMessage.getString("vendorSenderId");
//				String telecomId = jsonMessage.getString("telecomId");
//				String vendorParameter = jsonMessage.getString("vendorParameter");
//				String routedVendorId = jsonMessage.getString("routedVendorId");
				//String apiUserName = "pintarclients"; // general api to telin for non telin program 
				
				//String telinUrl = getTelinURL(apiUserName);
				String telinUrl = getTelinURL();
				LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS - " + theQueueName, "processQueueData", false, false, false, messageId, 
						"telinUrl: " + telinUrl, null);

				List<NameValuePair> listParameter = composeParameter(messageId, vendorSenderId, msisdn.trim(), theSMS.trim());
				LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS - " + theQueueName, "processQueueData", false, false, false, messageId, 
						"getParameter: " + listParameter.toString(), null);
				
				if(telinUrl.trim().length() > 0){
					//HashMap<String, String> mapHit = tool.hitHTTPSPostFormUrl(messageId, url, listParameter, null);
					HashMap<String, String> mapHit = tool.hitHTTPSPostFormUrl(messageId, telinUrl, listParameter, null);
					LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS - " + theQueueName, "processQueueData", false, false, false, messageId, 
							"mapHit: " + tool.convertHashMapToJSON(mapHit).toString(), null);
				} else {
					LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS - " + theQueueName, "processQueueData", false, false, false, messageId, 
							"FAILED to hit TELIN CPAAS. URL for the API UserName is NOT DEFINED.", null);
				}
				
			} catch (Exception e) {
				e.printStackTrace();
				LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS", "processQueueData", true, false, false, "", 
						"Failed to process the data from queue " + theQueueName + ". Error occured.", e);			
			}	
		}		

		@Override
		public void run() {
			processQueueData();
		}
	}
	

}
