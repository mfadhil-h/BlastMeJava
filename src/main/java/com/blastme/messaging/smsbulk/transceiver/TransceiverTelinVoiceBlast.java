package com.blastme.messaging.smsbulk.transceiver;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.json.JSONObject;

import com.beust.jcommander.Strings;
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

public class TransceiverTelinVoiceBlast {
	private static Logger logger;	

	private static Tool tool;
	
	private RabbitMQPooler rabbitMqPooler;
	private com.rabbitmq.client.Connection connectionRabbit;
	private Channel channelRabbit;
	
	private static RedisPooler redisPooler;
	private static RedisCommands<String, String> redisCommand;
	
	private Connection connection = null;
	private PreparedStatement statementUpdateTrxStatus = null;
	
	private HashMap<String, String> mapAccIdTokenId = new HashMap<String, String>();

	public TransceiverTelinVoiceBlast() {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("TRANSCEIVER_TELINVOICEBLAST");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate Tool
		tool = new Tool();
		
		try {
			// Initiate RabbitMQPooler
			rabbitMqPooler = new RabbitMQPooler();
			connectionRabbit = rabbitMqPooler.getConnection();
			channelRabbit = rabbitMqPooler.getChannel(connectionRabbit);
			
			// Initiate redis
			// Initiate RedisPooler
			redisPooler = new RedisPooler();
			redisCommand = redisPooler.redisInitiateConnection();
			
			// Initiate Database
            BasicDataSource bds = DataSource.getInstance().getBds();
            connection = bds.getConnection();
            
            // Initiate statementUpdateTrxStatus
            statementUpdateTrxStatus = connection.prepareStatement("update transaction_sms set status_code = ? where message_id = ?");
            
    		LoggingPooler.doLog(logger, "INFO", "TransceiverTelinVoiceBlast", "TransceiverTelinVoiceBlast", false, false, true, "", 
    				"Application TransceiverTelinVoiceBlast is running ...", null);
		} catch (Exception e) {
			e.printStackTrace();
			
			System.exit(-1);
		}
	}
	
	private void loadAccTokenMap(){
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
            	String telinAccToken = resultSet.getString("telin_url");
//            	telinAccToken = 
            	
//            	jsonDetail.put("apiUserName", apiUserName.trim());
//            	jsonDetail.put("telinUrl", telinUrl);
            	
            	// Write into redis
//            	String redisKey = "telintransceiver-" + apiUserName.trim();
//            	String redisVal = jsonDetail.toString();
//            	
//            	// Put into mapUrlTelin
//            	mapUrlTelin.put(apiUserName.trim(), redisVal);
            	
//            	redisPooler.redisSet(redisCommand, redisKey, redisVal);
//        		LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinCPAAS", "loadTelinURLMap", false, false, false, "", 
//        				"Load apiUserName: " + apiUserName + " - value: " + redisVal + " into REDIS.", null);	
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
			String urlCallback = "http://neuapix.sipintar.co.id:8898/telincb/voiceotp";
			
			String urlApp = "https://ACd60c3ce0c042d3588ff1597731ec4830:de0254297ee5fb844b1dd830ba7f8e8a@jatis.neuapix.com/visual-designer/"
					+ "services/apps/AP1f6eca9735b347eea1262daeb3d5b6c7/controller?SipHeader_X-message=" + URLEncoder.encode(message, "UTF-8");
			listParams.add(new BasicNameValuePair("From", from));
			listParams.add(new BasicNameValuePair("To", msisdn));
			listParams.add(new BasicNameValuePair("Url", urlApp));
			listParams.add(new BasicNameValuePair("StatusCallback", urlCallback));
		} catch (Exception e) {
			e.printStackTrace();
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinVOICEBLAST", "composeGetParameter", true, false, false, messageId, 
					"Failed to compose GET parameter message.", e);
		}
		
		return listParams;
	}
	
	private String getTelinURL(String messageId, String apiUserName){
		String theURL = "";
		
		try{
			theURL = "https://ACd60c3ce0c042d3588ff1597731ec4830:de0254297ee5fb844b1dd830ba7f8e8a@jatis.neuapix.com/restcomm/2012-04-24/Accounts/"
					+ "ACd60c3ce0c042d3588ff1597731ec4830/Calls";
			
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinVOICEBLAST", "getTelinURL", false, false, false, messageId, 
					"theURL: " + theURL, null);				
		} catch (Exception e) {
			e.printStackTrace();
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinVOICEBLAST", "getTelinURL", true, false, false, messageId, 
					"Failed to compose URL to hit to TELIN. Error occured.", e);				
		}			
		
		return theURL;
	}
	
	private void saveMapMessageidSid(String messageId, String callSid) {
		try {
			// Map callSid to messageId
			String redisKey_mapCallSidToMsgId = "mapCallSidToMsgId-" + callSid;
			String redisVal_mapCallSidTomsgId = messageId;
			
			redisPooler.redisSet(redisCommand, redisKey_mapCallSidToMsgId, redisVal_mapCallSidTomsgId);
			
			// Map messageId to callSid
			String redisKey_mapMsgIdToCallSid = "mapMsgIdToCallSid-" + messageId;
			String redisVal_mapMsgIdToCallSid = callSid;
			
			redisPooler.redisSet(redisCommand, redisKey_mapMsgIdToCallSid, redisVal_mapMsgIdToCallSid);
		} catch (Exception e) {
			e.printStackTrace();
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinVOICEBLAST", "saveMapMessageIdSid", true, false, false, messageId, 
					"Failed to save mapping messageId and callSid to redis. Error occured.", e);								
		}
	}
	
	private void updateTransactionStatus(String messageId, String trxStatus) {
		try {
			statementUpdateTrxStatus.setString(1, messageId);
			statementUpdateTrxStatus.setString(2, trxStatus);
			
			int impactedNumber = statementUpdateTrxStatus.executeUpdate();
			
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinVOICEBLAST", "updateTransactionStatus", false, false, false, messageId, 
					"Updating transaction status impacted row: " + impactedNumber, null);			
		} catch (Exception e) {
			e.printStackTrace();
			
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinVOICEBLAST", "updateTransactionStatus", true, false, false, messageId, 
					"Failed to update transaction status. Error occured", e);			
		}
	}

	private void processQueueData(String queueName, String queueMessage){
		System.out.println("Incoming Message: " + queueMessage);
		// {"messageId": "msgId12345", "msisdn": "08111253636", "message": "Ini message dalam bentuk voice", "vendorSenderId": "cakep", "telecomId": "86001", "vendorParameter": "", "routedVendorId": "1", "apiUserName": "chandra"}
//		jsonQueuedMessage.put("messageId", messageId);
//		jsonQueuedMessage.put("msisdn", msisdn);
//		jsonQueuedMessage.put("message", theSMS);
//		jsonQueuedMessage.put("vendorSenderId", vendorSenderId);
//		jsonQueuedMessage.put("telecomId", telecomId);
//		jsonQueuedMessage.put("vendorParameter", vendorParameter);
//		jsonQueuedMessage.put("routedVendorId", routedVendorId);	
		
		try{
			JSONObject jsonMessage = new JSONObject(queueMessage);
			
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinVOICEBLAST - " + queueName, "processQueueData", false, false, false, "", 
					"jsonMessage: " + jsonMessage.toString(), null);
			
			String messageId = jsonMessage.getString("messageId");
			String msisdn = jsonMessage.getString("msisdn");
			String theSMS = jsonMessage.getString("message");
			String vendorSenderId = jsonMessage.getString("vendorSenderId");
			String apiUserName = jsonMessage.getString("apiUserName");
			
			String telinUrl = getTelinURL(messageId, apiUserName);
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinVOICEBLAST - " + queueName, "processQueueData", false, false, false, messageId, 
					"telinUrl: " + telinUrl, null);

			List<NameValuePair> listParameter = composeParameter(messageId, vendorSenderId, msisdn.trim(), theSMS.trim());
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinVOICEBLAST - " + queueName, "processQueueData", false, false, false, messageId, 
					"getParameter: " + listParameter.toString(), null);
			
			if(telinUrl.trim().length() > 0){
				//HashMap<String, String> mapHit = tool.hitHTTPSPostFormUrl(messageId, url, listParameter, null);
				HashMap<String, String> mapHit = tool.hitHTTPSPostFormUrl(messageId, telinUrl, listParameter, null);
				LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinVOICEBLAST - " + queueName, "processQueueData", false, false, false, messageId, 
						"mapHit: " + tool.convertHashMapToJSON(mapHit).toString(), null);
				
				if (mapHit.get("hitStatus").trim().equals("SUCCESS")) {
					String bodyResponse = mapHit.get("hitResultBody").trim();
					String callSID = StringUtils.substringBetween(bodyResponse, "<Sid>", "</Sid>");
					String telinStatus = StringUtils.substringBetween(bodyResponse, "<Status>", "</Status>");
					String blastmeStatus = "002";
					
					// Save map messageId to callSid to redis
					saveMapMessageidSid(messageId, callSID);
					
					// Update delivery data in table transaction
					if (telinStatus.trim().equals("QUEUED")) {
						// PENDING
						blastmeStatus = "002";
					} else if (telinStatus.trim().toLowerCase().equals("initiated")) {
						blastmeStatus = "005";
					} else if (telinStatus.trim().toLowerCase().equals("ringing")) {
						blastmeStatus = "006";
					} else if (telinStatus.trim().toLowerCase().equals("answered")) {
						blastmeStatus = "000";
					} 
					
					if (!blastmeStatus.trim().equals("002")) {
						updateTransactionStatus(messageId, blastmeStatus);
					}					

					LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinVOICEBLAST - " + queueName, "processQueueData", false, false, false, messageId, 
							"Success to hit TELIN. Return status " + blastmeStatus, null);
				} else {
					updateTransactionStatus(messageId, "900");
					
					LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinVOICEBLAST - " + queueName, "processQueueData", false, false, false, messageId, 
							"Failed to hit TELIN. Network issue. Return status 900", null);
				}				
			} else {
				LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinVOICEBLAST - " + queueName, "processQueueData", false, false, false, messageId, 
						"FAILED to hit TELIN CPAAS. URL for the API UserName is NOT DEFINED.", null);
			}
		} catch (Exception e) {
			e.printStackTrace();
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverTelinVOICEBLAST", "processQueueData", true, false, false, "", 
					"Failed to process the data from queue " + queueName + ". Error occured.", e);			
		}	
	}
	
	private void readQueue(String queueName){
		try{
			channelRabbit.queueDeclare(queueName, true, false, false, null);
			channelRabbit.basicQos(50);
			
			LoggingPooler.doLog(logger, "INFO", "TransceiverTelinVOICEBLAST - " + queueName, "readQueue - " + queueName, false, false, true, "", 
					"Reading queue " + queueName + " is ready, waiting for message ... ", null);
			
			Consumer consumer = new DefaultConsumer(channelRabbit) {
			      @Override
			      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
			          throws IOException {
			        String message = new String(body, "UTF-8");
			        
					try{
						LoggingPooler.doLog(logger, "INFO", "TransceiverTelinVOICEBLAST - " + queueName, "readQueue - " + queueName, false, false, true, "", 
								"Receive message: " + message, null);

						// Process the message from queue
				        processQueueData(queueName, message);
					} finally {
						LoggingPooler.doLog(logger, "INFO", "TransceiverTelinVOICEBLAST - " + queueName, "readQueue - " + queueName, false, false, true, "", 
								"Done processing message: " + message, null);

						channelRabbit.basicAck(envelope.getDeliveryTag(), false);
					}
			      }
			};

			boolean autoAck = false; // If not finally exectued well, no ack to rabbitmq, message not gone
			channelRabbit.basicConsume(queueName, autoAck, consumer);
		} catch (Exception e){
			LoggingPooler.doLog(logger, "INFO", "TransceiverTelinVOICEBLAST", "readQueue - " + queueName, true, false, false, "", 
					"Failed to access queue " + queueName, e);
		}
	}
	
	public static void main(String[] args) {
		//String queueTelin = "TELIN01";
		String queueTelin = args[0];
		
		TransceiverTelinVoiceBlast cpass = new TransceiverTelinVoiceBlast();
		
		System.out.println("Starting Transceiver TELIN by reading queue " + queueTelin);
		cpass.readQueue(queueTelin);		
	}
}
