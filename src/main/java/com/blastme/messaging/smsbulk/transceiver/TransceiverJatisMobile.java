package com.blastme.messaging.smsbulk.transceiver;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.json.JSONObject;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.RedisPooler;
import com.blastme.messaging.toolpooler.SaveDBRedisTrxDataPooler;
import com.blastme.messaging.toolpooler.Tool;
import com.blastme.messaging.toolpooler.TransactionRedisPooler;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;

import io.lettuce.core.api.sync.RedisCommands;

public class TransceiverJatisMobile {
	private static Logger logger;	

	private static String url = "http://sms-api.jatismobile.com/index.ashx";
	private static String requeueName = "REQUEUE_TRANSMISSION";
	
	private Tool tool;
	private SimpleDateFormat sdf;
	
	private TransactionRedisPooler transactionRedisPooler;
	private SaveDBRedisTrxDataPooler saveDBRedisTrxDataPooler;
	private RabbitMQPooler rabbitMqPooler;
	private Connection connectionRabbit;
	private Channel channelRabbit;
	
	private RedisPooler redisPooler;
	private RedisCommands<String, String> redisCommand;
	private static int redisExpiry = 7 * 24 * 60 * 60; // 7 days from last operation

	public TransceiverJatisMobile() {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("TRANSCEIVER_JATISMOBILE");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate sdf
		sdf = new SimpleDateFormat("yyMMddHHmmssSSS");
		
		// Initiate Tool
		tool = new Tool();
		
		// Initiate transactionRedisPooler
		transactionRedisPooler = new TransactionRedisPooler();
		
		// Initiate RabbitMQPooler
		rabbitMqPooler = new RabbitMQPooler();
		connectionRabbit = rabbitMqPooler.getConnection();
		channelRabbit = rabbitMqPooler.getChannel(connectionRabbit);
		
		// Initiate RedisPooler
		redisPooler = new RedisPooler();
		redisCommand = redisPooler.redisInitiateConnection();

		// Initiate SaveDBRedisTrxData
		saveDBRedisTrxDataPooler = new SaveDBRedisTrxDataPooler();
		
		LoggingPooler.doLog(logger, "INFO", "TransceiverJatisMobile", "TransceiverJatisMobile", false, false, false, "", "Starting Transceiver Jatis Mobile ...", null);				
	}
	
	private String composeGetUrlParameter(String messageId, String userId, String password, String senderId,
			String msisdn, String message, String division, String batchName, String uploadBy, String channel){

		String completeUrl = "";
		try {
			completeUrl = "userid=" + userId + "&password=" + password + "&sender=" + URLEncoder.encode(senderId, "UTF-8") + "&msisdn=" +
					msisdn + "&message=" + URLEncoder.encode(message, "UTF-8") + "&division=" + URLEncoder.encode(division, "UTF-8") + "&Batchname=" +
					URLEncoder.encode(batchName, "UTF-8") + "&Uploadby=" + URLEncoder.encode(uploadBy, "UTF-8") + "&Channel=" + channel;

			LoggingPooler.doLog(logger, "DEBUG", "TransceiverJatisMobile", "composeGetUrl", false, false, false, messageId, 
					"GET Url: " + completeUrl, null);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverJatisMobile", "composeGetUrl", true, false, false, messageId, 
					"Failed to compose GET URL. Error occured.", e);
		}		
		
		return completeUrl;
	}
	
	
	private HashMap<String, String> hitHTTPGet(String messageId, String url, String getParameter){
		HashMap<String, String> mapHasil = new HashMap<String, String>();
		
		LoggingPooler.doLog(logger, "DEBUG", "TransceiverJatisMobile", "hitHTTPGet", false, false, false, messageId, 
				"Complete URL: " + url + "?" + getParameter, null);
		
		try{
			HashMap<String, String> mapHit = tool.hitHTTPGetGeneral(messageId, url, getParameter, null);
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverJatisMobile", "hitHTTPGet", false, false, false, messageId, 
					"mapHit: " + tool.convertHashMapToJSON(mapHit).toString(), null);

			String statusCode = mapHit.get("hitResultStatus"); // 000, 201 etc - adopting from swithcing
			String bodyRequest = mapHit.get("hitRequestBody");
			String bodyResponse = mapHit.get("hitResultBody");
			String hitDateTime = mapHit.get("hitRequestDateTime");
			String respDateTime = mapHit.get("hitResultDateTime");
			String hitHTTPStatus = mapHit.get("hitResultHttpStatusCode");
			
			mapHasil.put("hitStatus", statusCode);
			mapHasil.put("hitHttpStatusCode", hitHTTPStatus);
			mapHasil.put("hitBodyRequest", bodyRequest);
			mapHasil.put("hitBodyResponse", bodyResponse);
			mapHasil.put("hitRequestDateTime", hitDateTime);
			mapHasil.put("hitResponseDateTime", respDateTime);
		} catch (Exception e) {
			mapHasil.put("hitStatus", "240");
			mapHasil.put("hitHttpStatusCode", "");
			mapHasil.put("hitBodyRequest", url + "?" + getParameter);
			mapHasil.put("hitBodyResponse", "");
			mapHasil.put("hitRequestDateTime", sdf.format(new Date()));
			mapHasil.put("hitResponseDateTime", sdf.format(new Date()));
		}
		
		LoggingPooler.doLog(logger, "DEBUG", "TransceiverJatisMobile", "hitHTTPGet", false, false, false, messageId, 
				"mapHit: " + tool.convertHashMapToJSON(mapHasil).toString(), null);
		
		return mapHasil;
	}
	
	public JSONObject transmitToVendor(String queueName, String messageId, String msisdn, String message, String jatisSenderId, String telecomId, String vendorParameter){
		JSONObject jsonTransmit = new JSONObject();
		/*
		 * 		Parameter as in Jatis SMS MT Bulk Documentation
		 * 		- userid
		 * 		- password
		 * 		- sender
		 * 		- msisdn
		 * 		- message
		 * 		- division
		 * 		- Batchname
		 * 		- Uploadby
		 * 		- Channel	- 0 : Normal SMS
		 * 					- 1 : Alert SMS
		 * 					- 2 : OTP SMS
		 */
		
		int status = 0;
		String statusDesc = "";
		String vendorHitRequest = "";
		String vendorHitDateTime = "";
		String vendorRespBody = "";
		String vendorRespDateTime = "";
		String vendorTrxId = "";
		
		try{
			// Get vendorParameter - {"userid": "xxx", "password": "pss", "division": "div1", "batchName": "batch01", "uploadBy": "me", "channel": "12}
			// {"userid": "pcumitsubishi", "password": "pcumitsubishi235", "division": "Biru Merah", "batchName": "BIRUMERAHMITSUBISHI", "uploadBy": "Franciscus Chandra Pawitra", "channel": "1"}
			JSONObject jsonVendorParameter = new JSONObject(vendorParameter);
			
			if(jsonVendorParameter.has("userid") && jsonVendorParameter.has("password") && jsonVendorParameter.has("division") &&
					jsonVendorParameter.has("batchName") && jsonVendorParameter.has("uploadBy") && jsonVendorParameter.has("channel")){
				String jatisUserId = jsonVendorParameter.getString("userid");
				String jatisPassword = jsonVendorParameter.getString("password");
				String jatisDivision = jsonVendorParameter.getString("division");
				String jatisBatchName = jsonVendorParameter.getString("batchName");
				String jatisUploadBy = jsonVendorParameter.getString("uploadBy");
				String jatisChannel = jsonVendorParameter.getString("channel");
				
				// Compose GET URL
				String getUrlParameter = composeGetUrlParameter(messageId, jatisUserId, jatisPassword, jatisSenderId, msisdn, message, jatisDivision, 
						jatisBatchName, jatisUploadBy, jatisChannel);
				LoggingPooler.doLog(logger, "DEBUG", "TransceiverJatisMobile - " + queueName, "transmitToVendor", false, false, false, messageId, 
						"getURL: " + getUrlParameter, null);
				
				// Hit Jatis Mobile
				HashMap<String, String> mapHit = hitHTTPGet(messageId, url, getUrlParameter);
				
				vendorHitRequest = mapHit.get("hitBodyRequest");
				vendorHitDateTime = mapHit.get("hitRequestDateTime");
				vendorRespBody = mapHit.get("hitBodyResponse");
				vendorRespDateTime = mapHit.get("hitResponseDateTime");
				
				if(mapHit.get("hitStatus").trim().equals("000")){
					status = 0;
					statusDesc = "SUCCESS";
					
					// Extrace response from Jatis Mobile
					String jatisBodyResponse = mapHit.get("hitBodyResponse");
					String[] jtsBodyRespSplitted = jatisBodyResponse.split("&");
					
					// get status from jatis
					String jtsStatusWord = "";
					for(int x = 0; x < jtsBodyRespSplitted.length; x++){
						if(jtsBodyRespSplitted[x].toLowerCase().contains("status")){
							jtsStatusWord = jtsBodyRespSplitted[x].toLowerCase();
						}
					}
					
					if(jtsStatusWord.trim().length() > 0){
						String jtsStatus = jtsStatusWord.replace("status=", "");
						
						if(jtsStatus.trim().equals("1")){
							status = 0;
							statusDesc = "SUCCESS";
							
							// Get jatis messageId
							for(int y = 0; y < jtsBodyRespSplitted.length; y++){
								if(jtsBodyRespSplitted[y].toLowerCase().contains("messageid")){
									vendorTrxId = jtsBodyRespSplitted[y].replace("MessageId=", "");
								}
							}
						} else {
							status = -5; // Bad response from Jatis
							statusDesc = "NEGATIVE RESPONSE FROM JATIS MOBILE.";
						}
					} else {
						status = -4; // Invalid response from Jatis
						statusDesc = "NEGATIVE RESPONSE FROM JATIS MOBILE.";
					}
				} else {
					status = -3;
					statusDesc = "FAILED RESPONSE FROM JATISMOBILE";
					
					LoggingPooler.doLog(logger, "INFO", "TransceiverJatisMobile - " + queueName, "transmitToVendor", false, false, false, messageId, 
							"Failed hitting Jatis Mobile. Negative response. mapHit: " + tool.convertHashMapToJSON(mapHit).toString(), null);
				}
			} else {
				// Vendor parameter for Jatis Mobile is incomplete
				LoggingPooler.doLog(logger, "DEBUG", "TransceiverJatisMobile - " + queueName, "transmitToVendor", false, false, false, messageId, 
						"INCOMPLETE VENDOR PARAMETER for Jatis Mobile. Please check the parameter.", null);
				
				status = -1;
				statusDesc = "INCOMPLETE VENDOR PARAMETER";
			}
		} catch (Exception e) {
			e.printStackTrace();
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverJatisMobile - " + queueName, "transmitToVendor", true, false, false, messageId, 
					"Failed to transmit to Jatis Mobile. Error occured.", e);
			
			status = -2;
			statusDesc = "FAILED. EXCEPTION";
		}

		jsonTransmit.put("status", status);
		jsonTransmit.put("statusDesc", statusDesc);
		jsonTransmit.put("vendorReqBody", vendorHitRequest);
		jsonTransmit.put("vendorReqDateTime", vendorHitDateTime);
		jsonTransmit.put("vendorRespBody", vendorRespBody);
		jsonTransmit.put("vendorRespDateTime", vendorRespDateTime);
		jsonTransmit.put("vendorMessageId", vendorTrxId);
		
		LoggingPooler.doLog(logger, "DEBUG", "TransceiverJatisMobile - " + queueName, "transmitToVendor", false, false, false, messageId, 
				"jsonTransmit: " + jsonTransmit.toString(), null);
		
		return jsonTransmit;
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
			
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverJatisMobile - " + queueName, "processQueueData", false, false, false, "", 
					"jsonMessage: " + jsonMessage.toString(), null);
			
			String messageId = jsonMessage.getString("messageId");
			String msisdn = jsonMessage.getString("msisdn");
			String theSMS = jsonMessage.getString("message");
			String vendorSenderId = jsonMessage.getString("vendorSenderId");
			String telecomId = jsonMessage.getString("telecomId");
			String vendorParameter = jsonMessage.getString("vendorParameter");
			String routedVendorId = jsonMessage.getString("routedVendorId");
						
			JSONObject jsonTransmit = transmitToVendor(queueName, messageId, msisdn, theSMS, vendorSenderId, telecomId, vendorParameter);
			LoggingPooler.doLog(logger, "DEBUG", "TransceiverJatisMobile - " + queueName, "processQueueData", false, false, false, messageId, 
					"jsonTransmit: " + jsonTransmit.toString(), null);
			
			if(jsonTransmit.getInt("status") != 0 && jsonTransmit.getInt("status") != -4){
				// Requeued to follow up
				JSONObject jsonRequeue = new JSONObject();
				jsonRequeue.put("source", "TransceiverJatisMobile");
				jsonRequeue.put("queueName", queueName);
				jsonRequeue.put("messageId", messageId);
				jsonRequeue.put("msisdn", msisdn);
				jsonRequeue.put("theSMS", theSMS);
				jsonRequeue.put("vendorSenderId", vendorSenderId);
				jsonRequeue.put("telecomId", telecomId);
				jsonRequeue.put("vendorParameter", vendorParameter);
				jsonRequeue.put("routedVendorId", routedVendorId);
				
				channelRabbit.queueDeclare(requeueName, true, false, false, null);
				channelRabbit.basicPublish("", requeueName, MessageProperties.PERSISTENT_TEXT_PLAIN, jsonRequeue.toString().getBytes());
				LoggingPooler.doLog(logger, "INFO", "SMSRouter", "processQueueData", false, false, false, messageId, 
						"Failed transmitting the message. Requeuing to " + requeueName, null);			
			}
			
			String vendorRequestBody = jsonTransmit.getString("vendorReqBody");
			String vendorRequestDateTime = jsonTransmit.getString("vendorReqDateTime");
			String vendorResponseBody = jsonTransmit.getString("vendorRespBody");
			String vendorResponseDateTime = jsonTransmit.getString("vendorRespDateTime");
			String vendorMessageId = jsonTransmit.getString("vendorMessageId");

			// Update trxdata
			JSONObject newTrxData = new JSONObject();
			newTrxData.put("vendorId", routedVendorId);
			newTrxData.put("vendorHitDateTime", vendorRequestDateTime);
			newTrxData.put("vendorRequest", vendorRequestBody);
			newTrxData.put("vendorResponse", vendorResponseBody);
			newTrxData.put("vendorRespDateTime", vendorResponseDateTime);
			newTrxData.put("vendorMessageId", vendorMessageId);
			
			transactionRedisPooler.updateTrxRedisData(messageId, newTrxData);
			
			// Load trxdata- and save to database disc
			String redisTrxData = transactionRedisPooler.getTrxRedisData(messageId);
			LoggingPooler.doLog(logger, "DEBUG", "SMSRouter", "processQueueData", false, false, false, messageId, 
					"redisTrxData - " + messageId + ": " + redisTrxData, null);			

			// Save trxdata- from redis to database
			saveDBRedisTrxDataPooler.saveToTrxDBFromTrxRedisTransceiverStage(messageId);
			LoggingPooler.doLog(logger, "DEBUG", "SMSRouter", "processQueueData", false, false, false, messageId, 
					"Redis trxdata saved to database.", null);	
			
			// Update trxJatis-[jatisMessageId] to map to blastmeMessageId
			String redisMapJatisToBlastMeKey = "trxJatis-" + vendorMessageId;
			String redisMapJatisToBlastMeVal = messageId;
			redisPooler.redisSetWithExpiry(redisCommand, redisMapJatisToBlastMeKey, redisMapJatisToBlastMeVal, redisExpiry);
			LoggingPooler.doLog(logger, "DEBUG", "SMSRouter", "processQueueData", false, false, false, messageId, 
					"Redis Mapping Jatis Mobile to Orginal Message Id is saved successfully.", null);	
			
		} catch(Exception e) {
			e.printStackTrace();
			LoggingPooler.doLog(logger, "DEBUG", "SMSRouter", "processQueueData", true, false, false, "", 
					"Failed to process the data from queue " + queueName + ". Error occured.", e);			
		}
	}
	
	private void readQueue(String queueName){
		try{
			channelRabbit.queueDeclare(queueName, true, false, false, null);			
			LoggingPooler.doLog(logger, "INFO", "TransceiverJatisMobile - " + queueName, "readQueue - " + queueName, false, false, true, "", 
					"Reading queue " + queueName + " is ready, waiting for message ... ", null);
			
			Consumer consumer = new DefaultConsumer(channelRabbit) {
			      @Override
			      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
			          throws IOException {
			        String message = new String(body, "UTF-8");
			        
					try{
						LoggingPooler.doLog(logger, "INFO", "TransceiverJatisMobile - " + queueName, "readQueue - " + queueName, false, false, true, "", 
								"Receive message: " + message, null);

						// Process the message from queue
				        processQueueData(queueName, message);
					} finally {
						LoggingPooler.doLog(logger, "INFO", "TransceiverJatisMobile - " + queueName, "readQueue - " + queueName, false, false, true, "", 
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
		if(args.length > 0){
			String queueName = args[0];
			System.out.println("Queue name: " + queueName);
			
			TransceiverJatisMobile trc = new TransceiverJatisMobile();
			trc.readQueue(queueName);
		} else {
			System.out.println("How to use: TransceiverJatisMobile.jar [queueName]");
		}
	}

}
