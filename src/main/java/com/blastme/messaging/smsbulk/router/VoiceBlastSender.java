package com.blastme.messaging.smsbulk.router;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.TimeZone;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.json.JSONObject;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.ClientPropertyPooler;
import com.blastme.messaging.toolpooler.DataSource;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.RedisPooler;
import com.blastme.messaging.toolpooler.RouteSMSPooler;
import com.blastme.messaging.toolpooler.SMSTransactionOperationPooler;
import com.blastme.messaging.toolpooler.TelecomPrefixPooler;
import com.blastme.messaging.toolpooler.Tool;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;

import io.lettuce.core.api.sync.RedisCommands;

public class VoiceBlastSender {
	private static Logger logger;	
	private Tool tool;
	
	private static String queueBatchToProcess = "VOICE_BLAST_SEND_BATCHID";
	private static String queueIncomingMessage = "SMS_INCOMING";
	private static String smsChannel = "VOICEBLAST_WEBUPLOADER";

	
	private static RedisPooler redisPooler;
	private static RedisCommands<String, String> redisCommand;

	private Connection connection = null;
	private PreparedStatement statementReadDetail = null;
	private PreparedStatement statementUpdateDetail = null;
    
    private SMSTransactionOperationPooler smsTransactionOperationPooler;
    private ClientPropertyPooler clientPropertyPooler;
    private RabbitMQPooler rabbitMqPooler;
    private com.rabbitmq.client.Connection connectionRabbit;
    private Channel channelRabbitConsume;
    private Channel channelRabbitPublish;
    
    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMddHHmmssSSS");
    
	public VoiceBlastSender() {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("VOICEBLAST_SENDER");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate tool
		tool = new Tool();
	
		// Initiate smsTransactionOperationPooler
		smsTransactionOperationPooler = new SMSTransactionOperationPooler();
		
		// Initiate TelecomPrefixPooler
		new TelecomPrefixPooler();
		
		// Initiate RouteSMSPooler
		new RouteSMSPooler();
		
		// clientPropertyPooler
		clientPropertyPooler = new ClientPropertyPooler();
		
        try {
    		// Initiate RedisPooler
    		redisPooler = new RedisPooler();
    		redisCommand = redisPooler.redisInitiateConnection();
    		
    		// Initiate RabbitMQPooler
    		rabbitMqPooler = new RabbitMQPooler();	
    		connectionRabbit = rabbitMqPooler.getConnection();
    		
    		// Channel rabbit for consuming from VOICE_BLAST_SEND_BATCHID
    		channelRabbitConsume = rabbitMqPooler.getChannel(connectionRabbit);
    		
    		// Channel rabbit for publishing to SMS_INCOMING
    		channelRabbitPublish = rabbitMqPooler.getChannel(connectionRabbit);
    		
    		// Initiate connection to Postgresql
            BasicDataSource bds = DataSource.getInstance().getBds();
            connection = bds.getConnection();
    		LoggingPooler.doLog(logger, "INFO", "VoiceBlastSender", "VoiceBlastSender", false, false, false, "", 
    				"Application VoiceBlastProcessor is loaded and run.", null);
    		
    		// Select from voice blast detail
			statementReadDetail = connection
					.prepareStatement("select vdet.msisdn, vdet.message, vup.client_sender_id_id, csi.sender_id from "
							+ "voice_blast_upload_detail as vdet left join voice_blast_uploader as vup on "
							+ "vdet.batch_id = vup.batch_id left join client_senderid_sms as csi on "
							+ "vup.client_sender_id_id = csi.client_sender_id_id "
							+ "where (vdet.is_sent IS NULL or vdet.is_sent = false) and vdet.batch_id = ?");

    		// Get autogen_a_number as batchId
			statementUpdateDetail = connection
					.prepareStatement("update voice_blast_upload_detail set is_sent = true, sent_datetime = ? where batch_id = ? and msisdn = ?");
			
        } catch(Exception e) {
        	e.printStackTrace();
    		LoggingPooler.doLog(logger, "INFO", "VoiceBlastSender", "VoiceBlastSender", true, false, false, "", 
    				"Failed to load connection to database server. Error occured.", e);
    		
    		System.exit(-1);
        }		
	}
	
	private JSONObject isPrefixValid(String msisdn){
		JSONObject jsonPrefix = new JSONObject();
		jsonPrefix.put("isValid", false);
		
		Iterator<String> keys = TelecomPrefixPooler.jsonPrefixProperty.keys();
		
		while(keys.hasNext()){
			String key = keys.next();
			if(msisdn.startsWith(key)){
				jsonPrefix.put("isValid", true);
				jsonPrefix.put("prefix", TelecomPrefixPooler.jsonPrefixProperty.getJSONObject(key).get("prefix"));
				jsonPrefix.put("telecomId", TelecomPrefixPooler.jsonPrefixProperty.getJSONObject(key).get("telecomId"));
				jsonPrefix.put("countryCode", TelecomPrefixPooler.jsonPrefixProperty.getJSONObject(key).get("countryCode"));
			}
		}
		
		return jsonPrefix;
	}
	
	private void updateSetVoiceDetail(String messageId, String batchId, String msisdn) {
		try {
			statementUpdateDetail.setObject(1, LocalDateTime.now());
			statementUpdateDetail.setString(2, batchId);
			statementUpdateDetail.setString(3, msisdn);
			
			int impacted = statementUpdateDetail.executeUpdate();
			
    		LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastSender", "updateSetVoiceDetail", false, false, false, messageId, 
    				"Successfully update voice uploader detail - sent datetime and status. Impacted row: " + impacted, null);
		} catch (Exception e) {
			e.printStackTrace();

    		LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastSender", "updateSetVoiceDetail", true, false, false, messageId, 
    				"Failed to update voice uploader detail. Error occured.", e);
		}
	}
	
	// Process the batch
	private void processSendingBatch(String message) {
		try {
			// message = {"batchId": "1928394830292", "clientId": "1029384", "username": "chandrapawitra02"}
			JSONObject jsonMessage = new JSONObject(message);
			
			String batchId = jsonMessage.getString("batchId");
			String clientId = jsonMessage.getString("clientId");
			String userName = jsonMessage.getString("username");
						
			// Read from DB voice detail
			statementReadDetail.setString(1, batchId);
			
			System.out.println("Reading query: " + statementReadDetail.toString());
			
			ResultSet resultSet = statementReadDetail.executeQuery();
			
			int count = 0;
			while (resultSet.next()) {
				String msisdn = resultSet.getString("msisdn");
				String voiceMessage = resultSet.getString("message");
				String clientSenderIdId = resultSet.getString("client_sender_id_id");
				String clientSenderId = resultSet.getString("sender_id");
				
				count = count + 1;
	    		LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastSender", "processSendingBatch", false, false, false, "", 
	    				count + ". Sending batchId: " + batchId + ", clientId: " + clientId + ", userName: " + userName + 
	    				" -> msisdn: " + msisdn + ", clientSenderIdId: " + clientSenderIdId + ", voiceMessage: " + voiceMessage, null);

	    		// send the message
	    		// Assign messageId
	    		String messageId = tool.generateUniqueID();
	    		
				// Check countryCode and Prefix
				JSONObject jsonPrefix = isPrefixValid(msisdn);

				String countryCode = "";
				String prefix = "";
				String telecomId = "";
				if(jsonPrefix.getBoolean("isValid") == true){
					countryCode = jsonPrefix.getString("countryCode");
					prefix = jsonPrefix.getString("prefix");
					telecomId = jsonPrefix.getString("telecomId");
				}

				// Get clientPrice and clientCurrency
				String routeId =  clientSenderIdId + "-" + userName + "-" + telecomId;

				if(RouteSMSPooler.jsonRouteSMSProperty.has(routeId)){
					LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastSender", "processSendingBatch", false, false, false, messageId, 
							"jsonRouteSMSProperty has routeId: " + routeId, null);	

					JSONObject jsonRoute = RouteSMSPooler.jsonRouteSMSProperty.getJSONObject(routeId);

					// Get client price from jsonRoute
					double clientPrice = 0.00000;
					String clientCurrency = "IDR";

					clientPrice = jsonRoute.getDouble("clientPrice");
					clientCurrency = clientPropertyPooler.getCurrencyId(clientId.trim());

					// Updaate data detail voice uploader
					updateSetVoiceDetail(messageId, batchId, msisdn);
					
					// Save initial data before send to router
					LocalDateTime now = LocalDateTime.now();
					String errorCode = "003"; // Default BEING PROCESSED
					smsTransactionOperationPooler.saveInitialSMPPData(messageId, now, batchId, "VOICE_BROADCAST", "", 
							"", now, now, msisdn, voiceMessage, countryCode, prefix, telecomId, errorCode, 
							"AUTOGENAUPLOADER", clientSenderIdId, clientSenderId, clientId, 
							userName, clientPrice, clientCurrency, "VOICE", 1, 
							1);

					// Submit to Redis as initial data with expiry 7 days
					int expiry = 7 * 24 * 60 * 60;
					JSONObject jsonRedis = new JSONObject();
					jsonRedis.put("messageId", messageId);
					jsonRedis.put("receiverDateTime", now.format(formatter));
					jsonRedis.put("transactionDateTime", now.format(formatter));
					jsonRedis.put("msisdn", msisdn);
					jsonRedis.put("message", voiceMessage);
					jsonRedis.put("telecomId", telecomId);
					jsonRedis.put("countryCode", countryCode);
					jsonRedis.put("prefix", prefix);
					jsonRedis.put("errorCode", errorCode);
					jsonRedis.put("apiUserName", userName);
					jsonRedis.put("clientSenderIdId", clientSenderIdId);
					jsonRedis.put("clientSenderId", clientSenderId);
					jsonRedis.put("clientId", clientId);
					jsonRedis.put("clientIpAddress", "");
					jsonRedis.put("receiverType", smsChannel); // AUTOGENAUPLOADER, SMPP and HTTP only

					String redisKey = "trxdata-" + messageId.trim();
					String redisVal = jsonRedis.toString();

					redisPooler.redisSetWithExpiry(redisCommand, redisKey, redisVal, expiry);
					LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastSender", "processSendingBatch", false, false, false, messageId, 
							"Successfully save Initial Data to Database and Redis.", null);

					// Send to INCOMING MESSAGE queue
					JSONObject jsonIncoming = new JSONObject();

					jsonIncoming.put("messageId", messageId);
					jsonIncoming.put("apiUserName", userName);
					jsonIncoming.put("msisdn", msisdn);
					jsonIncoming.put("message", voiceMessage);
					jsonIncoming.put("clientId", clientId);
					jsonIncoming.put("telecomId", telecomId);
					jsonIncoming.put("clientSenderId", clientSenderId);
					jsonIncoming.put("smsChannel",smsChannel);
					jsonIncoming.put("messageLength", 1);
					jsonIncoming.put("messageCount", 1);
					jsonIncoming.put("clientPrice", String.format("%.5f", clientPrice));

					// Publish jsonIncoming to queue INCOMING_SMS
					channelRabbitPublish.queueDeclare(queueIncomingMessage, true, false, false, null);
					channelRabbitPublish.basicPublish("", queueIncomingMessage, MessageProperties.PERSISTENT_TEXT_PLAIN, jsonIncoming.toString().getBytes());
					LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastSender", "processSendingBatch", false, false, false, messageId, 
							"jsonIncoming: " + jsonIncoming.toString() + " published SUCCESSfully!", null);
				} else {
					LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "processQueueBatch", false, false, false, messageId, 
							"Route is NOT DEFINED for batchId " + batchId + ", routeId " + routeId, null);

					//Save initial db with status failed
					String errorCode = "112";
					LocalDateTime now = LocalDateTime.now();
					smsTransactionOperationPooler.saveInitialSMPPData(messageId, now, batchId, "VOICE_BROADCAST", "", 
							"", now, now, msisdn, voiceMessage, countryCode, prefix, telecomId, errorCode, 
							"AUTOGENAUPLOADER", clientSenderIdId, clientSenderId, clientId, 
							userName, 0.00, "", "VOICE", 1, 1);
				}		    		
			}
			
			resultSet.close();
		} catch (Exception e) {
			e.printStackTrace();
			LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "processQueueBatch", true, false, false, "", 
					"Failed to send voice message. Error occured.", e);			
		}
	}
	
	// Read queue VOICE_BLAST_SEND_BATCHID
	private void readQueue() {
		// Incoming message will be: {"batchId": "1928394830292", "clientId": "1029384", "username": "chandrapawitra02"}
		try{
			channelRabbitConsume.queueDeclare(queueBatchToProcess, true, false, false, null);			
			LoggingPooler.doLog(logger, "INFO", "VoiceBlastSender", "readQueue", false, false, true, "", 
					"Reading queue is ready, waiting for message ... ", null);
			
			Consumer consumer = new DefaultConsumer(channelRabbitConsume) {
			      @Override
			      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
			          throws IOException {
			        String message = new String(body, "UTF-8");
			        
					try{
						LoggingPooler.doLog(logger, "INFO", "VoiceBlastSender", "readQueue", false, false, true, "", 
								"Receive message: " + message, null);

						// Process the message from queue
				        processSendingBatch(message);
					} finally {
						LoggingPooler.doLog(logger, "INFO", "VoiceBlastSender", "readQueue", false, false, true, "", 
								"Done processing message: " + message, null);

						channelRabbitConsume.basicAck(envelope.getDeliveryTag(), false);
					}
			      }
			};

			boolean autoAck = false; // If not finally exectued well, no ack to rabbitmq, message not gone
			channelRabbitConsume.basicQos(1); // Read one by one, so if other app FixANumberProcessor running, it can share load
			channelRabbitConsume.basicConsume(queueBatchToProcess, autoAck, consumer);			
		} catch (Exception e){
			LoggingPooler.doLog(logger, "INFO", "VoiceBlastSender", "readQueue", true, false, false, "", 
					"Failed to access queue " + queueBatchToProcess, e);
		}
	}

	public static void main(String[] args) {
		VoiceBlastSender sender = new VoiceBlastSender();
		sender.readQueue();
	}

}
