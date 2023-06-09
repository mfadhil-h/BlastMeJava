package com.blastme.messaging.smsbulk.router;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class SMSFixANumberSenderIdProcessor {
	private static Logger logger;	
	private Tool tool;
	
	private static String queueBatchToProcess = "FIXSIDUPLOADER_TOPUSH_BATCHID";
	private static String queueIncomingMessage = "SMS_INCOMING";
	private static String smsChannel = "FIXANO_WEBUPLOADER";
	private String batchFileDirectory;
	private DateTimeFormatter dtfNormal = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
	
	private static RedisPooler redisPooler;
	private static RedisCommands<String, String> redisCommand;

	private Connection connection = null;
	private PreparedStatement statement = null;
	private PreparedStatement statementIsProcessed = null;
	private PreparedStatement statementScheduledMessage = null;
	//private PreparedStatement statementProgress = null;
    private ResultSet resultSet = null;
    
    private SMSTransactionOperationPooler smsTransactionOperationPooler;
    private ClientPropertyPooler clientPropertyPooler;
    private RabbitMQPooler rabbitMqPooler;
    private com.rabbitmq.client.Connection connectionRabbit;
    private Channel channelRabbit;
	
	public SMSFixANumberSenderIdProcessor() {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("FIXANUMBER_PROCESSOR");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate tool
		tool = new Tool();
		
		// Initiate batchFileDirectory
		batchFileDirectory = Configuration.getUploadBatchFileDirectory();
		
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
    		channelRabbit = rabbitMqPooler.getChannel(connectionRabbit);
    		
    		// Initiate connection to Postgresql
            BasicDataSource bds = DataSource.getInstance().getBds();
            connection = bds.getConnection();
    		LoggingPooler.doLog(logger, "INFO", "FixANumberProcessor", "FixANumberProcessor", false, false, false, "", 
    				"Application AutoGeneratedANumberProcessor is loaded and run.", null);
    		
    		// Select job
			statement = connection.prepareStatement("select ff.message, ff.message_length, ff.no_of_records, ff.no_of_sms_each, "
					+ "ff.no_of_sms_total, ff.message_encoding, ff.sender_id_id, ss.sender_id, ff.b_number_filename, ff.is_processed from fix_a_number_sid as ff "
					+ "left join client_senderid_sms as ss on ff.sender_id_id = ss.client_sender_id_id "
					+ "where ff.batch_id = ? and ff.is_processed = false");

    		// Get autogen_a_number as batchId
			statementIsProcessed = connection
					.prepareStatement("update fix_a_number_sid set is_processed = true where batch_id = ?");
			
			// Insert into fix_a_number_scheduled_message
			statementScheduledMessage = connection
					.prepareStatement("INSERT INTO fix_a_number_scheduled_message(" + 
							"message_id, msisdn, message, client_id, telecom_id, client_sender_id, sms_channel, api_username, message_length, message_count, client_price_per_submit, schedule) " + 
							"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

			// Insert into table autogenbnumberprogress
//			statementProgress = connection.prepareStatement("INSERT INTO autogenbnumberprogress"
//					+ "(batch_id, sms_count, sent_count, last_update_datetime, client_id) VALUES (?, ?, ?, ?, ?)");
        } catch(Exception e) {
        	e.printStackTrace();
    		LoggingPooler.doLog(logger, "INFO", "FixANumberProcessor", "FixANumberProcessor", true, false, false, "", 
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
	
	private int getGSM7MessageLength(String message){
		int length = 300;
		
		int count = 0;
		for(int x = 0; x < message.length(); x++){
			String theChar = message.substring(x, x + 1); 
			
			if(theChar.equals("[")){
				count = count + 2;
			} else if(theChar.equals("]")){
				count = count + 2;
			} else if(theChar.equals("^")){
				count = count + 2;
			} else if(theChar.equals("~")){
				count = count + 2;
			} else if(theChar.equals("{")){
				count = count + 2;
			} else if(theChar.equals("}")){
				count = count + 2;
			} else if(theChar.equals("€")){
				count = count + 2;
			} else if(theChar.equals("/")){
				count = count + 2;
			} else if(theChar.equals("\\")){
				count = count + 2;
			} else {
				count = count + 1;
			}
		}
		
		if(count > 0){
			length = count;
		}
		
		return length;
	}
	
	private int getSmsCount(String message, String encoding){
		if(encoding.trim().equals("GSM7")){
			if(getGSM7MessageLength(message) <= 160){
				return 1;
			} else {
				return (int) Math.ceil(getGSM7MessageLength(message) / 153.00);
			}
		} else if(encoding.trim().equals("UCS2")) {
			if(message.length() <= 70){
				return 1;
			} else {
				return (int) Math.ceil(message.length() / 67.00);
			}
		} else {
			return (int) Math.ceil(message.length() / 67.00);
		}
	}
	
	private void updateIsProcessed(String batchId){
		try{			
			statementIsProcessed.setString(1, batchId);
			statementIsProcessed.executeUpdate();
			
			LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "updateIsProcessed", false, false, false,"", 
					"Succesfully update batchId " + batchId + " ALREADY PROCESSED TO TRUE.", null);	
		} catch (Exception e) {
			e.printStackTrace();
			
			LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "updateIsProcessed", true, false, false,"", 
					"Failed to update batchId " + batchId + " ALREADY PROCESSED TO TRUE.", e);				
		}
	}
	
	private boolean isGSM7(String theText){
		boolean isGSM = false;
		
		String theRegex = "^[A-Za-z0-9 \\r\\n@£$¥èéùìòÇØøÅå\u0394_\u03A6\u0393\u039B\u03A9\u03A0\u03A8\u03A3\u0398\u039EÆæßÉ!\"#$%&'()*+,\\-./:;<=>?¡ÄÖÑÜ§¿äöñüà^{}\\\\\\[~\\]|\u20AC]*$";
		Pattern pattern = Pattern.compile(theRegex);
		
		Matcher matcher = pattern.matcher(theText);
		
		isGSM = matcher.matches();
		
		return isGSM;
	}
	
	private int doProcessTheBatch(String clientId, String batchId, String theSMS, String messageEncoding, String username, String fileName, 
			int recordCount, String clientSenderIdId, String clientSenderId, String queueMessage, LocalDateTime sendingSchedule, String function){
		int result = 0;
		LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "processQueueBatch", false, false, false, "", 
				"doProcessTheBatch - clientId: " + clientId + ", batchId: " + batchId + ", function: " + function, null);	

		// Initiate formatter
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMddHHmmssSSS");

		// Start Date Time
		LocalDateTime startDateTime = LocalDateTime.now();

		try{
			// Open file for the batch
			String completeFileName = batchFileDirectory + fileName.trim();
			
			BufferedReader br = new BufferedReader(new FileReader(completeFileName));
			String line = "";
			int count = 0;
			
			while((line = br.readLine()) != null){
				// Clean up space, dash, +, ( and )
				String bNumber = line.trim();
				bNumber = bNumber.replace(" ", "");
				bNumber = bNumber.replace("-", "");
				bNumber = bNumber.replace("+", "");
				bNumber = bNumber.replace("(", "");
				bNumber = bNumber.replace(")", "");

				System.out.println("Line " + count + ": " + bNumber);

				// Map bNumber with aNumber
				LocalDateTime now = LocalDateTime.now();				

				// Set messageId
				String messageId = tool.generateUniqueID();
				LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "processQueueBatch", false, false, false, messageId, 
						"Assigning messageId: " + messageId + " for message to bNumber: " + bNumber + ", content: " + theSMS, null);	
				
				// Check sAchedule
				// Set errorCode
				String errorCode = "003"; // MESSAGE IS BEING PROCESSED
				LocalDateTime theNowTime = LocalDateTime.now();
				
				LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "processQueueBatch", false, false, false, messageId, 
						"theNowTime: " + theNowTime.format(dtfNormal) + ", schedule to send: " + sendingSchedule.format(dtfNormal), null);	

				if (theNowTime.isAfter(sendingSchedule)) {
					LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "processQueueBatch", false, false, false, messageId, 
							"theNowTime is after the schedule to send - DO SEND NOW! -> errorCode: 003", null);	

					errorCode = "003";

					// Update status is_processed = true
					updateIsProcessed(batchId);					
				} else {
					errorCode = "004"; // WAITING FOR THE SCHEDULE
					
					LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "processQueueBatch", false, false, false, messageId, 
							"theNowTime is before the schedule to send -> errorCode: 004", null);	
				}
				LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "processQueueBatch", false, false, false, messageId, 
						"errorCode: " + errorCode, null);	

				// Reconfirm messageEncoding
				boolean isGSM7 = isGSM7(theSMS);
				if(isGSM7 == false){
					messageEncoding = "UCS2";
				}

				// Check countryCode and Prefix
				JSONObject jsonPrefix = isPrefixValid(bNumber);

				String countryCode = "";
				String prefix = "";
				String telecomId = "";
				if(jsonPrefix.getBoolean("isValid") == true){
					countryCode = jsonPrefix.getString("countryCode");
					prefix = jsonPrefix.getString("prefix");
					telecomId = jsonPrefix.getString("telecomId");
				}

				// Get clientPrice and clientCurrency
				String routeId =  clientSenderIdId + "-" + username + "-" + telecomId;

				if(RouteSMSPooler.jsonRouteSMSProperty.has(routeId)){
					LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "processQueueBatch", false, false, false, messageId, 
							"jsonRouteSMSProperty has routeId: " + routeId, null);	

					JSONObject jsonRoute = RouteSMSPooler.jsonRouteSMSProperty.getJSONObject(routeId);

					// Get client price from jsonRoute
					double clientPricePerSubmit = 0.00000;
					String clientCurrency = "IDR";

					clientPricePerSubmit = jsonRoute.getDouble("clientPricePerSubmit");
					clientCurrency = clientPropertyPooler.getCurrencyId(clientId.trim());

					System.out.println("MessageEncoding: " + messageEncoding + ", Message length: " + theSMS.length() + ", SMS Count: " + getSmsCount(theSMS, messageEncoding) + ", function: " + function);

					// Save initial data before send to router
					// First time transaction insertion. Save initial data
					// Submit to Redis as initial data with expiry 7 days
					int expiry = 30 * 24 * 60 * 60;
					JSONObject jsonRedis = new JSONObject();
					jsonRedis.put("messageId", messageId);
					jsonRedis.put("receiverDateTime", now.format(formatter));
					jsonRedis.put("transactionDateTime", now.format(formatter));
					jsonRedis.put("msisdn", bNumber);
					jsonRedis.put("message", theSMS);
					jsonRedis.put("telecomId", telecomId);
					jsonRedis.put("countryCode", countryCode);
					jsonRedis.put("prefix", prefix);
					jsonRedis.put("errorCode", errorCode);
					jsonRedis.put("apiUserName", username);
					jsonRedis.put("clientSenderIdId", clientSenderIdId);
					jsonRedis.put("clientSenderId", clientSenderId);
					jsonRedis.put("clientId", clientId);
					jsonRedis.put("clientIpAddress", "");
					jsonRedis.put("receiverType", smsChannel); // AUTOGENAUPLOADER, SMPP and HTTP only

					String redisKey = "trxdata-" + messageId.trim();
					String redisVal = jsonRedis.toString();

					redisPooler.redisSetWithExpiry(redisCommand, redisKey, redisVal, expiry);
					LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "processQueueBatch", false, false, false, messageId, 
							"Successfully save Initial Data to Database and Redis. jsonRedis: " + jsonRedis.toString(), null);

					smsTransactionOperationPooler.saveInitialSMPPData(messageId, now, batchId, "AUTOGENAUPLOADER: " + queueMessage, "", 
							"", now, now, bNumber, theSMS, countryCode, prefix, telecomId, errorCode, 
							"AUTOGENAUPLOADER", clientSenderIdId, clientSenderId, clientId, 
							username, clientPricePerSubmit, clientCurrency, messageEncoding, getGSM7MessageLength(theSMS), 
							getSmsCount(theSMS, messageEncoding));

					LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "processQueueBatch", false, false, false, messageId, 
							"This far errorCode: " + errorCode, null);	

					if (errorCode.trim().equals("003")) {
						// Langsung dikirim
						// Send to INCOMING MESSAGE queue
						JSONObject jsonIncoming = new JSONObject();

						jsonIncoming.put("messageId", messageId);
						jsonIncoming.put("apiUserName", username);
						jsonIncoming.put("msisdn", bNumber);
						jsonIncoming.put("message", theSMS);
						jsonIncoming.put("clientId", clientId);
						jsonIncoming.put("telecomId", telecomId);
						jsonIncoming.put("clientSenderId", clientSenderId);
						jsonIncoming.put("smsChannel",smsChannel);
						jsonIncoming.put("messageLength", getGSM7MessageLength(theSMS));
						jsonIncoming.put("messageCount", getSmsCount(theSMS, messageEncoding));
						jsonIncoming.put("clientPricePerSubmit", String.format("%.5f", clientPricePerSubmit));

						// Publish jsonIncoming to queue INCOMING_SMS
						channelRabbit.queueDeclare(queueIncomingMessage, true, false, false, null);
						channelRabbit.basicPublish("", queueIncomingMessage, MessageProperties.PERSISTENT_TEXT_PLAIN, jsonIncoming.toString().getBytes());
						LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "processQueueBatch", false, false, false, messageId, 
								"jsonIncoming: " + jsonIncoming.toString() + " published SUCCESSfully!", null);
					} else if (errorCode.trim().equals("004")){
						// Jangan dikirim dulu, 
						// Submit table schedule_message
						statementScheduledMessage.setString(1, messageId);
						statementScheduledMessage.setString(2, bNumber);
						statementScheduledMessage.setString(3, theSMS);
						statementScheduledMessage.setString(4, clientId);
						statementScheduledMessage.setString(5, telecomId);
						statementScheduledMessage.setString(6, clientSenderId);
						statementScheduledMessage.setString(7, smsChannel);
						statementScheduledMessage.setString(8, username);
						statementScheduledMessage.setInt(9, getGSM7MessageLength(theSMS));
						statementScheduledMessage.setInt(10, getSmsCount(theSMS, messageEncoding));
						statementScheduledMessage.setDouble(11, clientPricePerSubmit);
						statementScheduledMessage.setObject(12, sendingSchedule);
						
						statementScheduledMessage.executeUpdate();

						LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "processQueueBatch", false, false, false, messageId, 
								"Still not due to schedule. Pending it for later sending", null);
					}

				} else {
					LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "processQueueBatch", false, false, false, messageId, 
							"Route is NOT DEFINED for batchId " + batchId + ", routeId " + routeId, null);

					if (function.trim().equals("executingschedule")) {
						// Jangan dikirim dulu, 
						LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "processQueueBatch", false, false, false, messageId, 
								"Do nothing, karena sudah error dari proses pertama.", null);
					} else {
						//Save initial db with status failed
						errorCode = "112";
						smsTransactionOperationPooler.saveInitialSMPPData(messageId, now, batchId, "AUTOGENAUPLOADER: " + queueMessage, "", 
								"", now, now, bNumber, theSMS, countryCode, prefix, telecomId, errorCode, 
								"AUTOGENAUPLOADER", clientSenderIdId, clientSenderId, clientId, 
								username, 0.00, "", messageEncoding, getGSM7MessageLength(theSMS), 
								getSmsCount(theSMS, messageEncoding));
					}
				}					

				count = count + 1;
			}
			
			// Save progress counter
			// batch_id, sms_count, sent_count, last_update_datetime
			//sLocalDateTime now = LocalDateTime.now();				

//			statementProgress.setString(1, batchId);
//			statementProgress.setInt(2, getSmsCount(theSMS, messageEncoding) * count);
//			statementProgress.setInt(3, 0);
//			statementProgress.setObject(4, now);
//			statementProgress.setString(5, clientId);
//			
//			statementProgress.executeUpdate();

			br.close();
			
			// End Date Time
			LocalDateTime endDateTime = LocalDateTime.now();
			
			// Calculate speed (transactions per second)
			long durationMilliSecond = Duration.between(startDateTime, endDateTime).toMillis();
			System.out.println("Traffic: " + Integer.toString(count) + ". Duration: " + String.valueOf(durationMilliSecond) + " milliseconds. TPS: " + String.valueOf((count*1000.000000000000000)/durationMilliSecond));
		} catch (Exception e) {
			e.printStackTrace();
		}		
		
		return result;
	}
	
	private void processQueueBatch(String queueMessage){
		try{			
			// Get batchId and clientId from queueMessage
			JSONObject jsonMessage = new JSONObject(queueMessage);
			
			String batchId = jsonMessage.getString("batchId");
			String clientId = jsonMessage.getString("clientId");
			String username = jsonMessage.getString("username");
			String strSendingSchedule = "";
			LocalDateTime ldtSendingSchedule = LocalDateTime.now();
					
			if (jsonMessage.has("sendingSchedule")) {
				strSendingSchedule = jsonMessage.getString("sendingSchedule");
				ldtSendingSchedule = LocalDateTime.parse(strSendingSchedule, dtfNormal);
			}
			
			String function = "";
			if (jsonMessage.has("function")) {
				function = jsonMessage.getString("function");
			}
			
			LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "processQueueBatch", false, false, false,"", 
					"Incoming data - batchId: " + batchId + ", clientId: " + clientId + ", username: " + username, null);	

			// Query table autogenbnumberuploaderdetail, get the batch which alerady_processed == false or null
			try{
				statement.setString(1, batchId);
				
				resultSet = statement.executeQuery();
				
				while(resultSet.next()){	
	            	// Get now localdatetime
					String fileName = resultSet.getString("b_number_filename");
					String theSMS = resultSet.getString("message");
					int recordCount = resultSet.getInt("no_of_records");
					String messageEncoding = resultSet.getString("message_encoding");
					String clientSenderIdId = resultSet.getString("sender_id_id");
					String clientSenderId = resultSet.getString("sender_id");
					boolean isProcessed = resultSet.getBoolean("is_processed");
					
					// Update status is_processed = true
					//updateIsProcessed(batchId);
					
					// Process the batch hanya untuk memastikan yang diproses yang isProcessed false
					if (isProcessed == false) {
						doProcessTheBatch(clientId, batchId, theSMS, messageEncoding, username, fileName, recordCount, clientSenderIdId, 
								clientSenderId, queueMessage, ldtSendingSchedule, function);
					} else {
						// Do not process for already processed
			    		LoggingPooler.doLog(logger, "INFO", "FixANumberProcessor", "processQueueBatch", false, false, false, "", 
			    				"Not processing " + batchId + " for is_processed is TRUE (already processed.).", null);
					}
				}				
			} catch (Exception e) {
				e.printStackTrace();
	    		LoggingPooler.doLog(logger, "INFO", "FixANumberProcessor", "processQueueBatch", true, false, false, "", 
	    				"Failed to process incoming instruction. Error occured.", e);
			} 
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private void readQueue(){
		// Listen to queue that contains information about which batchnumber to process - queuename: AUTOGEN_BATCH_INSTRUCT
		// Incoming message will be: {"batchId": "1928394830292", "clientId": "1029384"}
		try{
			channelRabbit.queueDeclare(queueBatchToProcess, true, false, false, null);			
			LoggingPooler.doLog(logger, "INFO", "FixANumberProcessor", "readQueue", false, false, true, "", 
					"Reading queue is ready, waiting for message ... ", null);
			
			Consumer consumer = new DefaultConsumer(channelRabbit) {
			      @Override
			      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
			          throws IOException {
			        String message = new String(body, "UTF-8");
			        
					try{
						LoggingPooler.doLog(logger, "INFO", "FixANumberProcessor", "readQueue", false, false, true, "", 
								"Receive message: " + message, null);

						// Process the message from queue
				        processQueueBatch(message);
					} finally {
						LoggingPooler.doLog(logger, "INFO", "FixANumberProcessor", "readQueue", false, false, true, "", 
								"Done processing message: " + message, null);

						channelRabbit.basicAck(envelope.getDeliveryTag(), false);
					}
			      }
			};

			boolean autoAck = false; // If not finally exectued well, no ack to rabbitmq, message not gone
			channelRabbit.basicQos(1); // Read one by one, so if other app FixANumberProcessor running, it can share load
			channelRabbit.basicConsume(queueBatchToProcess, autoAck, consumer);			
		} catch (Exception e){
			LoggingPooler.doLog(logger, "INFO", "FixANumberProcessor", "readQueue", true, false, false, "", 
					"Failed to access queue " + queueBatchToProcess, e);
		}
	}

	public static void main(String[] args) {
		System.out.println("Starting FixANumberProcessor ...");
		
		SMSFixANumberSenderIdProcessor processor = new SMSFixANumberSenderIdProcessor();
		processor.readQueue();
	}

}
