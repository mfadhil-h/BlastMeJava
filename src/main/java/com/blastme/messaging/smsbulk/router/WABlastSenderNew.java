package com.blastme.messaging.smsbulk.router;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Iterator;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import com.blastme.messaging.toolpooler.ClientPropertyPooler;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RedisPooler;
import com.blastme.messaging.toolpooler.RouteSMSPooler;
import com.blastme.messaging.toolpooler.SMSTransactionOperationPooler;
import com.blastme.messaging.toolpooler.TelecomPrefixPooler;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import io.lettuce.core.api.sync.RedisCommands;

@DisallowConcurrentExecution
public class WABlastSenderNew implements Job{
	private static String queueIncomingMessage = "WA_INCOMING";
	private static String smsChannel = "WABLAST_WEBUPLOADER";

    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMddHHmmssSSS");
    
	private static Logger logger;	
	
	private static RedisPooler redisPooler;
	private static RedisCommands<String, String> redisCommand;

	private PreparedStatement statementGetMesssageToSend = null;
	private PreparedStatement statementUpdateDetail = null;
    
    private SMSTransactionOperationPooler smsTransactionOperationPooler;
    private ClientPropertyPooler clientPropertyPooler;
    private Channel channelRabbitPublish;
    
	public WABlastSenderNew() {
		// Alert of initing
		System.out.println("INTIATING VOICE SENDER!!!");
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
	
	private void updateSetVoiceDetail(String messageId, String batchId, String msisdn, String sentStatus, String routeKey) {
		try {
			System.out.println("Updating status detail uploader messageId: " + messageId + ", sentStatus: " + sentStatus);
			statementUpdateDetail.setObject(1, LocalDateTime.now());
			statementUpdateDetail.setString(2, sentStatus);
			statementUpdateDetail.setString(3, routeKey);
			statementUpdateDetail.setString(4, batchId);
			statementUpdateDetail.setString(5, msisdn);
			
    		LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastSender", "updateSetVoiceDetail", false, false, false, messageId, 
    				"Update voice uploader detail - query: " + statementUpdateDetail.toString(), null);

    		int impacted = statementUpdateDetail.executeUpdate();
			
    		LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastSender", "updateSetVoiceDetail", false, false, false, messageId, 
    				"Successfully update voice uploader detail - sent datetime and status. Impacted row: " + impacted, null);
		} catch (Exception e) {
			e.printStackTrace();

    		LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastSender", "updateSetVoiceDetail", true, false, false, messageId, 
    				"Failed to update voice uploader detail. Error occured.", e);
		}
	}
	
	private void doSending() {
		try {
			System.out.println("Checking message to send using query: " + statementGetMesssageToSend.toString());
			
			// Do sending - get messages to send
			ResultSet resultSet = statementGetMesssageToSend.executeQuery();
						
			int count = 0;
			while (resultSet.next()) {
				String messageId = resultSet.getString("message_id");
				String batchId = resultSet.getString("batch_id");
				String msisdn = resultSet.getString("msisdn");
				String message = resultSet.getString("message");
				LocalDateTime sendingSchedule = resultSet.getObject("sending_schedule", LocalDateTime.class);
				String clientSenderIdId = resultSet.getString("client_sender_id_id");
				String clientSenderId = resultSet.getString("sender_id");
				String username = resultSet.getString("processed_username");
				String clientId = resultSet.getString("client_id");
				
				count = count + 1;
	    		LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastSender", "doSending", false, false, false, "", 
	    				count + ". Sending batchId: " + batchId + ", clientId: " + clientId + ", userName: " + username + 
	    				" -> msisdn: " + msisdn + ", clientSenderIdId: " + clientSenderIdId + ", voiceMessage: " + message, null);

				// If sendingSchedule < now, do send
				if (LocalDateTime.now().isAfter(sendingSchedule)) {
		    		// send the message
		    		
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

					int messageLength = message.length();
					int messageCount = (int) Math.ceil(message.length()/200);

					// Get clientPrice and clientCurrency
					String routeId =  clientSenderIdId + "-" + username + "-" + telecomId;

					if(RouteSMSPooler.jsonRouteSMSProperty.has(routeId)){
						LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastSender", "doSending", false, false, false, messageId, 
								"jsonRouteSMSProperty has routeId: " + routeId, null);	

						// Updaate data detail voice uploader
						updateSetVoiceDetail(messageId, batchId, msisdn, "002", routeId); // status pending

						JSONObject jsonRoute = RouteSMSPooler.jsonRouteSMSProperty.getJSONObject(routeId);

						// Get client price from jsonRoute
						double clientPricePerSubmit = 0.00000;
						String clientCurrency = "IDR";

						clientPricePerSubmit = jsonRoute.getDouble("clientPricePerSubmit") * messageCount;
						clientCurrency = clientPropertyPooler.getCurrencyId(clientId.trim());
						
						// Save initial data before send to router
						LocalDateTime now = LocalDateTime.now();
						String errorCode = "003"; // Default BEING PROCESSED
						String trxStatusCurrent = smsTransactionOperationPooler.getTransactionStatus(messageId);
						
						System.out.println("trxStatusCurrent: " + trxStatusCurrent + ", errorCode Processed: " + errorCode);
						if (trxStatusCurrent.trim().length() == 0 || trxStatusCurrent.trim().equals(errorCode)) {	
							System.out.println("XXXXXXXXXXXXXXXXXXXXXXXX");
							try {
								smsTransactionOperationPooler.saveInitialSMPPData(messageId, now, batchId, "WA_BROADCAST", "", 
										"", now, now, msisdn, message, countryCode, prefix, telecomId, errorCode, 
										"AUTOGENAUPLOADER", clientSenderIdId, clientSenderId, clientId, 
										username, clientPricePerSubmit, clientCurrency, "WA", messageLength, messageCount);
							} catch (Exception ex) {
								ex.printStackTrace();
								LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastSender", "doSending", true, false, false, messageId, 
										"Failed to initial data for execption.", ex);								
							}

							System.out.println("UUUUUUUUUUUUU");
							// Submit to Redis as initial data with expiry 7 days
							int expiry = 7 * 24 * 60 * 60;
							JSONObject jsonRedis = new JSONObject();
							jsonRedis.put("messageId", messageId);
							jsonRedis.put("receiverDateTime", now.format(formatter));
							jsonRedis.put("transactionDateTime", now.format(formatter));
							jsonRedis.put("msisdn", msisdn);
							jsonRedis.put("message", message);
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
							System.out.println("XXXXXXXXXXXXXX");
							
							redisPooler.redisSetWithExpiry(redisCommand, redisKey, redisVal, expiry);
							LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastSender", "doSending", false, false, false, messageId, 
									"Successfully save Initial Data to Database and Redis.", null);

							// Send to INCOMING MESSAGE queue
							JSONObject jsonIncoming = new JSONObject();

							jsonIncoming.put("messageId", messageId);
							jsonIncoming.put("apiUserName", username);
							jsonIncoming.put("msisdn", msisdn);
							jsonIncoming.put("message", message);
							jsonIncoming.put("clientId", clientId);
							jsonIncoming.put("telecomId", telecomId);
							jsonIncoming.put("clientSenderId", clientSenderId);
							jsonIncoming.put("smsChannel",smsChannel);
							jsonIncoming.put("messageLength", messageLength);
							jsonIncoming.put("messageCount", messageCount);
							jsonIncoming.put("clientPricePerSubmit", String.format("%.5f", clientPricePerSubmit));
							System.out.println("UUUUUUUUUUUUUHIHIHI");
							// Publish jsonIncoming to queue INCOMING_SMS
							channelRabbitPublish.queueDeclare(queueIncomingMessage, true, false, false, null);
							channelRabbitPublish.basicPublish("", queueIncomingMessage, MessageProperties.PERSISTENT_TEXT_PLAIN, jsonIncoming.toString().getBytes());
							LoggingPooler.doLog(logger, "DEBUG", "VoiceBlastSender", "doSending", false, false, false, messageId, 
									"jsonIncoming: " + jsonIncoming.toString() + " published SUCCESSfully!", null);
						} else {
							System.out.println("Skip sending message due to being processed in engine.");
						}

					} else {
						LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "processQueueBatch", false, false, false, messageId, 
								"Route is NOT DEFINED for batchId " + batchId + ", routeId " + routeId, null);

						// Updaate data detail voice uploader
						updateSetVoiceDetail(messageId, batchId, msisdn, "121", "");

						//Save initial db with status failed
						String errorCode = "112";
						LocalDateTime now = LocalDateTime.now();
						smsTransactionOperationPooler.saveInitialSMPPData(messageId, now, batchId, "WA_BROADCAST", "", 
								"", now, now, msisdn, message, countryCode, prefix, telecomId, errorCode, 
								"AUTOGENAUPLOADER", clientSenderIdId, clientSenderId, clientId, 
								username, 0.00, "", "WA", messageLength, messageCount);
					}					
				} else {
					// Belum saatnya sending message
					// Do nothing, just logging
					LoggingPooler.doLog(logger, "DEBUG", "FixANumberProcessor", "processQueueBatch", false, false, false, "", 
							"Not sending it now. Still not in the schedule.", null);					
				}
			}
			
			resultSet.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		System.out.println(new Date() + ". Sending message job!");
		
		JobDataMap data = context.getJobDetail().getJobDataMap();
		
		logger = (Logger) data.get("logger");

		redisPooler = (RedisPooler) data.get("redisPooler");
		redisCommand = (RedisCommands<String, String>) data.get("redisCommand");
		
		//connection = (Connection) data.get("jdbcConnection");
		statementGetMesssageToSend = (PreparedStatement) data.get("statementGetMesssageToSend");
		//statementReadDetail = (PreparedStatement) data.get("statementReadDetail");
		statementUpdateDetail = (PreparedStatement) data.get("statementUpdateDetail");
		
		smsTransactionOperationPooler = (SMSTransactionOperationPooler) data.get("smsTransactionOperationPooler");
		clientPropertyPooler = (ClientPropertyPooler) data.get("clientPropertyPooler");
		
		//connectionRabbit = (com.rabbitmq.client.Connection) data.get("rabbitConnection");
		//channelRabbitConsume = (Channel) data.get("channelRabbitConsume");
		channelRabbitPublish = (Channel) data.get("channelRabbitPublish");
		
		// Sending
		doSending();
	}	
}
