package com.blastme.messaging.callback;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.TimeZone;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.json.JSONObject;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.ClientBalancePooler;
import com.blastme.messaging.toolpooler.ClientPropertyPooler;
import com.blastme.messaging.toolpooler.DataSource;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.RedisPooler;
import com.blastme.messaging.toolpooler.RouteSMSPooler;
import com.blastme.messaging.toolpooler.SMSTransactionOperationPooler;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import io.lettuce.core.api.sync.RedisCommands;

public class TelinCallbackProcessor {
	private static Logger logger;	
	//private Tool tool;
	
	private static RedisPooler redisPooler;
	private static RedisCommands<String, String> redisCommand;

    private RabbitMQPooler rabbitMqPooler;
    private com.rabbitmq.client.Connection connectionRabbit;
    private Channel channelRabbitConsume;
    private String queueName = "TELINCALLBACK";

	private Connection connection = null;
	private PreparedStatement pstmtUpdateTrxStatus = null;
	private PreparedStatement pstmtGetRouteKey = null;
	private PreparedStatement pstmtUpdateVoiceDetail = null;
	
	private ClientBalancePooler clientBalancePooler;	
	private ClientPropertyPooler clientPropertyPooler;
	private SMSTransactionOperationPooler smsTransactionOperationPooler;
	
	public TelinCallbackProcessor() {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("TELIN_CALLBACK_PROCESSOR");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate tool
		//tool = new Tool();
		
		// Initiate RouteSMSPooler
		new RouteSMSPooler();
	
		try {
    		// Initiate RedisPooler
    		redisPooler = new RedisPooler();
    		redisCommand = redisPooler.redisInitiateConnection();
    		
    		// Initiate RabbitMQPooler
    		rabbitMqPooler = new RabbitMQPooler();	
    		connectionRabbit = rabbitMqPooler.getConnection();
    		
    		// Channel rabbit for consuming from VOICE_BLAST_SEND_BATCHID
    		channelRabbitConsume = rabbitMqPooler.getChannel(connectionRabbit);
    		
    		// Initiate connection to Postgresql
            BasicDataSource bds = DataSource.getInstance().getBds();
            connection = bds.getConnection();
            
            pstmtUpdateTrxStatus = connection.prepareStatement("update transaction_sms set status_code = ?, "
            		+ "client_price_per_unit = client_price_per_unit + ?, client_price_total = client_price_total + ?, call_duration = ? "
            		+ "where message_id = ?");
            
            pstmtGetRouteKey = connection.prepareStatement("select route_key from voice_blast_upload_detail where message_id = ?");
            
            pstmtUpdateVoiceDetail = connection.prepareStatement("update voice_blast_upload_detail set delivery_datetime=?, delivery_status=? where message_id=?");
            
            // Initiate clientBalancePooler
            clientBalancePooler = new ClientBalancePooler();
            
            // Initiate clientPropertyPooler
            clientPropertyPooler = new ClientPropertyPooler();
            
            // Initiate smsTransactionOperationPooler
            smsTransactionOperationPooler = new SMSTransactionOperationPooler();
            
    		LoggingPooler.doLog(logger, "INFO", "TelinCallbackProcessor", "CallbackProcessor", false, false, false, "", 
    				"Application CallbackProcessor is loaded and run.", null);
		} catch (Exception e) {
			e.printStackTrace();
			
    		LoggingPooler.doLog(logger, "INFO", "TelinCallbackProcessor", "CallbackProcessor", true, false, false, "", 
    				"Failed to load application CallbackProcessor. Error occured.", e);
    		
    		System.exit(-1);
			
		}
	}
	
	private String getMessageIdByCallSid(String callSid) {
		String messageId = "";
		
		try {
			String redisKey = "mapCallSidToMsgId-" + callSid;
			messageId = redisPooler.redisGet(redisCommand, redisKey);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		LoggingPooler.doLog(logger, "DEBUG", "TelinCallbackProcessor", "getMessageIdByCallSid", false, false, false, "", 
				"CallSid: " + callSid + " -> messageId: " + messageId, null);
		
		return messageId;
	}
	
	private void updateTransactionStatus(String messageId, String statusCode, double clientPricePerDelivery, double totalClientPricePerDelivery, int callDuration) {
		try {
			pstmtUpdateTrxStatus.setString(1, statusCode);
			pstmtUpdateTrxStatus.setDouble(2, clientPricePerDelivery);
			pstmtUpdateTrxStatus.setDouble(3, totalClientPricePerDelivery);
			pstmtUpdateTrxStatus.setInt(4, callDuration);
			pstmtUpdateTrxStatus.setString(5, messageId);
			
			int impactRow = pstmtUpdateTrxStatus.executeUpdate();
			
			LoggingPooler.doLog(logger, "DEBUG", "TelinCallbackProcessor", "updateTransactionStatus", false, false, false, messageId, 
					"Updating transaction data impactRow: " + impactRow, null);
			
			pstmtUpdateVoiceDetail.setObject(1, LocalDateTime.now());
			pstmtUpdateVoiceDetail.setString(2, statusCode);
			pstmtUpdateVoiceDetail.setString(3, messageId);
			
			int impactRow1 = pstmtUpdateVoiceDetail.executeUpdate();
			
			LoggingPooler.doLog(logger, "DEBUG", "TelinCallbackProcessor", "updateTransactionStatus", false, false, false, messageId, 
					"Updating voice upload data impactRow1: " + impactRow1, null);
		} catch (Exception e) {
			e.printStackTrace();

			LoggingPooler.doLog(logger, "DEBUG", "TelinCallbackProcessor", "updateTransactionStatus", true, false, false, messageId, 
					"Failed to update transaction. Error occured.", e);			
		}
	}
	
	private void processQueueData(String message) {
		try {
			// message:
			// {"incomingMessage":"InstanceId=ID711d15db3c5b4b7aa4dcc12068b67640&AccountSid=ACd60c3ce0c042d3588ff1597731ec4830&CallSid=ID711d15db3c5b4b7aa4dcc12068b67640-CA4d2c0060c659435ebcc7b4ddf79277b1&From=622180602341&To=628111253636&Direction=outbound-api&CallerName=622180602341&ForwardedFrom&CallStatus=ringing&Timestamp=Sun%2C+09+Feb+2020+09%3A30%3A03+Etc%2FUTC&CallbackSource=call-progress-events&SequenceNumber=1",
			// "callbackPath":"voiceotp",
			// "remoteIpAddress":"18.182.224.98",
			// "incomingDateTime":"2020-02-09 16:30:14.720"}
			
			LoggingPooler.doLog(logger, "DEBUG", "TelinCallbackProcessor", "processQueueData", false, false, false, "", 
					"Queue incoming message: " + message, null);			
			JSONObject jsonQueue = new JSONObject(message);
			
			String incomingMessage = jsonQueue.getString("incomingMessage").trim();
			String callbackPath = jsonQueue.getString("callbackPath").trim();
			//String remoteIpAddress = jsonQueue.getString("remoteIpAddress").trim();
			LoggingPooler.doLog(logger, "DEBUG", "TelinCallbackProcessor", "processQueueData", false, false, false, "", 
					"callbackPath: " + callbackPath, null);			
						
			if (callbackPath.trim().toLowerCase().equals("voiceotp")) {
				// Process response for voice otp
				String callSid = "";
				String callStatus = "ringing";
				int callDuration = 0;
				
				String blastMeStatus = "006";
				
				// Split incomingMessage by &
				String[] splittedIncMessage = incomingMessage.split("&");
				System.out.println("splittedIncMessage: " + Arrays.toString(splittedIncMessage));
				
				for (int x = 0; x < splittedIncMessage.length; x++) {
					System.out.print("x: " + x);
					
					if (splittedIncMessage[x].contains("CallStatus")) {
						String tempMessage = splittedIncMessage[x];
						System.out.println("Check CallStatus - tempMessage: " + tempMessage);
						
						// Split tempMessage
						String[] splittedTempMsg = tempMessage.split("=");
						if (splittedTempMsg.length == 2) {
							callStatus = splittedTempMsg[1];
						}
						System.out.println("CallStatus: " + callStatus);
					} else if (splittedIncMessage[x].contains("CallSid")) {
						String tempMessage = splittedIncMessage[x];
						System.out.println("Check CallSid - tempMessage: " + tempMessage);

						// Split tempMessage
						String[] splittedTempMsg = tempMessage.split("=");
						if (splittedTempMsg.length == 2) {
							callSid = splittedTempMsg[1];
						}
						System.out.println("CallSid: " + callSid);
					} else if (splittedIncMessage[x].contains("CallDuration")) {
						String tempMessage = splittedIncMessage[x];
						System.out.println("Check CallDuration - tempMessage: " + tempMessage);

						// Split tempMessage
						String[] splittedTempMsg = tempMessage.split("=");
						if (splittedTempMsg.length == 2) {
							callDuration = Integer.parseInt(splittedTempMsg[1]);
						}
						System.out.println("callDuration: " + callDuration);
					}
				}
				
				// Olah data
				if (callStatus.trim().equals("ringing")) {
					blastMeStatus = "006";
				} else if (callStatus.trim().equals("answered")) {
					blastMeStatus = "007";
				} else if (callStatus.trim().equals("completed")) {
					if (callDuration == 0) {
						blastMeStatus = "101"; // NOT ANSWERED/REJECTED
					} else {
						blastMeStatus = "000"; // ANSWERED
					}
				}
				
				// Get messageId from callSid
				String messageId = getMessageIdByCallSid(callSid);
				LoggingPooler.doLog(logger, "DEBUG", "TelinCallbackProcessor", "processQueueData", false, false, false, messageId, 
						"Incoming callSid: " + callSid + " -> messageId: " + messageId, null);			

				
				// Get cost client price per delivery --
				// Get routeId from voice_blast_upload_detail by messageId -> will be used for client price per delivery
				pstmtGetRouteKey.setString(1, messageId);
				
				ResultSet rs = pstmtGetRouteKey.executeQuery();
				
				String routeId = "";
				while (rs.next()) {
					routeId = rs.getString("route_key");
				}
				LoggingPooler.doLog(logger, "DEBUG", "TelinCallbackProcessor", "processQueueData", false, false, false, messageId, 
						"callSid: " + callSid + " -> routeId: " + routeId, null);			

				LoggingPooler.doLog(logger, "DEBUG", "TelinCallbackProcessor", "processQueueData", false, false, false, messageId, 
						"Voice call status - blastmeStatus: " + blastMeStatus, null);			
				if (blastMeStatus.trim().equals("000")) {
					LoggingPooler.doLog(logger, "DEBUG", "TelinCallbackProcessor", "processQueueData", false, false, false, messageId, 
							"blastmeStatus: " + blastMeStatus + " -> DEDUCT per SECONDS UNIT usage!", null);			

					// Get call duration unit
					int durationSecondUnit = 30; // Default, dicharge per 30 detik
					// Get call price per duration unit
					double callPricePerDurationUnit = 0.00000;
					
					if (RouteSMSPooler.jsonRouteSMSProperty.has(routeId)) {
						try {
							LoggingPooler.doLog(logger, "DEBUG", "TelinCallbackProcessor", "processQueueData", false, false, false, messageId, 
									"jsonRouteSMSProperty has routeId: " + routeId + " -> jsonRouteProperty: " + RouteSMSPooler.jsonRouteSMSProperty.getJSONObject(routeId).toString(), null);			

							JSONObject jsonDetail = RouteSMSPooler.jsonRouteSMSProperty.getJSONObject(routeId);
							
							if (jsonDetail.has("voice_unit_second")) {
								durationSecondUnit = jsonDetail.getInt("voice_unit_second");
							}
							
							if (jsonDetail.has("voice_price_per_unit")) {
								callPricePerDurationUnit = jsonDetail.getDouble("voice_price_per_unit");
							}

						} catch (Exception e) {
							e.printStackTrace();
							LoggingPooler.doLog(logger, "DEBUG", "TelinCallbackProcessor", "processQueueData", true, false, false, messageId, 
									"Failed parsing jsonRoutingTable. Error occured.", e);			
						}
					}
					LoggingPooler.doLog(logger, "DEBUG", "TelinCallbackProcessor", "processQueueData", false, false, false, messageId, 
							"ROUTED - durationSecondUnit: " + durationSecondUnit + ", clientPricePerDurationUnit: " + String.format("%.2f", callPricePerDurationUnit), null);			

					// Calculate callDurationInUnit
					int callDurationInUnit = (int) Math.ceil((double) callDuration/(double) durationSecondUnit);
					LoggingPooler.doLog(logger, "DEBUG", "TelinCallbackProcessor", "processQueueData", false, false, false, messageId, 
							"call Duration: " + callDuration + ", durationSecondUnit: " + durationSecondUnit + ", callDurationInUnit: " + callDurationInUnit, null);			
					
					// Make it total as per minute duration
					double totalCallPrice = callDurationInUnit * callPricePerDurationUnit;
					LoggingPooler.doLog(logger, "DEBUG", "TelinCallbackProcessor", "processQueueData", false, false, false, messageId, 
							"totalCallPrice: " + String.format("%.5f", totalCallPrice), null);			
					
					// update transaction status
					updateTransactionStatus(messageId, blastMeStatus, callPricePerDurationUnit, totalCallPrice, callDuration);
					
					// update deduction
					String usageBy = "TELINVOICECB";
					String usageType = "USAGE";
					String usageDescription = "Voice Call Delivery Deduction.";
					
					String clientId = smsTransactionOperationPooler.getClientId(messageId);

					// Get business model
					String businessModel = clientPropertyPooler.getBusinessMode(clientId);

					//JSONObject jsonDeduction = clientBalancePooler.deductClientBalance(messageId, clientId, usageType, usageValue, usageBy, usageDescription, businessModel, routedVendorId, vendorPrice, vendorCurrency, usageIdr, vendorPriceIdr, marginIdr, exchageRateToIdr)
					JSONObject jsonDeduction = clientBalancePooler.deductClientBalance(messageId, clientId, usageType, totalCallPrice, 
							usageBy, usageDescription, businessModel);
					LoggingPooler.doLog(logger, "DEBUG", "SMSRouter", "processQueueData", false, false, false, messageId, 
							"Deducting client: " + clientId + ", usageType: " + usageType + ", deduction: " + String.format("%.5f", totalCallPrice), null);
					
					if (jsonDeduction.getInt("status") == 0) {
						LoggingPooler.doLog(logger, "DEBUG", "SMSRouter", "processQueueData", false, false, false, messageId, 
								"Deducting balance is SUCCESS.", null);
					} else {
						LoggingPooler.doLog(logger, "DEBUG", "SMSRouter", "processQueueData", false, false, false, messageId, 
								"FAILED TO DEDUCT. BALANCE NOT ENOUGH", null);
					}
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void readQueueCallback() {
		try{
			channelRabbitConsume.queueDeclare(queueName, true, false, false, null);
			channelRabbitConsume.basicQos(10);
			
			LoggingPooler.doLog(logger, "INFO", "TelinCallbackProcessor", "readQueueCallback", false, false, true, "", 
					"Reading queue " + queueName + " is ready, waiting for message ... ", null);
			
			Consumer consumer = new DefaultConsumer(channelRabbitConsume) {
			      @Override
			      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
			          throws IOException {
			        String message = new String(body, "UTF-8");
			        
					try{
						LoggingPooler.doLog(logger, "INFO", "TelinCallbackProcessor", "readQueueCallback", false, false, true, "", 
								"Receive message: " + message, null);

						// Process the message from queue
				        processQueueData(message);
					} finally {
						LoggingPooler.doLog(logger, "INFO", "TelinCallbackProcessor", "readQueueCallback", false, false, true, "", 
								"Done processing message: " + message, null);

						channelRabbitConsume.basicAck(envelope.getDeliveryTag(), false);
					}
			      }
			};

			boolean autoAck = false; // If not finally exectued well, no ack to rabbitmq, message not gone
			channelRabbitConsume.basicConsume(queueName, autoAck, consumer);
		} catch (Exception e){
			LoggingPooler.doLog(logger, "INFO", "CallbackProcessor", "readQueueCallback", true, false, false, "", 
					"Failed to access queue " + queueName, e);
		}
	}

	public static void main(String[] args) {
		TelinCallbackProcessor processor = new TelinCallbackProcessor();
		processor.readQueueCallback();
	}

}
