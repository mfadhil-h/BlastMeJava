package com.blastme.messaging.smsbulk.router;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Random;
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
import com.blastme.messaging.toolpooler.SMPPDLRPooler;
import com.blastme.messaging.toolpooler.SMSTransactionOperationPooler;
import com.blastme.messaging.toolpooler.Tool;
import com.blastme.messaging.toolpooler.TransactionRedisPooler;
import com.blastme.messaging.toolpooler.VendorSMSPooler;
import com.blastme.messaging.toolpooler.VendorSMSSenderIdPooler;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;

import io.lettuce.core.api.sync.RedisCommands;

public class SMSRouter {
	private static Logger logger;	
	
	private TransactionRedisPooler transactionRedisPooler;
	
	private ClientBalancePooler clientBalancePooler;
    private SMSTransactionOperationPooler smsTransactionOperationPooler;
    private ClientPropertyPooler clientPropertyPooler;
    private SMPPDLRPooler smppDlrPooler;
    
    private RabbitMQPooler rabbitMqPooler;
    private com.rabbitmq.client.Connection connectionRabbitConsume;
    private com.rabbitmq.client.Connection connectionRabbitPublish;
    
    private Channel channelRabbitConsume;
    private Channel channelRabbitPublish;

	private static String QueueName;
	private static String loggingName = "ROUTER-" + QueueName;
	private Random rand = new Random();

	private Connection connection;
	private PreparedStatement statementUpdateStatus;
	//private PreparedStatement statementTelecomTrafficLimit, statementTelecomTrafficLimitSearch;
	//private PreparedStatement statementApiUserNamePrefixTrafficLimit, statementApiUsernmaePrefixTrafficLimitSearch;
	
	private Tool tool;

	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMddHHmmssSSS");
	private DateTimeFormatter formatterDay = DateTimeFormatter.ofPattern("yyMMdd");
	
	private RedisPooler redisPooler;
	private RedisCommands<String, String> redisCommand;
	
	public SMSRouter() {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("SMS_ROUTER");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Intiaite tool
		tool = new Tool();
		
		// Initiate transactionRedisPooler
		transactionRedisPooler = new TransactionRedisPooler();
		
		// Initiate RabbitMQPooler
		rabbitMqPooler = new RabbitMQPooler();
		connectionRabbitConsume = rabbitMqPooler.getConnection();
		connectionRabbitPublish = rabbitMqPooler.getConnection();
		
		channelRabbitConsume = rabbitMqPooler.getChannel(connectionRabbitConsume);
		channelRabbitPublish = rabbitMqPooler.getChannel(connectionRabbitPublish);
		try {
			channelRabbitConsume.basicQos(50);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
		
		// Initiate VendorSMSPooler
		new VendorSMSPooler();
		
		// Initiate RouteSMSPooler
		new RouteSMSPooler();
		
		// Initiate VendorSMSSenderId
		new VendorSMSSenderIdPooler();
		
		// Initiate clientBalancePooler
		clientBalancePooler = new ClientBalancePooler();
		
		// Initiate smsTransactionOperationPooler
		smsTransactionOperationPooler = new SMSTransactionOperationPooler();
		
		// Initiate SMPPDLRPooler
		smppDlrPooler = new SMPPDLRPooler();
		
		// Initiate ClientPropertyPooler
		clientPropertyPooler = new ClientPropertyPooler();
		
		try{
			BasicDataSource bds = DataSource.getInstance().getBds();
	        connection = bds.getConnection();
	        
			statementUpdateStatus = connection.prepareStatement("update transaction_sms set status_code = ? where message_id = ?");
			
//			statementTelecomTrafficLimit = connection.prepareStatement("select telecom_id, max_traffic_day from telecom_daily_traffic_limit"); 
//			statementTelecomTrafficLimitSearch = connection.prepareStatement("select telecom_id, max_traffic_day from telecom_daily_traffic_limit where telecom_id = ?");
			
			//statementApiUserNamePrefixTrafficLimit = connection.prepareStatement("select apiusername_prefix, telecom_id, max_traffic_day from apiusernameprefix_telecom_daily_traffic_limit");
			//statementApiUsernmaePrefixTrafficLimitSearch = connection.prepareStatement("select apiusername_prefix, telecom_id, max_traffic_day from apiusernameprefix_telecom_daily_traffic_limit where apiusername_prefix = ? and telecom_id = ?");
			
			// Initiate RedisPooler
			redisPooler = new RedisPooler();
			redisCommand = redisPooler.redisInitiateConnection();
			
			// Initiate daily traffic limit per telecom
			//initiateDailyTelecomTrafficLimit();
			//initDailyClientTelecomTrafficLimit();

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		LoggingPooler.doLog(logger, "INFO", "SMSRouter", "SMSRouter", false, false, false, "", "Starting SMS Router ...", null);				
	}
	
//	private void initiateDailyTelecomTrafficLimit() {
//		ResultSet resultSet = null;
//		try {
//			resultSet = statementTelecomTrafficLimit.executeQuery();
//			
//			while (resultSet.next()) {
//				String telecomId = resultSet.getString("telecom_id");
//				int maxTraffic = resultSet.getInt("max_traffic_day");
//				
//				JSONObject jsonTrafficLimit = new JSONObject();
//				jsonTrafficLimit.put("telecomId", telecomId);
//				jsonTrafficLimit.put("maxTrafficDay", maxTraffic);
//				jsonTrafficLimit.put("theDay", LocalDateTime.now().format(formatterDay));
//				
//				// Put into redis
//				String redisKey = "telecomLimit-" + telecomId;
//				String redisVal = jsonTrafficLimit.toString();
//				
//				// Check apakah sudah ada value di sana dan theDay is today
//				String existinRedisVal = redisPooler.redisGet(redisCommand, redisKey);
//				LoggingPooler.doLog(logger, "INFO", loggingName, "sendMessageToQueue", false, false, false, "", 
//						"Existing telecom traffic limit today: " + existinRedisVal, null);			
//
//				if (existinRedisVal == null || existinRedisVal.trim().length() == 0) {
//					// Tidak ada data
//					LoggingPooler.doLog(logger, "INFO", loggingName, "sendMessageToQueue", false, false, false, "", 
//							"Existing telecom traffic limit today IS NOT FOUND. INITIATE ONE!", null);			
//					redisPooler.redisSet(redisCommand, redisKey, redisVal);
//				} else {
//					JSONObject jsonExistingRedisVal = new JSONObject(existinRedisVal);
//					
//					String existingTheDay = jsonExistingRedisVal.getString("theDay");
//					
//					if (existingTheDay.trim().equals(LocalDateTime.now().format(formatterDay))) {
//						// Valid utk hari ini, jd dibiarin aja						
//						LoggingPooler.doLog(logger, "INFO", loggingName, "sendMessageToQueue", false, false, false, "", 
//								"Existing telecom traffic limit today IS FOUND. DO NOT INITIATE ONE!", null);			
//					} else {
//						// Sudah gak valid utk hari ini. Update
//						LoggingPooler.doLog(logger, "INFO", loggingName, "sendMessageToQueue", false, false, false, "", 
//								"Existing telecom traffic limit today IS FOUND BUT EXPIRED. INITIATE NEW ONE.", null);			
//						redisPooler.redisSet(redisCommand, redisKey, redisVal);
//					}
//				}
//				
//			}
//			
//			
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				resultSet.close();
//			} catch (SQLException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//	}
	
//	private void initDailyClientTelecomTrafficLimit() {
//		ResultSet resultSet = null;
//		try {
//			//resultSet = statementApiUserNamePrefixTrafficLimit.executeQuery();
//			
//			while(resultSet.next()) {
//				String apiUserNamePrefix = resultSet.getString("apiusername_prefix").toLowerCase().trim();
//				String telecomId = resultSet.getString("telecom_id").trim();
//				int maxTraffic = resultSet.getInt("max_traffic_day");
//				
//				JSONObject jsonTrafficLimit = new JSONObject();
//				jsonTrafficLimit.put("apiUserNamePrefix", apiUserNamePrefix);
//				jsonTrafficLimit.put("telecomId", telecomId);
//				jsonTrafficLimit.put("maxTrafficDay", maxTraffic);
//				jsonTrafficLimit.put("theDay", LocalDateTime.now().format(formatterDay));
//
//				// Put into redis
//				String redisKey = "clientTelecomLimit-" + apiUserNamePrefix + "-" + telecomId;
//				String redisVal = jsonTrafficLimit.toString();
//				
//				// Check apakah sudah ada value di sana dan theDay is today
//				String existinRedisVal = redisPooler.redisGet(redisCommand, redisKey);
//				LoggingPooler.doLog(logger, "INFO", loggingName, "sendMessageToQueue", false, false, false, "", 
//						"Existing telecom traffic limit today: " + existinRedisVal, null);			
//
//				if (existinRedisVal == null || existinRedisVal.trim().length() == 0) {
//					// Tidak ada data
//					LoggingPooler.doLog(logger, "INFO", loggingName, "sendMessageToQueue", false, false, false, "", 
//							"Existing telecom traffic limit today IS NOT FOUND. INITIATE ONE!", null);			
//					redisPooler.redisSet(redisCommand, redisKey, redisVal);
//				} else {
//					JSONObject jsonExistingRedisVal = new JSONObject(existinRedisVal);
//					
//					String existingTheDay = jsonExistingRedisVal.getString("theDay");
//					
//					if (existingTheDay.trim().equals(LocalDateTime.now().format(formatterDay))) {
//						// Valid utk hari ini, jd dibiarin aja						
//						LoggingPooler.doLog(logger, "INFO", loggingName, "sendMessageToQueue", false, false, false, "", 
//								"Existing telecom traffic limit today IS FOUND. DO NOT INITIATE ONE!", null);			
//					} else {
//						// Sudah gak valid utk hari ini. Update
//						LoggingPooler.doLog(logger, "INFO", loggingName, "sendMessageToQueue", false, false, false, "", 
//								"Existing telecom traffic limit today IS FOUND BUT EXPIRED. INITIATE NEW ONE.", null);			
//						redisPooler.redisSet(redisCommand, redisKey, redisVal);
//					}
//				}				
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				resultSet.close();
//			} catch (SQLException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//	}
	
	private String isStillInApiUserNamePrefixTelecomTrafficLimit(String messageId, String apiUserName, String apiUserNamePrefix, String telecomId) {
		String isInLimit = "000";
		//int clpLimitTrafficPerDay = 111300000;  // HERE IS THE LIMIT MAX
		//int mitsLimitTrafficPerDay = 11150000;
		int zltLimitCMTrafficPerDay = 100000;
		int fzhLimitCMTrafficPerDay = 100000;
		//int ymitsLimitTrafficPerDay = 11150000;
		//int niniLimitTrafficPerDay = 111150000;
		
//		ResultSet rs = null;
		
		try {
			LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
					"apiUsernamePrefix: " + apiUserNamePrefix + ", telecomId: " + telecomId, null);			

			if (apiUserNamePrefix.toLowerCase().startsWith("z") && telecomId.equals("86002")) {
				// ZLT
				// Review dulu
				String redisKey = "zltCMLimit";
				String redisVal = redisPooler.redisGet(redisCommand, redisKey);
				LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
						"Checking traffic CM limit - redisKey: " + redisKey + ", redisVal: " + redisVal, null);			

				if (redisVal == null || redisVal.trim().length() == 0) {
					// RedisVal is empty. FIRST PROCESS OF THE DAY. SET IT UP!
					LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit - ZLT", false, false, false, messageId, 
							"First traffic of today -> redisVal empty. Set new fresh ZLT limit in REDIS: " + Integer.toString(zltLimitCMTrafficPerDay), null);			
					
					String theDay = LocalDateTime.now().format(formatterDay);
					int newLimitCounter = zltLimitCMTrafficPerDay;
					
					JSONObject jsonLimit = new JSONObject();
					jsonLimit.put("maxTrafficDay", newLimitCounter);
					jsonLimit.put("theDay", theDay);
					
					// Put back to redis
					redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
					
					isInLimit = "000";
				} else {
					JSONObject jsonLimit = new JSONObject(redisVal);
					
					int limitCounter = jsonLimit.getInt("maxTrafficDay");
					String limitDay = jsonLimit.getString("theDay");
					LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
							"limitCounter: " + Integer.toString(limitCounter) + ", limitDay: " + limitDay, null);			

					if (limitDay.trim().equals(LocalDateTime.now().format(formatterDay))) {
						if (limitCounter - 1 > 0) {
							limitCounter = limitCounter - 1;
							
							jsonLimit.put("maxTrafficDay", limitCounter);
							
							// Put back to redis
							redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
							LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
									"IS IN LIMIT. Set new maxTrafficDay: " + Integer.toString(limitCounter) + ", theDay: " + limitDay, null);			

							isInLimit = "000";
						} else {
							LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
									"IS NOT IN LIMIT. Now limitCounter: " + Integer.toString(limitCounter) + " -> status: 903", null);			

							isInLimit = "903"; // TRAFFIC LIMITATION
						}
					} else {
						// LIMIT COUNTER NOT SETUP YET IN REDIS. FIRST PROCESS OF THE DAY. SET IT UP!
						LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
								"First traffic of today. Set new fresh ZLT limit in REDIS: " + Integer.toString(zltLimitCMTrafficPerDay), null);			
						
						String theDay = LocalDateTime.now().format(formatterDay);
						int newLimitCounter = zltLimitCMTrafficPerDay;
						
						jsonLimit.put("maxTrafficDay", newLimitCounter);
						jsonLimit.put("theDay", theDay);
						
						// Put back to redis
						redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
						
						isInLimit = "000";
					}
				}
			} else if ((apiUserNamePrefix.toLowerCase().startsWith("t") || apiUserNamePrefix.toLowerCase().startsWith("o")) && !apiUserName.equals("T3stvnHtc") &&
					telecomId.equals("86002")) {
				// THK
				// Review dulu
				String redisKey = "thkCMLimit";
				String redisVal = redisPooler.redisGet(redisCommand, redisKey);
				LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
						"Checking traffic limit - redisKey: " + redisKey + ", redisVal: " + redisVal, null);			

				if (redisVal == null || redisVal.trim().length() == 0) {
					// RedisVal is empty. FIRST PROCESS OF THE DAY. SET IT UP!
					LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit - CLP", false, false, false, messageId, 
							"First traffic of today -> redisVal empty. Set new fresh THK limit in REDIS: " + Integer.toString(fzhLimitCMTrafficPerDay), null);			
					
					String theDay = LocalDateTime.now().format(formatterDay);
					int newLimitCounter = fzhLimitCMTrafficPerDay;
					
					JSONObject jsonLimit = new JSONObject();
					jsonLimit.put("maxTrafficDay", newLimitCounter);
					jsonLimit.put("theDay", theDay);
					
					// Put back to redis
					redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
					
					isInLimit = "000";
				} else {
					JSONObject jsonLimit = new JSONObject(redisVal);
					
					int limitCounter = jsonLimit.getInt("maxTrafficDay");
					String limitDay = jsonLimit.getString("theDay");
					LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
							"limitCounter: " + Integer.toString(limitCounter) + ", limitDay: " + limitDay, null);			

					if (limitDay.trim().equals(LocalDateTime.now().format(formatterDay))) {
						if (limitCounter - 1 > 0) {
							limitCounter = limitCounter - 1;
							
							jsonLimit.put("maxTrafficDay", limitCounter);
							
							// Put back to redis
							redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
							LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
									"IS IN LIMIT. Set new maxTrafficDay: " + Integer.toString(limitCounter) + ", theDay: " + limitDay, null);			

							isInLimit = "000";
						} else {
							LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
									"IS NOT IN LIMIT. Now limitCounter: " + Integer.toString(limitCounter) + " -> status: 903", null);			

							isInLimit = "903"; // TRAFFIC LIMITATION
						}
					} else {
						// LIMIT COUNTER NOT SETUP YET IN REDIS. FIRST PROCESS OF THE DAY. SET IT UP!
						LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
								"First traffic of today. Set new fresh THK limit in REDIS: " + Integer.toString(fzhLimitCMTrafficPerDay), null);			
						
						String theDay = LocalDateTime.now().format(formatterDay);
						int newLimitCounter = fzhLimitCMTrafficPerDay;
						
						jsonLimit.put("maxTrafficDay", newLimitCounter);
						jsonLimit.put("theDay", theDay);
						
						// Put back to redis
						redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
						
						isInLimit = "000";
					}
				}
			} 
			
//			if (apiUserNamePrefix.toLowerCase().startsWith("c") || apiUserName.toLowerCase().startsWith("j")) {
//				// CLP
//				// Review dulu
//				String redisKey = "clpLimit";
//				String redisVal = redisPooler.redisGet(redisCommand, redisKey);
//				LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//						"Checking traffic limit - redisKey: " + redisKey + ", redisVal: " + redisVal, null);			
//
//				if (redisVal == null || redisVal.trim().length() == 0) {
//					// RedisVal is empty. FIRST PROCESS OF THE DAY. SET IT UP!
//					LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit - CLP", false, false, false, messageId, 
//							"First traffic of today -> redisVal empty. Set new fresh CLP limit in REDIS: " + Integer.toString(clpLimitTrafficPerDay), null);			
//					
//					String theDay = LocalDateTime.now().format(formatterDay);
//					int newLimitCounter = clpLimitTrafficPerDay;
//					
//					JSONObject jsonLimit = new JSONObject();
//					jsonLimit.put("maxTrafficDay", newLimitCounter);
//					jsonLimit.put("theDay", theDay);
//					
//					// Put back to redis
//					redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
//					
//					isInLimit = "000";
//				} else {
//					JSONObject jsonLimit = new JSONObject(redisVal);
//					
//					int limitCounter = jsonLimit.getInt("maxTrafficDay");
//					String limitDay = jsonLimit.getString("theDay");
//					LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//							"limitCounter: " + Integer.toString(limitCounter) + ", limitDay: " + limitDay, null);			
//
//					if (limitDay.trim().equals(LocalDateTime.now().format(formatterDay))) {
//						if (limitCounter - 1 > 0) {
//							limitCounter = limitCounter - 1;
//							
//							jsonLimit.put("maxTrafficDay", limitCounter);
//							
//							// Put back to redis
//							redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
//							LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//									"IS IN LIMIT. Set new maxTrafficDay: " + Integer.toString(limitCounter) + ", theDay: " + limitDay, null);			
//
//							isInLimit = "000";
//						} else {
//							LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//									"IS NOT IN LIMIT. Now limitCounter: " + Integer.toString(limitCounter) + " -> status: 903", null);			
//
//							isInLimit = "903"; // TRAFFIC LIMITATION
//						}
//					} else {
//						// LIMIT COUNTER NOT SETUP YET IN REDIS. FIRST PROCESS OF THE DAY. SET IT UP!
//						LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//								"First traffic of today. Set new fresh CLP limit in REDIS: " + Integer.toString(clpLimitTrafficPerDay), null);			
//						
//						String theDay = LocalDateTime.now().format(formatterDay);
//						int newLimitCounter = clpLimitTrafficPerDay;
//						
//						jsonLimit.put("maxTrafficDay", newLimitCounter);
//						jsonLimit.put("theDay", theDay);
//						
//						// Put back to redis
//						redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
//						
//						isInLimit = "000";
//					}
//				}
//			} else if (apiUserNamePrefix.toLowerCase().startsWith("z")) {
//				// ZLT
//				// Review dulu
//				String redisKey = "zltLimit";
//				String redisVal = redisPooler.redisGet(redisCommand, redisKey);
//				LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//						"Checking traffic limit - redisKey: " + redisKey + ", redisVal: " + redisVal, null);			
//
//				if (redisVal == null || redisVal.trim().length() == 0) {
//					// RedisVal is empty. FIRST PROCESS OF THE DAY. SET IT UP!
//					LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit - CLP", false, false, false, messageId, 
//							"First traffic of today -> redisVal empty. Set new fresh ZLT limit in REDIS: " + Integer.toString(zltLimitTrafficPerDay), null);			
//					
//					String theDay = LocalDateTime.now().format(formatterDay);
//					int newLimitCounter = zltLimitTrafficPerDay;
//					
//					JSONObject jsonLimit = new JSONObject();
//					jsonLimit.put("maxTrafficDay", newLimitCounter);
//					jsonLimit.put("theDay", theDay);
//					
//					// Put back to redis
//					redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
//					
//					isInLimit = "000";
//				} else {
//					JSONObject jsonLimit = new JSONObject(redisVal);
//					
//					int limitCounter = jsonLimit.getInt("maxTrafficDay");
//					String limitDay = jsonLimit.getString("theDay");
//					LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//							"limitCounter: " + Integer.toString(limitCounter) + ", limitDay: " + limitDay, null);			
//
//					if (limitDay.trim().equals(LocalDateTime.now().format(formatterDay))) {
//						if (limitCounter - 1 > 0) {
//							limitCounter = limitCounter - 1;
//							
//							jsonLimit.put("maxTrafficDay", limitCounter);
//							
//							// Put back to redis
//							redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
//							LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//									"IS IN LIMIT. Set new maxTrafficDay: " + Integer.toString(limitCounter) + ", theDay: " + limitDay, null);			
//
//							isInLimit = "000";
//						} else {
//							LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//									"IS NOT IN LIMIT. Now limitCounter: " + Integer.toString(limitCounter) + " -> status: 903", null);			
//
//							isInLimit = "903"; // TRAFFIC LIMITATION
//						}
//					} else {
//						// LIMIT COUNTER NOT SETUP YET IN REDIS. FIRST PROCESS OF THE DAY. SET IT UP!
//						LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//								"First traffic of today. Set new fresh ZLT limit in REDIS: " + Integer.toString(zltLimitTrafficPerDay), null);			
//						
//						String theDay = LocalDateTime.now().format(formatterDay);
//						int newLimitCounter = zltLimitTrafficPerDay;
//						
//						jsonLimit.put("maxTrafficDay", newLimitCounter);
//						jsonLimit.put("theDay", theDay);
//						
//						// Put back to redis
//						redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
//						
//						isInLimit = "000";
//					}
//				}
//			} else if ((apiUserNamePrefix.toLowerCase().startsWith("t") || apiUserNamePrefix.toLowerCase().startsWith("o")) && !apiUserName.equals("T3stvnHtc")) {
//				// THK
//				// Review dulu
//				String redisKey = "thkLimit";
//				String redisVal = redisPooler.redisGet(redisCommand, redisKey);
//				LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//						"Checking traffic limit - redisKey: " + redisKey + ", redisVal: " + redisVal, null);			
//
//				if (redisVal == null || redisVal.trim().length() == 0) {
//					// RedisVal is empty. FIRST PROCESS OF THE DAY. SET IT UP!
//					LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit - CLP", false, false, false, messageId, 
//							"First traffic of today -> redisVal empty. Set new fresh THK limit in REDIS: " + Integer.toString(fzhLimitTrafficPerDay), null);			
//					
//					String theDay = LocalDateTime.now().format(formatterDay);
//					int newLimitCounter = fzhLimitTrafficPerDay;
//					
//					JSONObject jsonLimit = new JSONObject();
//					jsonLimit.put("maxTrafficDay", newLimitCounter);
//					jsonLimit.put("theDay", theDay);
//					
//					// Put back to redis
//					redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
//					
//					isInLimit = "000";
//				} else {
//					JSONObject jsonLimit = new JSONObject(redisVal);
//					
//					int limitCounter = jsonLimit.getInt("maxTrafficDay");
//					String limitDay = jsonLimit.getString("theDay");
//					LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//							"limitCounter: " + Integer.toString(limitCounter) + ", limitDay: " + limitDay, null);			
//
//					if (limitDay.trim().equals(LocalDateTime.now().format(formatterDay))) {
//						if (limitCounter - 1 > 0) {
//							limitCounter = limitCounter - 1;
//							
//							jsonLimit.put("maxTrafficDay", limitCounter);
//							
//							// Put back to redis
//							redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
//							LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//									"IS IN LIMIT. Set new maxTrafficDay: " + Integer.toString(limitCounter) + ", theDay: " + limitDay, null);			
//
//							isInLimit = "000";
//						} else {
//							LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//									"IS NOT IN LIMIT. Now limitCounter: " + Integer.toString(limitCounter) + " -> status: 903", null);			
//
//							isInLimit = "903"; // TRAFFIC LIMITATION
//						}
//					} else {
//						// LIMIT COUNTER NOT SETUP YET IN REDIS. FIRST PROCESS OF THE DAY. SET IT UP!
//						LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//								"First traffic of today. Set new fresh THK limit in REDIS: " + Integer.toString(fzhLimitTrafficPerDay), null);			
//						
//						String theDay = LocalDateTime.now().format(formatterDay);
//						int newLimitCounter = fzhLimitTrafficPerDay;
//						
//						jsonLimit.put("maxTrafficDay", newLimitCounter);
//						jsonLimit.put("theDay", theDay);
//						
//						// Put back to redis
//						redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
//						
//						isInLimit = "000";
//					}
//				}
//			} else if (apiUserNamePrefix.toLowerCase().startsWith("m")) {
//				// MITS
//				// Review dulu
//				String redisKey = "mitsLimit";
//				String redisVal = redisPooler.redisGet(redisCommand, redisKey);
//				LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//						"Checking traffic limit - redisKey: " + redisKey + ", redisVal: " + redisVal, null);			
//
//				if (redisVal == null || redisVal.trim().length() == 0) {
//					// RedisVal is empty. FIRST PROCESS OF THE DAY. SET IT UP!
//					LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit - MITS", false, false, false, messageId, 
//							"First traffic of today -> redisVal empty. Set new fresh MITS limit in REDIS: " + Integer.toString(mitsLimitTrafficPerDay), null);			
//					
//					String theDay = LocalDateTime.now().format(formatterDay);
//					int newLimitCounter = mitsLimitTrafficPerDay;
//					
//					JSONObject jsonLimit = new JSONObject();
//					jsonLimit.put("maxTrafficDay", newLimitCounter);
//					jsonLimit.put("theDay", theDay);
//					
//					// Put back to redis
//					redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
//					
//					isInLimit = "000";
//				} else {
//					JSONObject jsonLimit = new JSONObject(redisVal);
//					
//					int limitCounter = jsonLimit.getInt("maxTrafficDay");
//					String limitDay = jsonLimit.getString("theDay");
//					LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//							"limitCounter: " + Integer.toString(limitCounter) + ", limitDay: " + limitDay, null);			
//
//					if (limitDay.trim().equals(LocalDateTime.now().format(formatterDay))) {
//						if (limitCounter - 1 > 0) {
//							limitCounter = limitCounter - 1;
//							
//							jsonLimit.put("maxTrafficDay", limitCounter);
//							
//							// Put back to redis
//							redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
//							LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//									"IS IN LIMIT. Set new maxTrafficDay: " + Integer.toString(limitCounter) + ", theDay: " + limitDay, null);			
//
//							isInLimit = "000";
//						} else {
//							LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//									"IS NOT IN LIMIT. Now limitCounter: " + Integer.toString(limitCounter) + " -> status: 903", null);			
//
//							isInLimit = "903"; // TRAFFIC LIMITATION
//						}
//					} else {
//						// LIMIT COUNTER NOT SETUP YET IN REDIS. FIRST PROCESS OF THE DAY. SET IT UP!
//						LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//								"First traffic of today. Set new fresh MITS limit in REDIS: " + Integer.toString(mitsLimitTrafficPerDay), null);			
//						
//						String theDay = LocalDateTime.now().format(formatterDay);
//						int newLimitCounter = mitsLimitTrafficPerDay;
//						
//						jsonLimit.put("maxTrafficDay", newLimitCounter);
//						jsonLimit.put("theDay", theDay);
//						
//						// Put back to redis
//						redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
//						
//						isInLimit = "000";
//					}
//				}
//			} else if (apiUserNamePrefix.toLowerCase().startsWith("y")) {
//				// YMITS
//				// Review dulu
//				String redisKey = "yMitsLimit";
//				String redisVal = redisPooler.redisGet(redisCommand, redisKey);
//				LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//						"Checking traffic limit - redisKey: " + redisKey + ", redisVal: " + redisVal, null);			
//
//				if (redisVal == null || redisVal.trim().length() == 0) {
//					// RedisVal is empty. FIRST PROCESS OF THE DAY. SET IT UP!
//					LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit - YMITS", false, false, false, messageId, 
//							"First traffic of today -> redisVal empty. Set new fresh YMITS limit in REDIS: " + Integer.toString(ymitsLimitTrafficPerDay), null);			
//					
//					String theDay = LocalDateTime.now().format(formatterDay);
//					int newLimitCounter = ymitsLimitTrafficPerDay;
//					
//					JSONObject jsonLimit = new JSONObject();
//					jsonLimit.put("maxTrafficDay", newLimitCounter);
//					jsonLimit.put("theDay", theDay);
//					
//					// Put back to redis
//					redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
//					
//					isInLimit = "000";
//				} else {
//					JSONObject jsonLimit = new JSONObject(redisVal);
//					
//					int limitCounter = jsonLimit.getInt("maxTrafficDay");
//					String limitDay = jsonLimit.getString("theDay");
//					LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//							"limitCounter: " + Integer.toString(limitCounter) + ", limitDay: " + limitDay, null);			
//
//					if (limitDay.trim().equals(LocalDateTime.now().format(formatterDay))) {
//						if (limitCounter - 1 > 0) {
//							limitCounter = limitCounter - 1;
//							
//							jsonLimit.put("maxTrafficDay", limitCounter);
//							
//							// Put back to redis
//							redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
//							LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//									"IS IN LIMIT. Set new maxTrafficDay: " + Integer.toString(limitCounter) + ", theDay: " + limitDay, null);			
//
//							isInLimit = "000";
//						} else {
//							LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//									"IS NOT IN LIMIT. Now limitCounter: " + Integer.toString(limitCounter) + " -> status: 903", null);			
//
//							isInLimit = "903"; // TRAFFIC LIMITATION
//						}
//					} else {
//						// LIMIT COUNTER NOT SETUP YET IN REDIS. FIRST PROCESS OF THE DAY. SET IT UP!
//						LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//								"First traffic of today. Set new fresh YMITS limit in REDIS: " + Integer.toString(ymitsLimitTrafficPerDay), null);			
//						
//						String theDay = LocalDateTime.now().format(formatterDay);
//						int newLimitCounter = ymitsLimitTrafficPerDay;
//						
//						jsonLimit.put("maxTrafficDay", newLimitCounter);
//						jsonLimit.put("theDay", theDay);
//						
//						// Put back to redis
//						redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
//						
//						isInLimit = "000";
//					}
//				}
//			}  else if (apiUserNamePrefix.toLowerCase().startsWith("n")) {
//				// Nini
//				// Review dulu
//				String redisKey = "niniLimit";
//				String redisVal = redisPooler.redisGet(redisCommand, redisKey);
//				LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//						"Checking traffic limit - redisKey: " + redisKey + ", redisVal: " + redisVal, null);			
//
//				if (redisVal == null || redisVal.trim().length() == 0) {
//					// RedisVal is empty. FIRST PROCESS OF THE DAY. SET IT UP!
//					LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit - NINI", false, false, false, messageId, 
//							"First traffic of today -> redisVal empty. Set new fresh nini limit in REDIS: " + Integer.toString(niniLimitTrafficPerDay), null);			
//					
//					String theDay = LocalDateTime.now().format(formatterDay);
//					int newLimitCounter = niniLimitTrafficPerDay;
//					
//					JSONObject jsonLimit = new JSONObject();
//					jsonLimit.put("maxTrafficDay", newLimitCounter);
//					jsonLimit.put("theDay", theDay);
//					
//					// Put back to redis
//					redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
//					
//					isInLimit = "000";
//				} else {
//					JSONObject jsonLimit = new JSONObject(redisVal);
//					
//					int limitCounter = jsonLimit.getInt("maxTrafficDay");
//					String limitDay = jsonLimit.getString("theDay");
//					LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//							"limitCounter: " + Integer.toString(limitCounter) + ", limitDay: " + limitDay, null);			
//
//					if (limitDay.trim().equals(LocalDateTime.now().format(formatterDay))) {
//						if (limitCounter - 1 > 0) {
//							limitCounter = limitCounter - 1;
//							
//							jsonLimit.put("maxTrafficDay", limitCounter);
//							
//							// Put back to redis
//							redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
//							LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//									"IS IN LIMIT. Set new maxTrafficDay: " + Integer.toString(limitCounter) + ", theDay: " + limitDay, null);			
//
//							isInLimit = "000";
//						} else {
//							LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//									"IS NOT IN LIMIT. Now limitCounter: " + Integer.toString(limitCounter) + " -> status: 903", null);			
//
//							isInLimit = "903"; // TRAFFIC LIMITATION
//						}
//					} else {
//						// LIMIT COUNTER NOT SETUP YET IN REDIS. FIRST PROCESS OF THE DAY. SET IT UP!
//						LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
//								"First traffic of today. Set new fresh NINI in REDIS: " + Integer.toString(niniLimitTrafficPerDay), null);			
//						
//						String theDay = LocalDateTime.now().format(formatterDay);
//						int newLimitCounter = niniLimitTrafficPerDay;
//						
//						jsonLimit.put("maxTrafficDay", newLimitCounter);
//						jsonLimit.put("theDay", theDay);
//						
//						// Put back to redis
//						redisPooler.redisSet(redisCommand, redisKey, jsonLimit.toString());
//						
//						isInLimit = "000";
//					}
//				}
//			} else {
////				if (apiUserName.equals("T3stvnHtc")) {
////					isInLimit = "000";
////					
////					LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
////							"Traffic comes for Hutch. Always in limit.", null);			
////				} else {
////					isInLimit = "902";
////
////					LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
////							"Traffic comes from non allowed. Blocked! status 902.", null);			
////				}
//				
//				// Open for all other account
//				isInLimit = "000";
//			}
		} catch (Exception e) {
			LoggingPooler.doLog(logger, "INFO", loggingName, "isStillInTrafficLimit", false, false, false, messageId, 
					"Failed to access REDIS. IS IN LIMIT = true", null);			

			e.printStackTrace();
			isInLimit = "000"; // Still can go! -> default
		}
		
		return isInLimit;
	}
	
	private void sendMessageToQueue(String messageId, String queuedMessage, String queueName){
		try{
			channelRabbitPublish.queueDeclare(queueName, true, false, false, null);
			channelRabbitPublish.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN, queuedMessage.toString().getBytes());
			LoggingPooler.doLog(logger, "INFO", loggingName, "sendMessageToQueue", false, false, false, messageId, 
					"Sending message " + queuedMessage + " to transceiver queue " + queueName, null);			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private String generateAutoVendorSenderId(String messageId, String clientSenderId) {
		String vendorSenderId = "";
		
		try {
			// Generate numeric
			vendorSenderId = tool.generateUniqueId("NUMERIC", 11);
			
			// Buang prefix 0 ganti dengan angka 2
			if (vendorSenderId.startsWith("0")) {
				vendorSenderId = "2" + vendorSenderId;
			}
			
			// Tambahkan 62 (country code indonesia)
			vendorSenderId = "62" + vendorSenderId;
		} catch (Exception e) {
			e.printStackTrace();
			
			LoggingPooler.doLog(logger, "DEBUG", loggingName, "generateAutoVendorSenderId", true, false, false, messageId, 
					"Failed to generate auto long number vendor sender id. Error occured.", e);			
		}
		
		LoggingPooler.doLog(logger, "DEBUG", loggingName, "generateAutoVendorSenderId", false, false, false, messageId, 
				"Assigning clientSenderId: " + clientSenderId + " new vendorSenderId: " + vendorSenderId, null);

		return vendorSenderId;
	}
	
	private void processQueueData(String message){
		// Route to relevant queue transceiver
		
		try{
			JSONObject jsonMessage = new JSONObject(message);
			LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, "", 
					"Incoming jsonMessage: " + jsonMessage.toString(), null);
			
			// incoming jsonMessage: {"apiUserName":"chand01","clientIpAddress":"127.0.0.1","clientId":"PRA190318","transactionDateTime":"190322121018392",
			// "receiverDateTime":"190322121018392","prefix":"811","telecomId":"62001","messageId":"7373ee5bcee14742bf89003c7bd39204","errorCode":"002",
			// "clientSenderId":"MITSUBISHI","message":"\u20ac Lorem [ipsum] dolor sit amet, consectetur adipiscing elit. Proin feugiat, leo id commodo tincidunt, nibh 
			// diam ornare est, vitae accumsan risus lacus sed sem metus.","clientSenderIdId":"MITSUBISHI-PRA190318","countryCode":"62","receiverType":"SMPP",
			// "msisdn":"628111253636", "sysSessionId": "chan01-9238392929", "smsChannel": "HTTP"}
			
			String messageId = jsonMessage.getString("messageId");
			//String clientSenderIdId = jsonMessage.getString("clientSenderIdId");
			String apiUsername = jsonMessage.getString("apiUserName");
			String telecomId = jsonMessage.getString("telecomId");
			String msisdn = jsonMessage.getString("msisdn");
			String theSMS = jsonMessage.getString("message");
			String clientId = jsonMessage.getString("clientId");
			String clientSenderId = jsonMessage.getString("clientSenderId");
			String smsChannel = jsonMessage.getString("smsChannel");
			String sysSessionId = "";
		
			// Check traffic limit
			String apiUserNamePrefix = apiUsername.substring(0, 1);
			String isInLimitStatus = isStillInApiUserNamePrefixTelecomTrafficLimit(messageId, apiUsername, apiUserNamePrefix, telecomId);
			
			if (isInLimitStatus.equals("000")) {
				if(jsonMessage.has("sysSessionId")){
					sysSessionId = jsonMessage.getString("sysSessionId");
				}
				
				double clientPricePerSubmit = jsonMessage.getDouble("clientPricePerSubmit");
				int messageCount = jsonMessage.getInt("messageCount");
				
				// Define routeId (clientSenderId + "-" + clientIdId + "-" + telecomId
				String originalClientSenderId = clientSenderId; // Sender ID dari client ditampung di originalClientSenderId
				if(clientSenderId.trim().startsWith("*AUTOGEN*-")){
					clientSenderId = "*AUTOGEN*";
				} else if(clientSenderId.trim().startsWith("*LONGNUMBER*-")){
					clientSenderId = "*LONGNUMBER*";
				} 
				
				String routeId = clientSenderId.trim() + "-" + clientId.trim() + "-" + apiUsername + "-" + telecomId.trim();
				LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
						"routeId: " + routeId, null);
				
				//System.out.println("\njsonRouteSMSProperty: " + RouteSMSPooler.jsonRouteSMSProperty.toString());
				if(RouteSMSPooler.jsonRouteSMSProperty.has(routeId)){
					// route is defined
					JSONObject jsonRoute = RouteSMSPooler.jsonRouteSMSProperty.getJSONObject(routeId);
					LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
							"jsonRoute: " + jsonRoute.toString(), null);
					
					double routeClientPricePerSubmit = 0.00;
					if (jsonRoute.has("clientPricePerSubmit")) {
						routeClientPricePerSubmit = jsonRoute.getDouble("clientPricePerSubmit");
					}
					
					if (clientPricePerSubmit < routeClientPricePerSubmit) {
						clientPricePerSubmit = routeClientPricePerSubmit;
					}
					
					// Get routed vendorId
					String routedVendorId = jsonRoute.getString("vendorId");
					LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
							"routedVendorId: " + routedVendorId, null);
					
					// Get vendorSenderId based on vendorId
					String vendorSenderIdId = jsonRoute.getString("vendorSenderidId");
					LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
							"routedVendorId: " + routedVendorId + " -> vendorSenderIdId: " + vendorSenderIdId, null);
					
					// Get vendorSenderId
					JSONObject jsonVSenderId = VendorSMSSenderIdPooler.jsonVendorSMSSenderIdProperty.getJSONObject(vendorSenderIdId);
					String vendorSenderId = jsonVSenderId.getString("vendorSenderId");
					
					// Handle vendorSenderId utk AUTOGEN
					if(vendorSenderId.contains("*AUTOGEN*")) {
						if (originalClientSenderId.contains("*AUTOGEN*")){
							vendorSenderId = originalClientSenderId.replace("*AUTOGEN*-", "");
						} else {
							vendorSenderId = generateAutoVendorSenderId(messageId, originalClientSenderId);
						}
					}
					
					LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
							"routedVendorId: " + routedVendorId + " -> vendorSenderIdId: " + vendorSenderIdId + " -> vendorSenderId: " + vendorSenderId, null);
					
					// Get vendorParameters
					String vendorParameter = "";
					if(jsonRoute.has("vendorParameter")){
						vendorParameter = jsonRoute.getString("vendorParameter");
					}
					LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
							"routedVendorId: " + routedVendorId + " -> vendorParameter: " + vendorParameter, null);
									
					// Get routedVendorId queuenames
					JSONObject jsonVendor = VendorSMSPooler.jsonVendorSMSProperty.getJSONObject(routedVendorId);
					LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
							"routedVendorId: " + routedVendorId + " -> property: " + jsonVendor.toString(), null);
					
					// Deduct balance, if failed return as FAILED DELIVERY REPORT
					//double usageValue = jsonRoute.getDouble("clientPrice") * ;
					double usageValue = clientPricePerSubmit * messageCount;
					LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
							"clientPricePerSubmit: " + String.format("%.5f", clientPricePerSubmit) + ", messageCount: " + messageCount + "usageValue: " + String.format("%.5f", usageValue), null);
					
					String usageBy = "SMSROUTER";
					String usageType = "USAGE";
					
					// Get business model
					String businessModel = clientPropertyPooler.getBusinessMode(clientId);
					
					// Compose usageDescription
					String usageDescription = "Usage by " + messageId + ", MSISDN: " + msisdn + ", SenderId: " + originalClientSenderId;
					
					//JSONObject jsonDeduction = clientBalancePooler.deductClientBalance(messageId, clientId, usageType, usageValue, usageBy, usageDescription, businessModel, routedVendorId, vendorPrice, vendorCurrency, usageIdr, vendorPriceIdr, marginIdr, exchageRateToIdr)
					JSONObject jsonDeduction = clientBalancePooler.deductClientBalance(messageId, clientId, usageType, usageValue, usageBy, usageDescription, businessModel);
					LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
							"routedVendorId: " + routedVendorId + " -> jsonDeduction: " + jsonDeduction.toString(), null);
					
					int deductionStatus = jsonDeduction.getInt("status");
					String deductionDateTime = jsonDeduction.getString("deductionDateTime");
									
					if(deductionStatus == 0){
						// DEDUCTING BALANCE IS SUCCESS -> Update trx status to PENDING
//						Connection connection = null;
//						Statement statement = null;
								
						try{
//							BasicDataSource bds = DataSource.getInstance().getBds();
//							connection = bds.getConnection();
//							String query = "update transaction_sms set status_code = '002' where message_id = '" + messageId + "'";
//							
//							statement = connection.createStatement();
//							statement.execute(query);
							
							statementUpdateStatus.setString(1, "002");
							statementUpdateStatus.setString(2, messageId);
							
							int impactedRow = statementUpdateStatus.executeUpdate();
							
							if (impactedRow > 0) {
								LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
										"Updating transaction status is SUCCESS. Impacted row: " + impactedRow, null);
							} else {
								LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
										"Updating transaction status is FAILED. Impacted row: " + impactedRow, null);
							}
						} catch (Exception ex) {
							ex.printStackTrace();
							LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", true, false, false, messageId, 
									"Failed to update status to 002. Error occured.", ex);
						}
						
						
						String queueNameAsSetup = jsonVendor.getString("queueName");
						
						// Split queueName to 
						String[] arrVendorQueues = queueNameAsSetup.split(",");
											
						//String[] arrVendorQueues = (String[]) jsonVendor.get("queueName");
						LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
								"routedVendorId: " + routedVendorId + " -> queues: " + Arrays.toString(arrVendorQueues), null);
											
						// Random number of arrVendorQueues
						int randSeed = arrVendorQueues.length;
						int selectedQIndex = rand.nextInt(randSeed);
						LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
								"routedVendorId: " + routedVendorId + " -> selectedQIndex: " + selectedQIndex, null);
						
						// Send to transceiver queue
						JSONObject jsonQueuedMessage = new JSONObject();
						jsonQueuedMessage.put("messageId", messageId);
						jsonQueuedMessage.put("msisdn", msisdn);
						jsonQueuedMessage.put("message", theSMS);
						jsonQueuedMessage.put("vendorSenderId", vendorSenderId);
						jsonQueuedMessage.put("telecomId", telecomId);
						jsonQueuedMessage.put("vendorParameter", vendorParameter);
						jsonQueuedMessage.put("routedVendorId", routedVendorId);
						
						// Adding apiUserName to send to transceiver for telin CPAAS requirement
						jsonQueuedMessage.put("apiUserName", apiUsername);
						
						// DON'T FORGET TO ALWAYS TRIM IN HERE! Split IS NOT TRIMMING
						String queueName = arrVendorQueues[selectedQIndex].trim();								
						sendMessageToQueue(messageId, jsonQueuedMessage.toString(), queueName);
						LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
								"routedVendorId: " + routedVendorId + " -> vendor queueName: " + queueName + ". SUBMIT SUCCESSFULLY.", null);

						LocalDateTime now = LocalDateTime.now();
						
						// Update trxdata in redis
						JSONObject jsonTrxDataRedis = new JSONObject();
						jsonTrxDataRedis.put("deduction", usageValue);
						jsonTrxDataRedis.put("deductionDateTime", deductionDateTime);
						jsonTrxDataRedis.put("routerToTransceiverDateTime", now.format(formatter));
						
						transactionRedisPooler.updateTrxRedisData(messageId, jsonTrxDataRedis);
						LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
								"routedVendorId: " + routedVendorId + " -> redis trxdata submited: " + jsonTrxDataRedis.toString() + ". SUBMIT SUCCESSFULLY.", null);
						
						// Send DLR if fakeDr is required
						boolean isFakeDrRequired = false;					
						if(RouteSMSPooler.jsonRouteSMSProperty.has(routeId)){
							System.out.println("jsonRouteSMSProperty: " + RouteSMSPooler.jsonRouteSMSProperty.getJSONObject(routeId).toString());
							if(RouteSMSPooler.jsonRouteSMSProperty.getJSONObject(routeId).has("fakeDr")){
								isFakeDrRequired = RouteSMSPooler.jsonRouteSMSProperty.getJSONObject(routeId).getBoolean("fakeDr");
							}
						}
						
						System.out.println("isFakeDr required: " + isFakeDrRequired);
						if(!smsChannel.trim().equals("AUTOGENSMS") && isFakeDrRequired == true){
							String errorCode = "000";
							System.out.println("isFakeDrRequired = " + isFakeDrRequired + ", errorCode: " + errorCode + ". smsChannel: " + smsChannel);
							if(smsChannel.trim().equals("SMPP")){
								now = LocalDateTime.now();
								smppDlrPooler.sendSMPPFakeDLR(messageId, msisdn, clientSenderId, theSMS, errorCode, now, "-", sysSessionId);
								LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
										"DLR delivered is sent.", null);
								smsTransactionOperationPooler.updateTransactionStatus(messageId, errorCode);
							}
						}
					} else {
						LocalDateTime now = LocalDateTime.now();

						// Failed to deduct balance, return Delivery Report failed - Balance is not enough
						// Update trxdata in redis
						JSONObject jsonTrxDataRedis = new JSONObject();
						jsonTrxDataRedis.put("deduction", usageValue);
						jsonTrxDataRedis.put("deductionDateTime", deductionDateTime);
						jsonTrxDataRedis.put("routerToDLRDateTime", now.format(formatter));
						
						transactionRedisPooler.updateTrxRedisData(messageId, jsonTrxDataRedis);
						LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
								"routedVendorId: " + routedVendorId + " -> redis trxdata submited: " + jsonTrxDataRedis.toString() + ". SUBMIT SUCCESSFULLY.", null);					

						String errorCode = "122";
						if(smsChannel.trim().equals("SMPP")){
							now = LocalDateTime.now();
							SMPPDLRPooler.sendSMPPDLR(messageId, msisdn, clientSenderId, theSMS, errorCode, now, "-", sysSessionId);
							LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
									"routedVendorId: " + routedVendorId + " -> FAILED TO DEDUCT. ERROR CODE " + errorCode, null);
						}
						
						// Update data transaction_sms, status to be 122 (BALANCE NOT ENOUGH)
						smsTransactionOperationPooler.updateTransactionStatus(messageId, errorCode);
						LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
								"Database transaction is updated to failed for not enough balance.", null);
						
					}
				} else {
					// route is not defined. Send failed delivery report to relevant receiverType DR
					LocalDateTime now = LocalDateTime.now();

					// Update trxdata in redis
					JSONObject jsonTrxDataRedis = new JSONObject();
					jsonTrxDataRedis.put("routerToDLRDateTime", now.format(formatter));
					
					transactionRedisPooler.updateTrxRedisData(messageId, jsonTrxDataRedis);

					String errorCode = "125";
					
					if(smsChannel.trim().equals("SMPP")){
						now = LocalDateTime.now();
						SMPPDLRPooler.sendSMPPDLR(messageId, msisdn, clientSenderId, theSMS, errorCode, now, "-", sysSessionId);
					}
					LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
							"ROUTE NOT DEFINED. ERROR CODE " + errorCode, null);

					// Update data transaction_sms, status to be 900 (ERROR FOR ROUTE IS NOT DEFINED)
					smsTransactionOperationPooler.updateTransactionStatus(messageId, errorCode);
				}
			} else {
				// Response failed
				LocalDateTime now = LocalDateTime.now();

				// Update trxdata in redis
				JSONObject jsonTrxDataRedis = new JSONObject();
				jsonTrxDataRedis.put("routerToDLRDateTime", now.format(formatter));
				
				transactionRedisPooler.updateTrxRedisData(messageId, jsonTrxDataRedis);

				String errorCode = isInLimitStatus;
				LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
						"REACH MAX LIMIT TRAFFIC TO TELECOM: " + telecomId + ". ERROR CODE " + errorCode, null);
				
				if(smsChannel.trim().equals("SMPP")){
					now = LocalDateTime.now();
					SMPPDLRPooler.sendSMPPDLR(messageId, msisdn, clientSenderId, theSMS, errorCode, now, "-", sysSessionId);
					LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, messageId, 
							"REACH MAX LIMIT TRAFFIC TO TELECOM: " + telecomId + ". ERROR CODE " + errorCode, null);
				}

				// Update data transaction_sms, status to be 900 (ERROR FOR ROUTE IS NOT DEFINED)
				smsTransactionOperationPooler.updateTransactionStatus(messageId, errorCode);
			}
		} catch (Exception e) {
			e.printStackTrace();
			LoggingPooler.doLog(logger, "DEBUG", loggingName, "processQueueData", false, false, false, "", 
					"Failed to process the incoming message. Error occured.", e);			
		}		
	}
	
	private void processTheQueue(String SMSQueueName){
		try{
			// Set QueueName variable for logging naming
			QueueName = SMSQueueName;
			
			channelRabbitConsume.queueDeclare(SMSQueueName, true, false, false, null);		
			LoggingPooler.doLog(logger, "INFO", loggingName, "processTheQueue", false, false, true, "", 
					"Reading queue is ready, waiting for message ... ", null);
			
			Consumer consumer = new DefaultConsumer(channelRabbitConsume) {
			      @Override
			      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
			          throws IOException {
			        String message = new String(body, "UTF-8");
			        
					try{
						LoggingPooler.doLog(logger, "INFO", loggingName, "processTheQueue", false, false, true, "", 
								"Receive message: " + message, null);

						// Process the message from queue
				        processQueueData(message);
					} finally {
						LoggingPooler.doLog(logger, "INFO", loggingName, "processTheQueue", false, false, true, "", 
								"Done processing message: " + message, null);

						channelRabbitConsume.basicAck(envelope.getDeliveryTag(), false);
					}
			      }
			};

			boolean autoAck = false; // If not finally exectued well, no ack to rabbitmq, message not gone
			channelRabbitConsume.basicConsume(SMSQueueName, autoAck, consumer);
			
//			// Development
//			System.out.println("TIDURRRR DULU ...");
//			Thread.sleep(100000);
		} catch (Exception e){
			LoggingPooler.doLog(logger, "INFO", loggingName, "readQueue", true, false, false, "", 
					"Failed to access queue " + SMSQueueName, e);
		}
	}
	
	public static void main(String[] args) {
		if (args.length == 1) {
			// QUEUE INCOMING AS RECEIVEER QUEUE as the first parameter
			System.out.println("Starting SMS Router. QUEUE NAME: " + args[0]);
			SMSRouter router = new SMSRouter();
			router.processTheQueue(args[0]);
			System.out.println("SMS Router is running using QUEUE " + args[0]);
		} else {
			System.out.println("Invalid Parameter. System exit!");
			System.exit(-1);
		}		
	}

}
