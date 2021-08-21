package com.blastme.messaging.smsbulk.http;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.json.JSONObject;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.ClientBalancePooler;
import com.blastme.messaging.toolpooler.ClientPropertyPooler;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.RedisPooler;
import com.blastme.messaging.toolpooler.RouteSMSPooler;
import com.blastme.messaging.toolpooler.SMSTransactionOperationPooler;
import com.blastme.messaging.toolpooler.SenderIdSMSPooler;
import com.blastme.messaging.toolpooler.TelecomPrefixPooler;
import com.blastme.messaging.toolpooler.UserAPIHTTPSMSPooler;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

import io.lettuce.core.api.sync.RedisCommands;

public class SMSBulkHTTPAPIServer {
	private static Logger logger;	

	private final static double eliandrieBalanceThreshold = 50000.00;
	
	private static RedisPooler redisPooler;
	private static RedisCommands<String, String> redisCommand;

	private static int httpAPIPort = 2778;
	private static String path = "sms";

	private static String SMSQueueName = "SMS_INCOMING";
	private SimpleDateFormat sdf;
	private ClientBalancePooler clientBalancePooler;
	private SMSTransactionOperationPooler smsTransactionOperationPooler;
	private ClientPropertyPooler clientPropertyPooler;
	private UserAPIHTTPSMSPooler userApiHttpSMSPooler;
	
	private RabbitMQPooler rabbitMqPooler;

	public SMSBulkHTTPAPIServer() {
		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone(Configuration.getTimeZone()));
		
		// Setup logger
		logger = LogManager.getLogger("HTTP_SMS");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate SimpleDateFormat
		sdf = new SimpleDateFormat("yyMMddHHmmssSSS");
		
		// Initiate RedisPooler
		redisPooler = new RedisPooler();
		redisCommand = redisPooler.redisInitiateConnection();
		
		// Initiate RabbitMQPooler
		rabbitMqPooler = new RabbitMQPooler();		
		
		// Initiate UserAPIHTTPSMSPooler
		new UserAPIHTTPSMSPooler();
				
		// Initiate jsonPrefixProperty in TelecomPrefixPooler
		new TelecomPrefixPooler();
		
		// Initiate SenderIdSMSPooler
		new SenderIdSMSPooler();
		
		// Initiate RouteSMSPooler
		new RouteSMSPooler();
		
		// Initiate clientBalancePooler
		clientBalancePooler = new ClientBalancePooler();
		
		// Initiate smsTransactionOperationPooler
		smsTransactionOperationPooler = new SMSTransactionOperationPooler();
		
		// Initiate clientPropertyPooler
		clientPropertyPooler = new ClientPropertyPooler();
		
		// Initiate UserAPIHTTPSMSPooler
		userApiHttpSMSPooler = new UserAPIHTTPSMSPooler();
		
		LoggingPooler.doLog(logger, "INFO", "SMSBulkHTTPAPIServer", "SMSBulkHTTPAPIServer", false, false, false, "", 
				"Starting HTTP API SMS...", null);
	}

	private void startServer(){
		Server server = new Server(httpAPIPort);
		
		ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.setContextPath("/" + path);

		server.setHandler(context);
		context.addServlet(new ServletHolder(new HandlerServlet()), "/*");
		
		try {
			server.start();
			server.join();
			
			LoggingPooler.doLog(logger, "INFO", "SMSBulkHTTPAPIServer", "startServer", false, false, false, "", 
					"Service Status Receiver HTTP Server is started.", null);
		} catch (Exception e) {
			LoggingPooler.doLog(logger, "INFO", "SMSBulkHTTPAPIServer", "startServer", true, false, false, "", 
					"Service Status Receiver HTTP Server is started.", e);
			
			e.printStackTrace();
		}
	}
	
	private class HandlerServlet extends HttpServlet{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		private String getBodyContentOfPost(HttpServletRequest request){
			String bodyContent = null;
			StringBuilder buffer = new StringBuilder();
			BufferedReader reader = null;
			
			try {
				reader = request.getReader();
				
				String line = "";
				
				while((line = reader.readLine()) != null){
					buffer.append(line);
				}
			} catch (IOException e) {
				e.printStackTrace();
				
				LoggingPooler.doLog(logger, "DEBUG", "SMSBulkHTTPAPIServer", "HandlerServlet - getBodyContetOfPost", true, false, false, "", 
						"Error while getting body content of post. Error occured.", e);
			} finally {
		        if (reader != null) {
		            try {
		                reader.close();
		            } catch (IOException ex) {
		                ex.printStackTrace();
		            }
		        }
		    }
			
			bodyContent = buffer.toString();
			return bodyContent;
		}
		
		private String generateMessageId(){
			String hasil = "";
			
			UUID uniqueId = UUID.randomUUID();			
			hasil = String.valueOf(uniqueId).replace("-", "");
			
			return hasil;
		}
		
		private JSONObject isValidAPIUser(String username, String password, String ipAddress){
			JSONObject jsonReturn = new JSONObject();
			
			if(UserAPIHTTPSMSPooler.jsonHTTPAPIUser.has(username.trim())){
				// systemId valid, check password
				JSONObject jsonDetail = UserAPIHTTPSMSPooler.jsonHTTPAPIUser.getJSONObject(username.trim());
				
				if(password.equals(jsonDetail.get("password"))){
					// Password valid, check IPAddress
					String theRegisteredIPAddress = jsonDetail.getString("ipAddress");
					
					if(!theRegisteredIPAddress.trim().equals("ALL")){
						// Registered certain IP, split them by comma
						String[] splittedIpAddress = theRegisteredIPAddress.split(",");
						
						for(int oh = 0; oh < splittedIpAddress.length; oh++){
							if(ipAddress.trim().equals(splittedIpAddress[oh].trim())){
								jsonReturn.put("status", 0); // Success
								jsonReturn.put("description", "Validation success.");
								
								break;
							} else {
								jsonReturn.put("status", -3);
								jsonReturn.put("description", "Unregistered IP Address.");							
							}
						}
					} else {
						// Open for ALL
						jsonReturn.put("status", 0);
						jsonReturn.put("description", "Validation success.");
					}
				} else {
					jsonReturn.put("status", -2);
					jsonReturn.put("description", "Bad password.");
				}
			} else {
				jsonReturn.put("status", -1);
				jsonReturn.put("description", "Bad username.");
			}
			
			return jsonReturn;
		}
		
//		public String getClientId(String username){
//			String clientId = "";
//			
//			if(UserAPIHTTPSMSPooler.jsonHTTPAPIUser.has(username.trim())){
//				JSONObject jsonDetail = UserAPIHTTPSMSPooler.jsonHTTPAPIUser.getJSONObject(username.trim());
//				
//				clientId = jsonDetail.getString("clientId");
//			}
//			
//			return dId;
//		}
//		
		private boolean isSenderIdValid(String clientId, String clientSenderId){			
			boolean isValid = false;
					
			String senderIdId = clientId.trim() + "-" + clientSenderId.trim();
			
			System.out.println(SenderIdSMSPooler.jsonSenderIdSMSProperty.toString());
			if(SenderIdSMSPooler.jsonSenderIdSMSProperty.has(senderIdId)){
				isValid = true;
			}
					
			return isValid;
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
		
		private boolean isRouteDefined(String clientSenderIdId, String telecomId){
			boolean isDefined = false;
			
			String routeId = clientSenderIdId.trim() + "-" + telecomId.trim();
			
			if(RouteSMSPooler.jsonRouteSMSProperty.has(routeId)){
				isDefined = true;
			}
			
			return isDefined;
		}		
		
		private int getSmsCount(String message, String encoding){
			if(encoding.equals("GSM7")){
				if(message.length() <= 160){
					return 1;
				} else {
					return (int) Math.ceil(message.length() / 153);
				}
			} else if(encoding.equals("UCS2")) {
				if(message.length() <= 70){
					return 1;
				} else {
					return (int) Math.ceil(message.length() / 67);
				}
			} else {
				return (int) Math.ceil(message.length() / 67);
			}
		}

		@Override
		protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
			// Generating messageId
			String messageId = generateMessageId();
			
			// Default respStatus
			String respStatus = "002"; 
			
			// Default msisdn & clientIpAddress
			String msisdn = null;
			String clientIpAddress = req.getRemoteAddr();
			
			// Parameters
			String username = "";
			String clientSenderId = "";
			String message = "";
			String countryCode = "";
			String prefix = "";
			String telecomId = "";
			String senderIdId = "";
			String clientId = "";
			double clientPrice = 0.00;
			String currency = "IDR";
			String incomingData = "";
			
			try{
				incomingData = getBodyContentOfPost(req);
				LoggingPooler.doLog(logger, "INFO", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
						"incomingData: " + incomingData, null);

				// Check if incoming data is valid JSON
				JSONObject jsonIncomingData = null;
				try{
					jsonIncomingData = new JSONObject(incomingData);
					LoggingPooler.doLog(logger, "INFO", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
							"VALID JSON INCOMING REQUEST - " + jsonIncomingData.toString(), null);
					
					// Extract detail incoming data
					if(jsonIncomingData.has("type") && jsonIncomingData.has("senderid") && jsonIncomingData.has("username") &&
							jsonIncomingData.has("password") && jsonIncomingData.has("message") && jsonIncomingData.has("msisdn")){
						String type = jsonIncomingData.getString("type");
						clientSenderId = jsonIncomingData.getString("senderid");
						username = jsonIncomingData.getString("username");
						String password = jsonIncomingData.getString("password");
						message = jsonIncomingData.getString("message");
						msisdn = jsonIncomingData.getString("msisdn");						


						if(type.equals("sms")){
							LoggingPooler.doLog(logger, "INFO", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
									"Message type: " + type, null);

							// Send to queue and give resp 1;
							// Validate userName and password
							JSONObject isValidUser = isValidAPIUser(username, password, clientIpAddress);
							LoggingPooler.doLog(logger, "INFO", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
									"Check username validity - username: " + username + " - clientIpAddreess: " + clientIpAddress + " - jsonIsValidUser: " + isValidUser.toString(), null);

							int userValidStatus = isValidUser.getInt("status");
							
							if(userValidStatus == 0){
								// Username, password and IP Address are VALID.
								// Get clientId
								clientId = userApiHttpSMSPooler.getClientId(username);
								LoggingPooler.doLog(logger, "INFO", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
										"Username, password and client IP Address VALID -> username: " + username + ", clientId: " + clientId, null);
								
								// Validate senderId
								if(isSenderIdValid(clientId, clientSenderId) == true){
									LoggingPooler.doLog(logger, "INFO", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
											"clientId: " + clientId + " - clientSenderId: " + clientSenderId + " is VALID SENDER ID.", null);

									// Sender Id is valid
									// Validate countryCode nad prefix
									JSONObject isValidPrefix = isPrefixValid(msisdn);
									boolean prefixIsValid = isValidPrefix.getBoolean("isValid");
									
									if(prefixIsValid == true){
										LoggingPooler.doLog(logger, "INFO", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
												"MSISDN: " + msisdn + " - VALID COUNTRY CODE AND PREFIX. JSON: " + isValidPrefix.toString(), null);

										// Prefix is valid
										prefix = isValidPrefix.getString("prefix");
										countryCode = isValidPrefix.getString("countryCode");
										telecomId = isValidPrefix.getString("telecomId");
										
										// Check client business model
										String businessModel = clientPropertyPooler.getBusinessMode(clientId);
										
										// Check client currency
										currency = clientPropertyPooler.getCurrencyId(clientId);

										// Check if user credit balance is ENOUGH
										if((businessModel.equals("PREPAID") && clientBalancePooler.getClientBalance(clientId) > eliandrieBalanceThreshold) || businessModel.equals("POSTPAID")){
											LoggingPooler.doLog(logger, "INFO", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
													"Client balance is ENOUGH (MORE THAN THRESHOLD).", null);
											// client credit balance is enough
											senderIdId = clientSenderId.trim() + "-" + clientId.trim();
											LoggingPooler.doLog(logger, "DEBUG", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
													"Client ID: " + clientId.trim() + ", clientSenderId: " + clientSenderId.trim() + " -> senderIdId: " + senderIdId, null);
											
											// Check if route is defined for the clientsenderid
											if(isRouteDefined(senderIdId, telecomId) == true){
												// Route is defined, process!
												// Get clientPrice
												// Get clientPrice from routing table
												String routeId = senderIdId.trim() + "-" + telecomId.trim();
												JSONObject jsonRoute = RouteSMSPooler.jsonRouteSMSProperty.getJSONObject(routeId);
												clientPrice = jsonRoute.getDouble("clientPrice");

												// Set respStatus = 1 (submitted)
												respStatus = "002";
												
												// Submit to Queue for further process
												JSONObject jsonIncoming = new JSONObject();
												jsonIncoming.put("messageId", messageId);
												jsonIncoming.put("receiverDateTime", sdf.format(new Date()));
												jsonIncoming.put("transactionDateTime", sdf.format(new Date()));
												jsonIncoming.put("msisdn", msisdn);
												jsonIncoming.put("message", message);
												jsonIncoming.put("telecomId", telecomId);
												jsonIncoming.put("countryCode", countryCode);
												jsonIncoming.put("prefix", prefix);
												jsonIncoming.put("errorCode", respStatus);
												jsonIncoming.put("apiUserName", username);
												jsonIncoming.put("clientSenderIdId", senderIdId);
												jsonIncoming.put("clientSenderId", clientSenderId);
												jsonIncoming.put("clientId", clientId);
												jsonIncoming.put("clientIpAddress", clientIpAddress);
												jsonIncoming.put("receiverType", "HTTP"); // SMPP and HTTP only
												LoggingPooler.doLog(logger, "DEBUG", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
														"jsonIncoming: " + jsonIncoming.toString(), null);				
												
												Connection connectionRabbit = null;
												Channel channel = null;
												try{
													connectionRabbit = rabbitMqPooler.getConnection();
													channel = rabbitMqPooler.getChannel(connectionRabbit);
													
													channel.queueDeclare(SMSQueueName, true, false, false, null);
													channel.basicPublish("", SMSQueueName, MessageProperties.PERSISTENT_TEXT_PLAIN, jsonIncoming.toString().getBytes());
													LoggingPooler.doLog(logger, "DEBUG", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
															"jsonIncoming: " + jsonIncoming.toString() + " published SUCCESSFULLY!", null);
																
												} catch (Exception ex) {
													respStatus = "002";

													LoggingPooler.doLog(logger, "DEBUG", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", true, false, false, messageId, 
															"Failed publishing message to queue " + SMSQueueName + ". Error occured.", ex);
												} finally {
													channel.close();
													connectionRabbit.close();
												}
											} else {
												// Route is NOT DEFINED
												respStatus = "900";
												
												LoggingPooler.doLog(logger, "DEBUG", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
														"ROUTE IS NOT DEFINED for clientId: " + clientId + ", senderId: " + clientSenderId, null);
											}											
										} else {
											// client credit balance is NOT ENOUGH
											respStatus = "122";
											
											LoggingPooler.doLog(logger, "DEBUG", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
													"BALANCE IS NOT ENOUGH for clientId: " + clientId, null);											
										}
									} else {
										// Prefix IS NOT VALID
										respStatus = "113";
										
										LoggingPooler.doLog(logger, "DEBUG", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
												"BAD PREFIX OR COUNTRY CODE.", null);											
									}
								} else {
									// Sender Id is NOT VALID
									respStatus = "121";
									
									LoggingPooler.doLog(logger, "DEBUG", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
											"INVALID SENDERID.", null);											
								}
							} else {
								// Username, password and IP Address are NOT VALID.
								respStatus = "110";
								
								LoggingPooler.doLog(logger, "DEBUG", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
										"INVALID USERNAME, PASSWORD AND OR IP ADDRESS.", null);											
							}
						}
					} else {
						respStatus = "901";

						LoggingPooler.doLog(logger, "INFO", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, "", 
								"INVALID JSON INCOMING REQUEST - " + incomingData, null);
					}
				} catch(Exception e) {
					e.printStackTrace();
					LoggingPooler.doLog(logger, "INFO", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", true, false, false, "", 
							"INVALID JSON INCOMING REQUEST - " + incomingData, e);
					
					respStatus = "901";
				}
			} catch (Exception e) {
				e.printStackTrace();
				LoggingPooler.doLog(logger, "INFO", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", true, false, false, "", 
						"Failed to process the request. Error occured.", e);

				respStatus = "900";
			}
			
			// Prepare for JSON response
			JSONObject jsonResponse = new JSONObject();
			jsonResponse.put("messageid", messageId);
			jsonResponse.put("msisdn", msisdn);
			jsonResponse.put("statuscode", respStatus);
			jsonResponse.put("datetime", sdf.format(new Date()));
			
			try{
				LocalDateTime now = LocalDateTime.now();
				smsTransactionOperationPooler.saveInitialSMPPData(messageId, now, "HTTPAPI", incomingData, jsonResponse.toString(), 
						clientIpAddress, now, now, msisdn, message, countryCode, prefix, telecomId, respStatus, "HTTP", senderIdId, clientSenderId, clientId, 
						username, clientPrice, currency, "GSM7", message.length(), getSmsCount(message, "GSM7"));
				
				LoggingPooler.doLog(logger, "DEBUG", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
						"Successfully save Initial Data to Database.", null);	

				// Submit to Redis as initial data with expiry 7 days
				int expiry = 7 * 24 * 60 * 60;
				JSONObject jsonRedis = new JSONObject();
				jsonRedis.put("messageId", messageId);
				jsonRedis.put("receiverDateTime", sdf.format(new Date()));
				jsonRedis.put("transactionDateTime", sdf.format(new Date()));
				jsonRedis.put("msisdn", msisdn);
				jsonRedis.put("message", message);
				jsonRedis.put("telecomId", telecomId);
				jsonRedis.put("countryCode", countryCode);
				jsonRedis.put("prefix", prefix);
				jsonRedis.put("errorCode", respStatus);
				jsonRedis.put("apiUserName", username);
				jsonRedis.put("clientSenderIdId", senderIdId);
				jsonRedis.put("clientSenderId", clientSenderId);
				jsonRedis.put("clientId", clientId);
				jsonRedis.put("clientIpAddress", clientIpAddress);
				jsonRedis.put("receiverType", "HTTP"); // SMPP and HTTP only

				String redisKey = "trxdata-" + messageId;
				String redisVal = jsonRedis.toString();
				
				redisPooler.redisSetWithExpiry(redisCommand, redisKey, redisVal, expiry);
				LoggingPooler.doLog(logger, "DEBUG", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
						"Successfully save Initial Data to Database and Redis.", null);	
			} catch (Exception e) {
				e.printStackTrace();
				LoggingPooler.doLog(logger, "DEBUG", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", true, false, false, messageId, 
						"Failed to save initial data to database and redis. Error occured.", e);	
			}
			
			// Send out the response
			resp.setStatus(HttpStatus.OK_200);
			resp.getWriter().println(jsonResponse.toString());
			LoggingPooler.doLog(logger, "INFO", "SMSBulkHTTPAPIServer", "HandlerServlet - doPost", false, false, false, messageId, 
					"CLIENT RESPONSE: " + jsonResponse.toString(), null);	
		}
	}
	
	public static void main(String[] args) {
		SMSBulkHTTPAPIServer api = new SMSBulkHTTPAPIServer();
		api.startServer();
	}
}
