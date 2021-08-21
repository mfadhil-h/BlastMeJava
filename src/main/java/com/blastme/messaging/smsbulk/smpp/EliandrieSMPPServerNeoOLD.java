package com.blastme.messaging.smsbulk.smpp;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.tika.parser.txt.CharsetDetector;
import org.joda.time.DateTime;
import org.json.JSONObject;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.ClientBalancePooler;
import com.blastme.messaging.toolpooler.ClientPropertyPooler;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.RedisPooler;
import com.blastme.messaging.toolpooler.RouteSMSPooler;
import com.blastme.messaging.toolpooler.SMPPDLRPooler;
import com.blastme.messaging.toolpooler.SMPPEnquiryLinkPooler;
import com.blastme.messaging.toolpooler.SMSTransactionOperationPooler;
import com.blastme.messaging.toolpooler.SenderIdSMSPooler;
import com.blastme.messaging.toolpooler.TelecomPrefixPooler;
import com.blastme.messaging.toolpooler.Tool;
import com.blastme.messaging.toolpooler.UserAPISMPPSMSPooler;
import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppServerConfiguration;
import com.cloudhopper.smpp.SmppServerHandler;
import com.cloudhopper.smpp.SmppServerSession;
import com.cloudhopper.smpp.SmppSessionConfiguration;
import com.cloudhopper.smpp.impl.DefaultSmppServer;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.BaseBind;
import com.cloudhopper.smpp.pdu.BaseBindResp;
import com.cloudhopper.smpp.pdu.DeliverSm;
import com.cloudhopper.smpp.pdu.EnquireLink;
import com.cloudhopper.smpp.pdu.PduRequest;
import com.cloudhopper.smpp.pdu.PduResponse;
import com.cloudhopper.smpp.pdu.SubmitSm;
import com.cloudhopper.smpp.pdu.SubmitSmResp;
import com.cloudhopper.smpp.type.Address;
import com.cloudhopper.smpp.type.SmppChannelException;
import com.cloudhopper.smpp.type.SmppProcessingException;
import com.cloudhopper.smpp.util.DeliveryReceipt;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;

import io.lettuce.core.api.sync.RedisCommands;

public class EliandrieSMPPServerNeoOLD {
	private static Logger logger;	
	
	private final static int smppPort = 2776;
	private final static int smppMaxConnection = 500;
	private final static int smppRequestExpiryTimeout = 30000;
	private final static int smppWindowMonitorInterval = 15000;
	private final static int smppWindowSize = 500;
	
	private final static double eliandrieBalanceThreshold = 50000.00;
		
	private static RedisPooler redisPooler;
	private static RedisCommands<String, String> redisCommand;

	private static String SMSQueueName = "SMPP_INCOMING";
	
	public static ConcurrentHashMap<String, Object> mapSystemIdSessionId = new ConcurrentHashMap<String, Object>();
	
	public static void main(String[] args) {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("SMPP_SERVER");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate RedisPooler
		redisPooler = new RedisPooler();
		redisCommand = redisPooler.redisInitiateConnection();
		
		// Initiate RabbitMQPooler
		new RabbitMQPooler();		
				
		// Initiate jsonPrefixProperty in TelecomPrefixPooler
		new TelecomPrefixPooler();
		
		// Initiate SenderIdSMSPooler
		new SenderIdSMSPooler();
		
		// Initiate RouteSMSPooler
		new RouteSMSPooler();
				
		// Initiate SMPPDLRPooler
		new SMPPDLRPooler();
				
		//
        // setup 3 things required for a server
        //
        
        // for monitoring thread use, it's preferable to create your own instance
        // of an executor and cast it to a ThreadPoolExecutor from Executors.newCachedThreadPool()
        // this permits exposing things like executor.getActiveCount() via JMX possible
        // no point renaming the threads in a factory since underlying Netty 
        // framework does not easily allow you to customize your thread names
        ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newCachedThreadPool();
        
        // to enable automatic expiration of requests, a second scheduled executor
        // is required which is what a monitor task will be executed with - this
        // is probably a thread pool that can be shared with between all client bootstraps
        ScheduledThreadPoolExecutor monitorExecutor = (ScheduledThreadPoolExecutor)Executors.newScheduledThreadPool(1, new ThreadFactory() {
            private AtomicInteger sequence = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("SmppServerSessionWindowMonitorPool-" + sequence.getAndIncrement());
                return t;
            }
        }); 
        
        // create a server configuration
        SmppServerConfiguration configuration = new SmppServerConfiguration();
        configuration.setPort(smppPort);
        configuration.setMaxConnectionSize(smppMaxConnection);
        configuration.setNonBlockingSocketsEnabled(true);
        configuration.setDefaultRequestExpiryTimeout(smppRequestExpiryTimeout);
        configuration.setDefaultWindowMonitorInterval(smppWindowMonitorInterval);
        configuration.setDefaultWindowSize(smppWindowSize);
        configuration.setDefaultWindowWaitTimeout(smppRequestExpiryTimeout);
        configuration.setDefaultSessionCountersEnabled(true);
        configuration.setJmxEnabled(true);
        
        // create a server, start it up
        DefaultSmppServer smppServer = new DefaultSmppServer(configuration, new DefaultSmppServerHandler(), executor, monitorExecutor);

        System.out.println("Starting SMPP Server ...");
		LoggingPooler.doLog(logger, "INFO", "EliandrieSMPPServerNeo", "main", false, false, false, "", "Starting SMPP Server ...", null);				
        try {
			smppServer.start();
	        System.out.println("SMPP server started");
			LoggingPooler.doLog(logger, "INFO", "EliandrieSMPPServerNeo", "main", false, false, false, "", 
					"SMPP Server started ...", null);				
		} catch (SmppChannelException e) {
			e.printStackTrace();
			LoggingPooler.doLog(logger, "INFO", "EliandrieSMPPServerNeo", "main", true, false, false, "", 
					"Failed to start SMPP Server. Error occured.", e);				
		} 
	}

	
	public static class DefaultSmppServerHandler implements SmppServerHandler {
		private UserAPISMPPSMSPooler userApiSMPPSMSPooler;
		private ClientBalancePooler clientBalancePooler;
		private SMSTransactionOperationPooler smsTransactionOperationPooler;
		private ClientPropertyPooler clientPropertyPooler;
		private SMPPEnquiryLinkPooler smppEnquiryLinkPooler;
		
		private final static ExecutorService execSystemVariableUpdater = Executors.newFixedThreadPool(1);
		private final static ExecutorService execDLR = Executors.newFixedThreadPool(1);
		private final static ExecutorService execSessionCleanUp = Executors.newFixedThreadPool(1);
		
		private Tool tool;
		
		public DefaultSmppServerHandler() {
			tool = new Tool();
			
			// Initiate UserAPISMPPSMSPooler
			userApiSMPPSMSPooler = new UserAPISMPPSMSPooler();
			
			// Initiate ClientBalancePooler
			clientBalancePooler = new ClientBalancePooler();	
			
			// Initiate smsTransactionOperationPooler
			smsTransactionOperationPooler = new SMSTransactionOperationPooler();
			// Initiate clientPropertyPooler
			clientPropertyPooler = new ClientPropertyPooler();
			
			// Initiate SMPPEnquiryLinPooler
			smppEnquiryLinkPooler = new SMPPEnquiryLinkPooler();

			// Run thread for variable updater - 
			execSystemVariableUpdater.execute(new SystemCommand());
			
			// Run thread for sending DRL
			execDLR.execute(new sendingDLR());
		}

		@Override
		public void sessionBindRequested(Long sessionId, SmppSessionConfiguration sessionConfiguration, @SuppressWarnings("rawtypes") final BaseBind bindRequest) throws SmppProcessingException {
			// test name change of sessions
			// this name actually shows up as thread context....
			sessionConfiguration.setName("Application.SMPP." + sessionConfiguration.getSystemId());

//			System.out.println("SMPP SystemID: " + sessionConfiguration.getSystemId());
//			System.out.println("SMPP Password: " + sessionConfiguration.getPassword());
//			System.out.println("SMPP HOST: " + sessionConfiguration.getHost());
			LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "DefaultSmppServerHandler - sessionBindRequested", false, false, false, "", 
					"Incoming System ID: " + sessionConfiguration.getSystemId() + ", Remote IP Address: " + sessionConfiguration.getHost(), null);				

			String systemId = sessionConfiguration.getSystemId();
			String password = sessionConfiguration.getPassword();
			String remoteIpAddress = sessionConfiguration.getHost();

			// Validasi systemId, password dan remoteIpAddress
			JSONObject jsonValidation = userApiSMPPSMSPooler.isValidSysId(systemId, password, remoteIpAddress);
			LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "DefaultSmppServerHandler - sessionBindRequested", false, false, false, "", 
					"jsonValidation: " + jsonValidation.toString(), null);				
			
			if(jsonValidation.getInt("status") != 0){
				LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "DefaultSmppServerHandler - sessionBindRequested", false, false, false, "", 
						"Failed to validate! jsonValidation: " + jsonValidation.toString(), null);				
				throw new SmppProcessingException(SmppConstants.STATUS_INVSYSID, null);
			}
			//throw new SmppProcessingException(SmppConstants.STATUS_BINDFAIL, null);
		}

		@Override
		public void sessionCreated(Long sessionId, SmppServerSession session, BaseBindResp preparedBindResponse) throws SmppProcessingException {
			System.out.println("Session created: " + session + ", sessionId: " + sessionId.toString());
			LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "DefaultSmppServerHandler - sessionCreated", false, false, false, "", 
					"Session created: " + session + ", sessionId: " + sessionId.toString(), null);	
			
			// Prepare data systemId, clientId, remoteIpAddress
			String systemId = session.getConfiguration().getSystemId();
			String remoteIpAddress = session.getConfiguration().getHost();
//			String divisionId = userApiSMPPSMSPooler.getDivisionId(systemId);
			String clientId = userApiSMPPSMSPooler.getClientId(systemId);
			LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "DefaultSmppServerHandler - sessionCreated", false, false, false, "", 
					"SessionId: " + sessionId + ", systemId: " + systemId + ", clientId: " + clientId, null);	

			// Put the session into mapDivisionSessionId - sebagai session baru
			// Create unique divisionId
			String sysIdKey = systemId + "-" + tool.generateUniqueID();
			
			// Save it
			mapSystemIdSessionId.put(sysIdKey.trim(), session);
//			LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "DefaultSmppServerHandler - sessionCreated", false, false, false, "", 
//					"SessionId: " + sessionId + ", new sysIdKey: " + sysIdKey + " is added to mapSystemIdSessionId.", null);
			
//			System.out.println("Keys for mapSystemIdSessionId: ");
//			for(String key: mapSystemIdSessionId.keySet()){
//				System.out.println("	" + key);
//			}
			
			session.serverReady(new SmppSessionHandler(session, systemId, clientId, remoteIpAddress, clientBalancePooler, smsTransactionOperationPooler, clientPropertyPooler, smppEnquiryLinkPooler, sysIdKey)); // clientBalancePooler is injected
		}

		@Override
		public void sessionDestroyed(Long sessionId, SmppServerSession session) {
			System.out.println("Session destroyed: " + session);
			LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "DefaultSmppServerHandler - sessionDestroyed", false, false, false, "", 
					"Session destroyed: " + session, null);				
			// print out final stats
			if (session.hasCounters()) {
				System.out.println(" final session rx-submitSM: " + session.getCounters().getRxSubmitSM());
				LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "DefaultSmppServerHandler - sessionDestroyed", false, false, false, "", 
						"Final session rx-submitSM: " + session.getCounters().getRxSubmitSM(), null);				
			}
			
			// make sure it's really shutdown
			session.destroy();
			
			// Clean Up!
			execSessionCleanUp.execute(new SessionCleaner());
		}
		
		private static class SessionCleaner implements Runnable{
			
			public SessionCleaner() {
				LoggingPooler.doLog(logger, "INFO", "SessionCleaner", "SessionCleaner", false, false, false, "", 
						"Successfully initialize module SessionCleaner.", null);
			}

			private void cleanUp(){
				try{
					LoggingPooler.doLog(logger, "INFO", "SessionCleaner", "cleanUp", false, false, false, "", 
							"Cleaning up unbound sessions.", null);

	 				// Check existing mapSystemSessionId masih connected apa nggak - bersihkan yang udah gak konek
					ArrayList<String> listToRemove = new ArrayList<String>();
					for(Map.Entry<String, Object> entry: mapSystemIdSessionId.entrySet()){
						String key = entry.getKey();
						SmppServerSession val = (SmppServerSession) entry.getValue();
						
						if(val.isBound() == false){
							System.out.println("SessionId: " + val.toString() + " is NOT OPEN OR NOT BOUND. REMOVE IT.");
							//mapSystemIdSessionId.remove(key);
							listToRemove.add(key);
						}
					}				
					
					// Remove dari mapSystemIdSessionId semua key yang ada di listToRemove
					Iterator<String> i = listToRemove.iterator();
					while(i.hasNext()){
						String theKey = i.next();
						System.out.println("Removing session for id: " + theKey);
						mapSystemIdSessionId.remove(theKey);
					}

					System.out.println("Keys for mapSystemIdSessionId: ");
					for(String key: mapSystemIdSessionId.keySet()){
						System.out.println("	" + key);
					}

					LoggingPooler.doLog(logger, "INFO", "SessionCleaner", "cleanUp", false, false, false, "", 
							"Done Cleaning up unbound sessions.", null);
				} catch (Exception e) {
					e.printStackTrace();
					
					LoggingPooler.doLog(logger, "INFO", "SessionCleaner", "cleanUp", true, false, false, "", 
							"Failed to clean up session. Error occured.", e);
				}
			}
			
			@Override
			public void run() {
				cleanUp();
			}			
		}
		
		// Class untuk listed system variable updater
		private static class SystemCommand implements Runnable{
			private static String queueSystemCommand = "SYSTEM_COMMAND";
			
			private RabbitMQPooler rabbitMQPooler;
			private Channel channel;

			public SystemCommand() {
				// Incoming queueMessage will be JSON String:
				// {"commander": "chandra.pawitra", "command": "RELOAD_CLIENT_SENDER_ID", "commandDateTime": "yyMMddHHmmss.SSS"}
				// Initiate rabbitMq
				rabbitMQPooler = new RabbitMQPooler();
				
				Connection connection = rabbitMQPooler.getConnection();
				channel = rabbitMQPooler.getChannel(connection);
				
				try {
					channel.queueDeclare(queueSystemCommand, true, false, false, null);
					LoggingPooler.doLog(logger, "INFO", "SystemCommand", "SystemCommand", false, false, false, "", 
							"Successfully initialize channel rabbitMq to queueName " + queueSystemCommand, null);
				} catch (IOException e) {
					LoggingPooler.doLog(logger, "INFO", "SystemCommand", "SystemCommand", true, false, true, "", 
							"Failed to initialize channle rabbitMq to queueName " + queueSystemCommand, e);
				}

			}
			
			private void processTheCommand(String message){
				// The message will be: {"commander": "chandra.pawitra", "command": "RELOAD_CLIENT_SENDER_ID", "commandDateTime": "yyMMddHHmmssSSS"}
				try{
					JSONObject jsonMessage = new JSONObject(message);
					LoggingPooler.doLog(logger, "DEBUG", "SystemCommand", "processTheCommand", false, false, true, "", 
							"jsonMessage: " + jsonMessage.toString(), null);
					
					String command = jsonMessage.getString("command");
					System.out.println("Incoming command: " + command);
					
					if(command.trim().equals("RELOAD_SYSTEMID")){
						// RELOADING SYSTEM ID
						System.out.println("Reloading system id...");
						UserAPISMPPSMSPooler.loadJSONSMPPSysId();
						LoggingPooler.doLog(logger, "DEBUG", "SystemCommand", "processTheCommand", false, false, true, "", 
								"New UserAPISMPPSMSPooler is reloaded.", null);
					} else if(command.trim().equals("RELOAD_CLIENTSENDERID")){
						System.out.println("Reloading client sender id...");
						SenderIdSMSPooler.initiateJsonSenderIdSMSProperty();
						LoggingPooler.doLog(logger, "DEBUG", "SystemCommand", "processTheCommand", false, false, true, "", 
								"New SenderIdSMSPooler is reloaded.", null);
					} else if(command.trim().equals("RELOAD_ROUTINGTABLE")){
						System.out.println("Reloading routing table...");
						RouteSMSPooler.initiateJSONRouteSMSProperty();
						LoggingPooler.doLog(logger, "DEBUG", "SystemCommand", "processTheCommand", false, false, true, "", 
								"New RouteSMSPooler is reloaded.", null);
					} else if(command.trim().equals("RELOAD_PREFIXANDCOUNTRYCODE")){
						System.out.println("Reloading telecom prefix pooler...");
						TelecomPrefixPooler.initiateJSONPrefixProperty();
						LoggingPooler.doLog(logger, "DEBUG", "SystemCommand", "processTheCommand", false, false, true, "", 
								"New TelecomPrefixPooler is reloaded.", null);
					}
				} catch(Exception e) {
					e.printStackTrace();
					LoggingPooler.doLog(logger, "INFO", "SystemCommand", "processTheCommand", true, false, true, "", 
							"Failed to process the COMMAND MESSAGE. Error occured.", e);
				}
				
				System.out.println("Done reloading things... Have a good day.");
			}

			@Override
			public void run() {
				try{				
					LoggingPooler.doLog(logger, "INFO", "SystemCommand", "run", false, false, false, "", 
							"Reading queue " + queueSystemCommand + " for Command/Instruction...", null);
					
					Consumer consumer = new DefaultConsumer(channel) {
					      @Override
					      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
					          throws IOException {
					        String message = new String(body, "UTF-8");
					        
							try{
								LoggingPooler.doLog(logger, "INFO", "SystemCommand", "run", false, false, false, "", 
										"Receive message: " + message, null);

								// Send to client via SMPPServer
								processTheCommand(message);
							} catch (Exception e){
								LoggingPooler.doLog(logger, "INFO", "SystemCommand", "run", true, false, false, "", 
										"Failed processing incoming command message " + message + ". Error occured.", e);
							} finally {
								LoggingPooler.doLog(logger, "INFO", "SystemCommand", "run", false, false, false, "", 
										"Done processing message: " + message, null);

								channel.basicAck(envelope.getDeliveryTag(), false);
							}
					      }
					};
					
					boolean autoAck = false; // If not finally exectued well, no ack to rabbitmq, message not gone
					channel.basicConsume(queueSystemCommand, autoAck, consumer);
				} catch (Exception e){
					LoggingPooler.doLog(logger, "INFO", "SystemCommand", "run", true, false, false, "", 
							"Failed to access queue " + queueSystemCommand, e);
				}			
			}
		}
		
		private class sendingDLR implements Runnable{
			//private static final String DLRQUEUE = "CALLBACK";
			private static final String DLRQUEUE = "SMPP_DLR";
			
			private RabbitMQPooler rabbitMqPooler;
			private Channel channel;

			private RedisPooler redisPooler;
			private RedisCommands<String, String> redisCommand;

			public sendingDLR() {
				rabbitMqPooler = new RabbitMQPooler();
				
				// Initiate redisPooler
				redisPooler = new RedisPooler();
				redisCommand = redisPooler.redisInitiateConnection();
				
				try {
					Connection connection = rabbitMqPooler.getConnection();
					channel = rabbitMqPooler.getChannel(connection);
					
					channel.queueDeclare(DLRQUEUE, true, false, false, null);
					LoggingPooler.doLog(logger, "INFO", "sendingDLR", "sendingDLR", false, false, false, "", 
							"Successfully initialize channel rabbitMq to queueName " + DLRQUEUE, null);
				} catch (IOException e) {
					LoggingPooler.doLog(logger, "INFO", "sendingDLR", "sendingDLR", true, false, true, "", 
							"Failed to initialize channle rabbitMq to queueName " + DLRQUEUE, e);
				}
			}
			
			private void processDlrQueue(String message){
				// message will be: {"messageId": "abcdef1234567890", "status": "000", "msisdn": "621234567890", "sysSessionId": "chand01-192837467"}
				LoggingPooler.doLog(logger, "INFO", "sendingDLR", "sendingDLR", false, false, false, "", 
						"message: " + message, null);
				try{
					JSONObject jsonMessage = new JSONObject(message);
					
					String messageId = jsonMessage.getString("messageId");
					String status = jsonMessage.getString("errorCode");
					String msisdn = jsonMessage.getString("msisdn");
					
					// Get sessionId yang digunakan utk terima message, spy DR menggunakan session yang sama
					String sysSessionId = "";
					if(jsonMessage.has("sysSessionId")){
						sysSessionId = jsonMessage.getString("sysSessionId");
					}
					
					// Get message and systemId and clientSenderId
					String redisKey = "trxdata-" + messageId.trim();
					String redisVal = redisPooler.redisGet(redisCommand, redisKey);
					
					LoggingPooler.doLog(logger, "DEBUG", "sendingDLR", "sendingDLR", false, false, false, messageId, 
							"messageId: " + messageId + ", status: " + status + ", msisdn: " + msisdn + ", trxdata value " + redisVal, null);
					//{"apiUserName":"tig079","clientIpAddress":"106.14.2.221","clientId":"TUP19062103600","transactionDateTime":"190628145108879","receiverDateTime":"190628145108879","prefix":"821",
					//	"deductionDateTime":"190628145108919","telecomId":"62001","messageId":"0175af6f5db34cdd8848caa58bb95079","errorCode":"002","clientSenderId":"MITSUBISHI",
					//	"sysSessionId":"tig079-bd07ba3f3ee447129af7a7da85de0d2f","message":"ÄchuanglanÑyou verification code:3333,please used it for 5 minutes",
					//	"routerToTransceiverDateTime":"190628145108938","clientSenderIdId":"MITSUBISHI-TUP19062103600","deduction":320,"countryCode":"62","receiverType":"SMPP","msisdn":"6282179008809"}
					
					JSONObject jsonRedis = new JSONObject(redisVal);
					String theSMS = jsonRedis.getString("message");
					String theSysId = jsonRedis.getString("apiUserName").trim();
					String theClientSenderId = jsonRedis.getString("clientSenderId").trim();
					String clientId = jsonRedis.getString("clientId");
					
					LoggingPooler.doLog(logger, "DEBUG", "sendingDLR", "sendingDLR", false, false, false, messageId, 
							"messageId: " + messageId + ", theSMS: " + theSMS, null);
					
					// Get session
					
					DeliveryReceipt dlr = new DeliveryReceipt();
					dlr.setMessageId(messageId);
					if(status.equals("000")){
						dlr.setSubmitCount(0);
						dlr.setDeliveredCount(1);
					} else if(status.equals("001") || status.equals("002") || status.equals("003")){
						dlr.setDeliveredCount(0);						
						dlr.setSubmitCount(1);
					} else {
						dlr.setDeliveredCount(0);
					}
					
					dlr.setSubmitDate(new DateTime());
					dlr.setDoneDate(new DateTime());
					
					byte deliveryState = SmppConstants.STATE_DELIVERED;
					if(status.equals("000")){
						deliveryState = SmppConstants.STATE_DELIVERED;
					} else if(status.equals("001") || status.equals("002") || status.equals("003")){
						deliveryState = SmppConstants.STATE_ACCEPTED;
					} else {
						deliveryState = SmppConstants.STATE_UNDELIVERABLE;
					}
					dlr.setState(deliveryState);
					
					// change errorCode to int
					int iErrorCode = 0;
					if(status.equals("000")){
						iErrorCode = 0;
					} else if(status.equals("001") || status.equals("002") || status.equals("003")){
						iErrorCode = 1;
					} else {
						iErrorCode = Integer.parseInt(status);
					}
					
					dlr.setErrorCode(iErrorCode);
					dlr.setText(theSMS);

					String receipt0 = dlr.toShortMessage();
					LoggingPooler.doLog(logger, "DEBUG", "sendingDLR", "sendingDLR", false, false, false, messageId, 
							"messageId: " + messageId + ", the DLR: " + receipt0, null);
					
					// Source address is the msidn - dibalik dari sms masuk
					Address mtDestinationAddress = new Address((byte)0x03, (byte)0x00, theClientSenderId);
					Address mtSourceAddress = new Address((byte)0x01, (byte)0x01, msisdn);

					DeliverSm deliver = new DeliverSm();
					deliver.setEsmClass(SmppConstants.ESM_CLASS_MT_SMSC_DELIVERY_RECEIPT);					
					deliver.setSourceAddress(mtSourceAddress);
					deliver.setDestAddress(mtDestinationAddress);
					if(receipt0.trim().length() > 0){
						deliver.setShortMessage(receipt0.getBytes());
						System.out.println("receipt0 length > 0. Content: " + receipt0.toString());
						System.out.println("deliver: " + CharsetUtil.decode(deliver.getShortMessage(), CharsetUtil.CHARSET_GSM));
					}

					if(mapSystemIdSessionId.contains(sysSessionId)){
						SmppServerSession theSession = (SmppServerSession) mapSystemIdSessionId.get(sysSessionId);

						theSession.sendRequestPdu(deliver, 10000, false);		
						LoggingPooler.doLog(logger, "DEBUG", "sendingDLR", "sendingDLR", false, false, false, messageId, 
							"Sending DLR to sysId " + theSysId.trim() + " via sessionId " + theSession.toString(), null);
							
						// Save to DB transaction_sms_dlr
						LocalDateTime now = LocalDateTime.now();
						SMPPDLRPooler.updateDLRTable(messageId, clientId, now, receipt0.toString(), status, "SMPP " + sysSessionId, "", "DR SENT TO CLIENT " + clientId + ", sessId: " + sysSessionId);
					} else {
						// Find sysSessionId saved in mapSystemIdSessionId started with sysId
						SmppServerSession theSession = null; // Initiate null
						
						// For development purposes - print mapSystemIdSessionId
						System.out.println(" ----***--- mapSystemIdSessionId Keys: ");
						for(Map.Entry<String, Object> entry: mapSystemIdSessionId.entrySet()){
							System.out.println("KEY: " + entry.getKey());
						}
						
						for(Map.Entry<String, Object> entry: mapSystemIdSessionId.entrySet()){
							String key = entry.getKey();
							SmppServerSession val = (SmppServerSession) entry.getValue();

							System.out.println(" *** MAPSYSTEMIDSESSIONID KEY: " + key + " - theSysId: " + theSysId);
							if(key.startsWith(theSysId.trim())){
								sysSessionId = key;
								theSession = val;
								
								System.out.println(" ### MAPSYSTEMIDSESSIONID " + sysSessionId + " IS SELECTED!");
								
								if(theSession.isBound()){
									System.out.println("isBound: " + theSession.isBound());
									break;
								} else {
									System.out.println(sysSessionId + " is NOT BOUND.");
								}
							}
						}	
						
						if(theSession != null)
						System.out.println("theSession isBound: " + theSession.isBound());
												
						if(theSession != null && theSession.isBound()){
							// Send using this sessionId
							theSession.sendRequestPdu(deliver, 10000, false);		
							LoggingPooler.doLog(logger, "DEBUG", "sendingDLR", "sendingDLR", false, false, false, messageId, 
								"Sending DLR to sysId " + theSysId.trim() + " via sessionId " + sysSessionId, null);
								
							// Save to DB transaction_sms_dlr
							LocalDateTime now = LocalDateTime.now();
							SMPPDLRPooler.updateDLRTable(messageId, clientId, now, receipt0.toString(), status, "SMPP " + sysSessionId, "", "DR SENT TO CLIENT " + clientId + ", sessId: " + sysSessionId);								
						} else {
							// SessionId is not found in mapSystemIdSessionId
							// RE-QUEUE
							SMPPDLRPooler.sendSMPPPreviouslyFailedDLR(messageId, message);
							LoggingPooler.doLog(logger, "DEBUG", "sendingDLR", "sendingDLR", false, false, false, messageId, 
									"Sending DLR to sysId " + theSysId.trim() + ", sessionId: " + sysSessionId + " is failed. Relevant session NOT FOUND. Re-queued.", null);
						}
					}
				} catch (Exception e) {
					LoggingPooler.doLog(logger, "INFO", "sendingDLR", "processDlrQueue", true, false, true, "", 
							"Failed to process the DLR message.", e);
					SMPPDLRPooler.sendSMPPPreviouslyFailedDLR("", message);
				}
			}

			@Override
			public void run() {
				try{				
					LoggingPooler.doLog(logger, "INFO", "sendingDLR", "run", false, false, false, "", 
							"Reading queue " + DLRQUEUE + " for DLR...", null);
					
					Consumer consumer = new DefaultConsumer(channel) {
					      @Override
					      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
					          throws IOException {
					        String message = new String(body, "UTF-8");
					        
							try{
								LoggingPooler.doLog(logger, "INFO", "sendingDLR", "run", false, false, false, "", 
										"Receive message: " + message, null);

								// Send to client via SMPPServer
								processDlrQueue(message);
							} finally {
								LoggingPooler.doLog(logger, "INFO", "sendingDLR", "run", false, false, false, "", 
										"Done processing message: " + message, null);

									channel.basicAck(envelope.getDeliveryTag(), false);
							}
					      }
					};
					
					boolean autoAck = false; // If not finally exectued well, no ack to rabbitmq, message not gone
					channel.basicConsume(DLRQUEUE, autoAck, consumer);
				} catch (Exception e){
					LoggingPooler.doLog(logger, "INFO", "sendingDLR", "run", true, false, false, "", 
							"Failed to access queue " + DLRQUEUE, e);
				}	
			}			
		}
	}

	public static class SmppSessionHandler extends DefaultSmppSessionHandler {
		//private WeakReference<SmppSession> sessionRef;
		private String systemId;
		private String clientId;
		private String remoteIpAddress;
		private SmppServerSession session;
		
		private ClientBalancePooler clientBalancePooler;
		private SMSTransactionOperationPooler smsTransactionOperationPooler;
		private ClientPropertyPooler clientPropertyPooler;
		private SMPPEnquiryLinkPooler smppEnquiryLinkPooler;
		
		private String sysSessionId;
		
		private RabbitMQPooler rabbitMqPooler;
		private Connection connection;
				
		public SmppSessionHandler(SmppServerSession session, String systemId, String clientId, String remoteIpAddress, ClientBalancePooler clientBalancePooler, 
				SMSTransactionOperationPooler smsTransactionOperationPooler, ClientPropertyPooler clientPropertyPooler, SMPPEnquiryLinkPooler smppEnquiryLinkPooler, String sessionId) {			
			//this.sessionRef = new WeakReference<SmppSession>(session);
			this.systemId = systemId;
			this.remoteIpAddress = remoteIpAddress;
			this.clientBalancePooler = clientBalancePooler;
			this.smsTransactionOperationPooler = smsTransactionOperationPooler;	
			this.clientPropertyPooler = clientPropertyPooler;
			this.smppEnquiryLinkPooler = smppEnquiryLinkPooler;
			this.clientId = clientId;
			this.session = session;
			this.sysSessionId = sessionId;
			
			// Initiate rabbitMqPooler
			rabbitMqPooler = new RabbitMQPooler();
			connection = rabbitMqPooler.getConnection();

			System.out.println("SmppSessionHandler - sysSessionId: " + this.sysSessionId);
		}
		
		private String generateUniqueID(){
			String hasil = "";
			
			UUID uniqueId = UUID.randomUUID();
			
			hasil = String.valueOf(uniqueId).replace("-", "");
			
			return hasil;
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
		
		private boolean isSenderIdValid(String clientId, String clientSenderId){
			boolean isValid = false;
					
			String senderIdId = clientSenderId.trim() + "-" + clientId.trim();
			
			if(SenderIdSMSPooler.jsonSenderIdSMSProperty.has(senderIdId)){
				isValid = true;
			}
					
			return isValid;
		}
		
		private boolean isRouteDefined(String clientId, String clientSenderId, String apiUsername, String telecomId){
			boolean isDefined = false;
			
			String routeId = clientSenderId.trim() + "-" + clientId.trim() + "-" + apiUsername.trim() + "-" + telecomId.trim();
			
			if(RouteSMSPooler.jsonRouteSMSProperty.has(routeId)){
				isDefined = true;
			}
			
			System.out.println("ROUTE ID " + routeId + " is DEFINED.");
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
		
		private void saveInitialData(String messageId, LocalDateTime receiverDateTime, String batchId, String receiverData, String receiverClientResponse, 
				String receiverclientIpAddress, LocalDateTime clientResponseDateTime, LocalDateTime trxDateTime, String msisdn, String message, String countryCode, String prefix, 
				String telecomId, String trxStatus, String receiverType, String clientSenderIdId, String clientSenderId, String clientId, String apiUserName, 
				double clientUnitPrice, String currency, String messageEncoding, int messageLength, int smsCount, String deliveryStatus){
			try{
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMddHHmmssSSS");

				System.out.println("DB AWAL - " + messageId);

				LocalDateTime now = LocalDateTime.now();
				smsTransactionOperationPooler.saveInitialSMPPData(messageId, now, "SMPPAPI", receiverData, receiverClientResponse, 
						this.remoteIpAddress, now, now, msisdn.trim(), message.trim(), countryCode, prefix, telecomId, trxStatus, "SMPP", clientSenderIdId, clientSenderId, this.clientId, 
						this.systemId, clientUnitPrice, currency, messageEncoding, messageLength, smsCount);
				
				LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
						"Successfully save Initial Data to Database.", null);	
				System.out.println("DB ANTARA AWAL DAN AKHIR - " + messageId);

				// Submit to Redis as initial data with expiry 7 days
				int expiry = 7 * 24 * 60 * 60;
				JSONObject jsonRedis = new JSONObject();
				jsonRedis.put("messageId", messageId);
				jsonRedis.put("receiverDateTime", receiverDateTime.format(formatter));
				jsonRedis.put("transactionDateTime", trxDateTime.format(formatter));
				jsonRedis.put("msisdn", msisdn.trim());		// Have to be trimmed
				jsonRedis.put("message", message.trim()); 	// Have to be trimmed
				jsonRedis.put("telecomId", telecomId);
				jsonRedis.put("countryCode", countryCode);
				jsonRedis.put("prefix", prefix);
				jsonRedis.put("errorCode", trxStatus);
				jsonRedis.put("apiUserName", this.systemId);
				jsonRedis.put("clientSenderIdId", clientSenderIdId);
				jsonRedis.put("clientSenderId", clientSenderId);
				jsonRedis.put("clientId", this.clientId);
				jsonRedis.put("apiUserName", this.systemId);
				jsonRedis.put("clientIpAddress", this.remoteIpAddress);
				jsonRedis.put("receiverType", "SMPP"); // SMPP and HTTP only
				jsonRedis.put("sysSessionId", this.sysSessionId);

				String redisKey = "trxdata-" + messageId.trim();
				String redisVal = jsonRedis.toString();
				
				redisPooler.redisSetWithExpiry(redisCommand, redisKey, redisVal, expiry);
				LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
						"Successfully save Initial Data to Database and Redis.", null);
				
				System.out.println("DB AKHIR - " + messageId);			
			} catch (Exception e) {
				e.printStackTrace();
				LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", true, false, false, messageId, 
						"Failed saving to database transaction. Error occured.", e);
			}
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

		@Override
		public PduResponse firePduRequestReceived(@SuppressWarnings("rawtypes") PduRequest pduRequest) {
//			System.out.println("pduRequest: " + pduRequest.toString() + " --- State Name: " + session.getStateName());
//			System.out.println(" ----***--- mapSystemIdSessionId Keys: ");
//			for(Map.Entry<String, Object> entry: mapSystemIdSessionId.entrySet()){
//				System.out.println("KEY: " + entry.getKey());
//			}
			
			// Update mapSystemIdSessionId with new BOUND session
//			System.out.println("## Adding new update session with key: " + sysSessionId);
			mapSystemIdSessionId.put(sysSessionId, session);
//			System.out.println(" ----***--- mapSystemIdSessionId Keys: ");
//			for(Map.Entry<String, Object> entry: mapSystemIdSessionId.entrySet()){
//				System.out.println("KEY: " + entry.getKey());
//			}

			//SmppSession session = sessionRef.get();
			PduResponse response = pduRequest.createResponse();

			if(pduRequest.getCommandId() == SmppConstants.CMD_ID_ENQUIRE_LINK){
				EnquireLink enqResp = new EnquireLink();
				int seqNumber = pduRequest.getSequenceNumber();
				enqResp.setSequenceNumber(seqNumber);
				response = enqResp.createResponse();
				
				System.out.println("Enquire Link Request from " + this.systemId + " - " + this.clientId + " - " + pduRequest.toString() + " - sequenceNumber: " + seqNumber);
				this.smppEnquiryLinkPooler.logEnquiryLink(clientId, this.sysSessionId, "ENQUIRE_LINK");
			} else if (pduRequest.getCommandId() == SmppConstants.CMD_ID_SUBMIT_SM) {
				String messageId = generateUniqueID();
				LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
						"Initial messageId: " + messageId, null);				

				try {
					this.smppEnquiryLinkPooler.logEnquiryLink(clientId, this.sysSessionId, "SUBMIT_SM");

					System.out.println(messageId + " IS COMING!");
					
					SubmitSm mt = (SubmitSm) pduRequest;
					Address mtSourceAddress = mt.getSourceAddress();
					String senderId = mtSourceAddress.getAddress().trim();
					
					Address mtDestinationAddress = mt.getDestAddress();
					String msisdn = mtDestinationAddress.getAddress().trim();
					
					//byte dataCoding = mt.getDataCoding();
					byte[] shortMessage = mt.getShortMessage();
					
					// Check the encoding. 
					CharsetDetector detector = new CharsetDetector();
					detector.setText(shortMessage);
					System.out.println("ENCODING: " + detector.detect().getName());
					
					//String theSMS = new String(shortMessage, "SCGSM");
					String theSMS = CharsetUtil.decode(shortMessage, CharsetUtil.CHARSET_GSM).trim();
					LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
							"mtSourceAddress: " + mtSourceAddress.getAddress() + ", mtDestinationAddress: " + mtDestinationAddress.getAddress() + ", shortMessage: " +
							Arrays.toString(shortMessage) + ", the SMS: " + theSMS, null);				
					
					//byte deliveryState = SmppConstants.STATE_ACCEPTED;
					String errorCode = "002";
					System.out.println(messageId + " IS ACCEPTED!");

					// Initiate formatter
					DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMddHHmmssSSS");
										
					//deliveryState = SmppConstants.STATE_ACCEPTED; 		// ACCEPTED AND SUBMITTED TO TELCO
					errorCode = "002";
					// Validate contrycode and prefix -
					JSONObject jsonPrefix = isPrefixValid(msisdn);
					boolean isPrefixValid = jsonPrefix.getBoolean("isValid");
					LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
							"Check if prefix valid: " + jsonPrefix.toString(), null);				
					
					String prefix = "";
					String countryCode = "";
					String telecomId = "";
					String senderIdId = "";
					double clientPricePerSubmit = 0.00;
					String clientCurrency = "IDR";

					LocalDateTime now = LocalDateTime.now();
					JSONObject jsonIncoming = new JSONObject();
					
					System.out.println(messageId + " VALID PREFIX IS " + isPrefixValid + "!");

					if(isPrefixValid == true){
						// Get other prefix data
						prefix = jsonPrefix.getString("prefix");
						countryCode = jsonPrefix.getString("countryCode");
						telecomId = jsonPrefix.getString("telecomId");

						System.out.println(messageId + " PREFIX IS TRUE CROT!");

						// Validate clientSenderId
						if(isSenderIdValid(this.clientId, senderId) == true){
							// Validate if route is properly setup-ed
							senderIdId = senderId.trim() + "-" + this.clientId.trim();
							LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
									"Check SENDERID - VALID! senderIdId: " + senderIdId, null);				

							System.out.println(messageId + " SENDERID IS DEFINED. SENDERIDID: " + senderIdId);

							if(isRouteDefined(this.clientId, senderId, this.systemId, telecomId) == true){
								LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
										"Check ROUTE - DEFINED!", null);		

								System.out.println(messageId + " ROUTE IS DEFINED!");

								// Validate balance
								// Get client currecnt
								clientCurrency = clientPropertyPooler.getCurrencyId(this.clientId.trim());
								
								// Check business model, prepaid or postpaid from clientPropertyPooler
								String businessModel = clientPropertyPooler.getBusinessMode(this.clientId).trim();
								System.out.print("Business model: " + businessModel);
								
								// routeId = clientSenderIdId + "-" + telecomId
								String routeId = senderId + "-" + this.clientId + "-" + this.systemId + "-" + telecomId;
								JSONObject jsonRoute = RouteSMSPooler.jsonRouteSMSProperty.getJSONObject(routeId);
								
								// Get client price from jsonRoute
								clientPricePerSubmit = jsonRoute.getDouble("clientPricePerSubmit");

								boolean isBalanceEnough = true;
								if(businessModel.equals("PREPAID")){
									double divisionBalance = clientBalancePooler.getClientBalance(clientId);
									
									if(divisionBalance > eliandrieBalanceThreshold && divisionBalance > clientPricePerSubmit){
										// Deduction balance - Function balance deduction is moved to ROUTER module
//										clientBalancePooler.deductClientBalance(messageId, clientId, clientPrice, "CORESMPPSMS");
//										LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
//												"Client " + clientId + " balance is deducted by " + String.format("%.2f", clientPrice), null);				
										isBalanceEnough = true;
									} else {
										isBalanceEnough = false;
									}
								}
								
								System.out.println(messageId + " ISBALANCE ENOUGH: " + isBalanceEnough);

								if(isBalanceEnough == true){
									LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
											"Check BALANCE - ENOUGH!", null);	
									
									System.out.println(messageId + " BALANCE IS ENOUGH!!!");									

									// Save initial data and send DR. It has to be after each if-else for multithread racing with DLR
									JSONObject jsonCompleteResponse = new JSONObject();
									
									String deliveryStatus = "ACCEPTED";

									jsonCompleteResponse.put("SMPPDeliveryState", deliveryStatus);
									jsonCompleteResponse.put("errorCode", errorCode);
									
									System.out.println(messageId + " SAVING INITIAL DATA");									

									// For now just support GSM7
									String messageEncoding = "GSM7";
									saveInitialData(messageId, now, "SMPPAPI", "SMPP: " + jsonIncoming.toString(), jsonCompleteResponse.toString(), 
										this.remoteIpAddress, now, now, msisdn, theSMS, countryCode, prefix, telecomId, errorCode, "SMPP", senderIdId, senderId, this.clientId, 
										this.systemId, clientPricePerSubmit, clientCurrency, messageEncoding, getGSM7MessageLength(theSMS), getSmsCount(theSMS, messageEncoding), deliveryStatus);
									
									System.out.println(messageId + " MAKING JSONINCOMING");									

									// Submit to Queue for further process
									jsonIncoming.put("messageId", messageId);
									jsonIncoming.put("receiverDateTime", now.format(formatter));
									jsonIncoming.put("transactionDateTime", now.format(formatter));
									jsonIncoming.put("msisdn", msisdn);
									jsonIncoming.put("message", theSMS);
									jsonIncoming.put("telecomId", telecomId);
									jsonIncoming.put("countryCode", countryCode);
									jsonIncoming.put("prefix", prefix);
									jsonIncoming.put("errorCode", errorCode);
									jsonIncoming.put("apiUserName", this.systemId);
									jsonIncoming.put("clientSenderIdId", senderIdId); // senderIdId-clientId
									jsonIncoming.put("clientSenderId", senderId);
									jsonIncoming.put("clientId", this.clientId);
									jsonIncoming.put("apiUserName", this.systemId);
									jsonIncoming.put("clientIpAddress", this.remoteIpAddress);
									jsonIncoming.put("receiverType", "SMPP"); // SMPP and HTTP only
									jsonIncoming.put("smsChannel", "SMPP");
									jsonIncoming.put("sysSessionId", this.sysSessionId);
					            	jsonIncoming.put("messageLength", getGSM7MessageLength(theSMS));
					            	jsonIncoming.put("messageCount", getSmsCount(theSMS, messageEncoding));
					            	jsonIncoming.put("clientPricePerSubmit", clientPricePerSubmit);

									LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
											"jsonIncoming: " + jsonIncoming.toString(), null);				

									System.out.println(messageId + " ABOUT TO SEND TO QUEUE");									

									Channel channel = rabbitMqPooler.getChannel(connection);
									
									channel.queueDeclare(SMSQueueName, true, false, false, null);
									channel.basicPublish("", SMSQueueName, MessageProperties.PERSISTENT_TEXT_PLAIN, jsonIncoming.toString().getBytes());
									LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
											"jsonIncoming: " + jsonIncoming.toString() + " published SUCCESSfully!", null);
									
									channel.close();
									
									System.out.println(messageId + " SEND TO QUEUE " + SMSQueueName);									
								} else {
									System.out.println(messageId + " BALANCE IS NOT ENOUGH!!!");									

									// Balance is not enough
									clientPricePerSubmit = 0; // clientPrice is resetted
									//deliveryState = SmppConstants.STATE_REJECTED; 	// BALANCE IS NOT ENOUGH
									errorCode = "122";								// BALANCE IS NOT ENOUGH							
									LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
											"Balance is NOT ENOUGH. deliveryState: STATE_REJECTED, errorCode: 122", null);			
									
									// Save initial data and send DR. It has to be after each if-else for multithread racing with DLR
									JSONObject jsonCompleteResponse = new JSONObject();
									
									String deliveryStatus = "REJECTED";

									jsonCompleteResponse.put("SMPPDeliveryState", deliveryStatus);
									jsonCompleteResponse.put("errorCode", errorCode);
									
									saveInitialData(messageId, now, "SMPPAPI", "SMPP: " + jsonIncoming.toString(), jsonCompleteResponse.toString(), 
										this.remoteIpAddress, now, now, msisdn, theSMS, countryCode, prefix, telecomId, errorCode, "SMPP", senderIdId, senderId, this.clientId, 
										this.systemId, clientPricePerSubmit, clientCurrency, "GSM7", theSMS.length(), getSmsCount(theSMS, "GSM7"), deliveryStatus);
									
//									sendDeliveryReceipt(session, messageId, mtDestinationAddress, mtSourceAddress, dataCoding, deliveryState, errorCode, theSMS);
//									LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
//											"Delivery receipt is SENT. messageId: " + messageId, null);				
//									//sendMoMessage(session, mtDestinationAddress, mtSourceAddress, shortMessage, dataCoding);					

								}			
							} else {
								System.out.println(messageId + " ROUTE IS NOT ENOUGH!!!");									

								// Route is not DEFINED
								//deliveryState = SmppConstants.STATE_REJECTED; 	// ROUTE NOT DEFINED
								errorCode = "900";								// ROUTE NOT DEFINED												
								LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
										"Check ROUTE - NOT DEFINED! deliveryState: STATE_REJECTED, errorCode: 900", null);		
								
								// Save initial data and send DR. It has to be after each if-else for multithread racing with DLR
								JSONObject jsonCompleteResponse = new JSONObject();
								
								String deliveryStatus = "REJECTED";

								jsonCompleteResponse.put("SMPPDeliveryState", deliveryStatus);
								jsonCompleteResponse.put("errorCode", errorCode);
								
								saveInitialData(messageId, now, "SMPPAPI", "SMPP: " + jsonIncoming.toString(), jsonCompleteResponse.toString(), 
									this.remoteIpAddress, now, now, msisdn, theSMS, countryCode, prefix, telecomId, errorCode, "SMPP", senderIdId, senderId, this.clientId, 
									this.systemId, clientPricePerSubmit, clientCurrency, "GSM7", theSMS.length(), getSmsCount(theSMS, "GSM7"), deliveryStatus);
								
//								sendDeliveryReceipt(session, messageId, mtDestinationAddress, mtSourceAddress, dataCoding, deliveryState, errorCode, theSMS);
//								LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
//										"Delivery receipt is SENT. messageId: " + messageId, null);				
//								//sendMoMessage(session, mtDestinationAddress, mtSourceAddress, shortMessage, dataCoding);					

							}
						} else {
							System.out.println(messageId + " SENDERID IS INVALID!!!");									

							// senderId is NOT valid
							//deliveryState = SmppConstants.STATE_REJECTED; 	// INVALID SENDERID
							errorCode = "121";								// INVALID SENDERID															
							LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
									"Check ROUTE - NOT DEFINED! deliveryState: STATE_REJECTED, errorCode: 121", null);	
							
							// Save initial data and send DR. It has to be after each if-else for multithread racing with DLR
							JSONObject jsonCompleteResponse = new JSONObject();
							
							String deliveryStatus = "REJECTED";

							jsonCompleteResponse.put("SMPPDeliveryState", deliveryStatus);
							jsonCompleteResponse.put("errorCode", errorCode);
							
							saveInitialData(messageId, now, "SMPPAPI", "SMPP: " + jsonIncoming.toString(), jsonCompleteResponse.toString(), 
								this.remoteIpAddress, now, now, msisdn, theSMS, countryCode, prefix, telecomId, errorCode, "SMPP", senderIdId, senderId, this.clientId, 
								this.systemId, clientPricePerSubmit, clientCurrency, "GSM7", theSMS.length(), getSmsCount(theSMS, "GSM7"), deliveryStatus);
							
//							sendDeliveryReceipt(session, messageId, mtDestinationAddress, mtSourceAddress, dataCoding, deliveryState, errorCode, theSMS);
//							LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
//									"Delivery receipt is SENT. messageId: " + messageId, null);				
//							//sendMoMessage(session, mtDestinationAddress, mtSourceAddress, shortMessage, dataCoding);					

						}							
					} else {
						System.out.println(messageId + " PREFIX IS NOT VALID");									

						// Prefix is not valid
						//deliveryState = SmppConstants.STATE_REJECTED; 		// UNREGISTERED PREFIX
						errorCode = "113";									// UNREGISTERED PREFIX
						LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
								"Check ROUTE - NOT DEFINED! deliveryState: STATE_REJECTED, errorCode: 113", null);	
						
						// Save initial data and send DR. It has to be after each if-else for multithread racing with DLR
						JSONObject jsonCompleteResponse = new JSONObject();
						
						String deliveryStatus = "REJECTED";

						jsonCompleteResponse.put("SMPPDeliveryState", deliveryStatus);
						jsonCompleteResponse.put("errorCode", errorCode);
						
						saveInitialData(messageId, now, "SMPPAPI", "SMPP: " + jsonIncoming.toString(), jsonCompleteResponse.toString(), 
							this.remoteIpAddress, now, now, msisdn, theSMS, countryCode, prefix, telecomId, errorCode, "SMPP", senderIdId, senderId, this.clientId, 
							this.systemId, clientPricePerSubmit, clientCurrency, "GSM7", theSMS.length(), getSmsCount(theSMS, "GSM7"), deliveryStatus);
						
//						sendDeliveryReceipt(session, messageId, mtDestinationAddress, mtSourceAddress, dataCoding, deliveryState, errorCode, theSMS);
//						LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
//								"Delivery receipt is SENT. messageId: " + messageId, null);				
//						//sendMoMessage(session, mtDestinationAddress, mtSourceAddress, shortMessage, dataCoding);					
					}

					System.out.println(messageId + " IS HANDLED!");

					// Create PduResponse
					SubmitSmResp submitSmResponse = mt.createResponse();
					submitSmResponse.setMessageId(messageId);
					
					// Sending delivery receipt
					// Sending dlr only for failed one.
					if(!errorCode.equals("002")){
						now = LocalDateTime.now();					
						SMPPDLRPooler.sendSMPPDLR(messageId, msisdn, senderId, theSMS, errorCode, now, "", this.sysSessionId);
						LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", false, false, false, messageId, 
								"Delivery receipt is SENT. messageId: " + messageId, null);				
					}

					response = submitSmResponse;
					System.out.println(messageId + " IS RESPONDED!");
				} catch (Exception e) {
					e.printStackTrace();
					LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - firePduRequestReceived", true, false, false, messageId, 
							"Failed to process the incoming message. Error occured.", e);				
				}
			}
			
			System.out.println("Sending Response: " + response.toString());
			return response;
		}

//		private void sendMoMessage(SmppSession session, Address moSourceAddress, Address moDestinationAddress, byte [] textBytes, byte dataCoding) {
//			DeliverSm deliver = new DeliverSm();
//
//			deliver.setSourceAddress(moSourceAddress);
//			deliver.setDestAddress(moDestinationAddress);
//			deliver.setDataCoding(dataCoding);
//			try {
//				deliver.setShortMessage(textBytes);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//
//			sendRequestPdu(session, deliver);
//		}

//		private void sendRequestPdu(String messageId, SmppSession session, DeliverSm deliver) {
//			try {
//				@SuppressWarnings("rawtypes")
//				WindowFuture<Integer,PduRequest,PduResponse> future = session.sendRequestPdu(deliver, 2000, false);
//				if (!future.await()) {
//					LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - sendRequestPdu", false, false, false, messageId, 
//							"Failed to receive deliver_sm_resp within specified time.", null);	
//				} else if (future.isSuccess()) {
//					DeliverSmResp deliverSmResp = (DeliverSmResp)future.getResponse();
//					LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - sendRequestPdu", false, false, false, messageId, 
//							"deliver_sm_resp: commandStatus [" + deliverSmResp.getCommandStatus() + "=" + deliverSmResp.getResultMessage() + "]", null);	
//				} else {
//					LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - sendRequestPdu", false, false, false, messageId, 
//							"Failed to properly receive deliver_sm_resp: " + future.getCause(), null);	
//				}
//			} catch (Exception e) {e.printStackTrace();}
//		}

//		private void sendDeliveryReceipt(SmppSession session, String messageId, Address mtDestinationAddress, Address mtSourceAddress, byte dataCoding, byte deliveryState, String errorCode, String smsMessage) {
//			try {
//				DeliveryReceipt dlr = new DeliveryReceipt();
//				dlr.setMessageId(messageId);
//				dlr.setSubmitCount(1);
//				dlr.setDeliveredCount(0);
//				//dlr.setSubmitDate(new DateTime(2010, 5, 23, 20, 39, 0, 0, DateTimeZone.UTC));
//				dlr.setSubmitDate(new DateTime());
//				//dlr.setDoneDate(new DateTime(2010, 5, 24, 23, 39, 0, 0, DateTimeZone.UTC));
//				dlr.setDoneDate(new DateTime());
//				//dlr.setState(SmppConstants.STATE_DELIVERED);
//				dlr.setState(deliveryState);
//				//dlr.setErrorCode(12);
//				
//				// change errorCode to int
//				int iErrorCode = 0;
//				if(errorCode.equals("000")){
//					iErrorCode = 0;
//				} else if(errorCode.equals("001") || errorCode.equals("002") || errorCode.equals("003")){
//					iErrorCode = 1;
//				} else {
//					iErrorCode = Integer.parseInt(errorCode);
//				}
//				
//				dlr.setErrorCode(iErrorCode);
//				dlr.setText(smsMessage.substring(0, 20));
//
//				String receipt0 = dlr.toShortMessage();
//
//				DeliverSm deliver = new DeliverSm();
//				deliver.setEsmClass(SmppConstants.ESM_CLASS_MT_SMSC_DELIVERY_RECEIPT);
//				deliver.setSourceAddress(mtDestinationAddress);
//				deliver.setDestAddress(mtSourceAddress);
//				deliver.setDataCoding(dataCoding);
//				if(smsMessage.trim().length() > 0){
//					//deliver.setShortMessage(smsMessage.substring(0, 20).getBytes());
//					//deliver.setShortMessage("PAIJO PERGI KEPASAR".getBytes());
//					deliver.setShortMessage(receipt0.getBytes());
//				}
//
//				execDLRService.execute(new sendPDUOut(messageId, session, deliver));
//				// sendRequestPdu(messageId, session, deliver);							 
//			} catch (Exception e) {
//				e.printStackTrace();
//				LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "SmppSessionHandler - sendDeliveryReceipt", true, false, false, messageId, 
//						"Failed to send deliveryReceipt. Error occured.", e);	
//			}
//		}
		
//		private class sendPDUOut implements Runnable{
//			private String messageId;
//			private SmppSession session;
//			private DeliverSm deliver;
//			
//			public sendPDUOut(String messageId, SmppSession session, DeliverSm deliver) {
//				this.messageId = messageId;
//				this.session = session;
//				this.deliver = deliver;
//			}
//
//			private void sendRequestPdu() throws RecoverablePduException, UnrecoverablePduException, SmppTimeoutException, SmppChannelException, InterruptedException {
//				@SuppressWarnings("rawtypes")
//				WindowFuture<Integer, PduRequest,PduResponse> future = this.session.sendRequestPdu(this.deliver, 30000, false);
//				if (!future.await()) {
//					LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "sendPDUOut - sendRequestPdu", false, false, false, this.messageId, 
//							"Failed to receive deliver_sm_resp within specified time.", null);	
//				} else if (future.isSuccess()) {
//					DeliverSmResp deliverSmResp = (DeliverSmResp) future.getResponse();
//					LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "sendPDUOut - sendRequestPdu", false, false, false, this.messageId, 
//							"deliver_sm_resp: commandStatus [" + deliverSmResp.getCommandStatus() + "=" + deliverSmResp.getResultMessage() + "]", null);	
//				} else {
//					LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "sendPDUOut - sendRequestPdu", false, false, false, this.messageId, 
//							"Failed to properly receive deliver_sm_resp: " + future.getCause(), null);	
//				}
//
////				try {
////					@SuppressWarnings("rawtypes")
////					WindowFuture<Integer, PduRequest,PduResponse> future = this.session.sendRequestPdu(this.deliver, 2000, false);
////					if (!future.await()) {
////						LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "sendPDUOut - sendRequestPdu", false, false, false, this.messageId, 
////								"Failed to receive deliver_sm_resp within specified time.", null);	
////					} else if (future.isSuccess()) {
////						DeliverSmResp deliverSmResp = (DeliverSmResp) future.getResponse();
////						LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "sendPDUOut - sendRequestPdu", false, false, false, this.messageId, 
////								"deliver_sm_resp: commandStatus [" + deliverSmResp.getCommandStatus() + "=" + deliverSmResp.getResultMessage() + "]", null);	
////					} else {
////						LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo", "sendPDUOut - sendRequestPdu", false, false, false, this.messageId, 
////								"Failed to properly receive deliver_sm_resp: " + future.getCause(), null);	
////					}
////				} catch (Exception e) {e.printStackTrace();}
//			}
//			
//			@Override
//			public void run() {
//				try{
//					sendRequestPdu();
//				} catch( Exception e ){
//					// Put to queue to send later
//					e.printStackTrace();
//					
////					System.out.println("Sleep 1 second and the resend it!");
////					try{
////						Thread.sleep(10000);
////					} catch (Exception ex) {
////						ex.printStackTrace();
////					}
//				}
//			}		
//		}
	}
}
