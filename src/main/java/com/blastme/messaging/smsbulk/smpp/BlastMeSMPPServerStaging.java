package com.blastme.messaging.smsbulk.smpp;

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.joda.time.DateTime;
import org.json.JSONObject;
import org.postgresql.util.Base64;

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
import com.cloudhopper.commons.charset.Charset;
import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.commons.gsm.GsmUtil;
import com.cloudhopper.commons.util.windowing.WindowFuture;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppServerConfiguration;
import com.cloudhopper.smpp.SmppServerHandler;
import com.cloudhopper.smpp.SmppServerSession;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.SmppSessionConfiguration;
import com.cloudhopper.smpp.impl.DefaultSmppServer;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.BaseBind;
import com.cloudhopper.smpp.pdu.BaseBindResp;
import com.cloudhopper.smpp.pdu.DeliverSm;
import com.cloudhopper.smpp.pdu.DeliverSmResp;
import com.cloudhopper.smpp.pdu.EnquireLink;
import com.cloudhopper.smpp.pdu.PduRequest;
import com.cloudhopper.smpp.pdu.PduResponse;
import com.cloudhopper.smpp.pdu.SubmitSm;
import com.cloudhopper.smpp.pdu.SubmitSmResp;
import com.cloudhopper.smpp.ssl.SslConfiguration;
import com.cloudhopper.smpp.tlv.Tlv;
import com.cloudhopper.smpp.type.Address;
import com.cloudhopper.smpp.type.SmppChannelException;
import com.cloudhopper.smpp.type.SmppProcessingException;
import com.cloudhopper.smpp.util.DeliveryReceipt;
import com.cloudhopper.smpp.util.SmppUtil;
import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;

import io.lettuce.core.api.sync.RedisCommands;

public class BlastMeSMPPServerStaging {
	private static Logger logger;	
	
	private final static int smppPort = 2778;
	private final static int smppMaxConnection = 500;
	private final static int smppRequestExpiryTimeout = 30000;
	private final static int smppWindowMonitorInterval = 15000;
	private final static int smppWindowSize = 500;
	
	private final static double eliandrieBalanceThresholdRupiah = 50000.00;
	private final static double eliandrieBalanceThresholdUSD = 1.00;
	private final static double eliandrieBalanceThresholdEuro = 1.00;
	private final static double eliandrieBalanceThresholdRMB = 10.00;
	private final static double eliandrieBalanceThresholdSMS = 100;
		
	private static RedisPooler redisPooler;
	private static RedisCommands<String, String> redisCommand;

	private static String SMSQueueName = "SMPP_INCOMING";
	
	public BlastMeSMPPServerStaging() {
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
		
		try {
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
			
			// Initiate SenderIdSMSPooler
			new SenderIdSMSPooler();
		} catch (Exception e) {
			e.printStackTrace();
			
			System.exit(-1);
		}
			
	}
	
	private void startSMPPServer() {
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
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("SmppServerSessionWindowMonitorPool-" + sequence.getAndIncrement());
                return t;
            }
        });

        try {
        	// create a server configuration
	        SmppServerConfiguration configuration = new SmppServerConfiguration();
	        configuration.setPort(smppPort);
	        configuration.setMaxConnectionSize(smppMaxConnection);
	        configuration.setNonBlockingSocketsEnabled(true);
	        configuration.setDefaultRequestExpiryTimeout(smppRequestExpiryTimeout);
	        configuration.setDefaultWindowMonitorInterval(smppWindowMonitorInterval);
	        configuration.setDefaultWindowSize(smppWindowSize);
	        configuration.setDefaultWindowWaitTimeout(configuration.getDefaultRequestExpiryTimeout());
	        configuration.setDefaultSessionCountersEnabled(true);
	        configuration.setJmxEnabled(true);
	        
//	        // Enable smpp configuration for SSL configuration
//	        SslConfiguration sslConfig = new SslConfiguration();
//	        sslConfig.setKeyStorePath("/app/certificate/pintar2020.jks");
//	        sslConfig.setKeyStorePassword("cakep123");
//	        //sslConfig.setKeyManagerPassword("cakep123");
//	        sslConfig.setTrustStorePath("/app/certificate/pintar2020.jks");
//	    	sslConfig.setTrustStorePassword("cakep123");
//	        //sslConfig.setTrustAll(true); //- Kalao gak ada ditemukan jks nya, makanya trust all certificate
//	        
//	        configuration.setSslConfiguration(sslConfig);
//	        configuration.setUseSsl(true);
	
	        // create a server, start it up
	        DefaultSmppServer smppServer = new DefaultSmppServer(configuration, new DefaultSmppServerHandler(), executor, monitorExecutor);
	
	        logger.info("Starting SMPP server...");
			smppServer.start();
	        logger.info("SMPP server started");
		} catch (SmppChannelException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public static void main(String[] args) {
		BlastMeSMPPServerStaging blastmeSmppServer = new BlastMeSMPPServerStaging();
		blastmeSmppServer.startSMPPServer();
	}

	// CLASS SMPP SERVER HANDLER - This class just call once
	public static class DefaultSmppServerHandler implements SmppServerHandler {
		private UserAPISMPPSMSPooler userApiSMPPSMSPooler;
		private SMPPEnquiryLinkPooler smppEnquiryLinkPooler;
		private ClientBalancePooler clientBalancePooler;
		private SMSTransactionOperationPooler smsTransactionOperationPooler;
		private ClientPropertyPooler clientPropertyPooler;
		private RabbitMQPooler rabbitMqPooler;
		private Connection rabbitMqConnection;

		private Tool tool;
		
		private static HashMap<String, SmppServerSession> mapSession = new HashMap<String, SmppServerSession>();
		
		//private final static ExecutorService execClientDLR = Executors.newFixedThreadPool(1);
		private final static ScheduledThreadPoolExecutor execClientDLR = new ScheduledThreadPoolExecutor(1);
		
		private final static ScheduledThreadPoolExecutor execClientMOWAMessage = new ScheduledThreadPoolExecutor(1);
		
        public DefaultSmppServerHandler() {
        	try {
        		// Initiate tool
        		tool = new Tool();
        		
        		userApiSMPPSMSPooler = new UserAPISMPPSMSPooler();
        		System.out.println("USERAPISMPPSMSPooler is initiated.");
        		
        		smppEnquiryLinkPooler = new SMPPEnquiryLinkPooler();
        		System.out.println("SMPPEnquiryLinkPooler is initiated.");
        		
        		clientBalancePooler = new ClientBalancePooler();
        		System.out.println("ClientBalancePooler is initiated.");
        		
        		smsTransactionOperationPooler = new SMSTransactionOperationPooler();
        		System.out.println("SMSTransactionPooler is initiated.");
        		
        		clientPropertyPooler = new ClientPropertyPooler();
        		System.out.println("ClientPropertyPooler is initiated.");
        		
        		rabbitMqPooler = new RabbitMQPooler();
        		System.out.println("RabbitMQPooler is initiated.");
        		
        		rabbitMqConnection = rabbitMqPooler.getConnection();
        		System.out.println("RabbitMQConnection is initiated.");
        		
        		// Run executor DLR Client
        		System.out.println("Executing CLIENTDLRSUBMITTER.");
        		//execClientDLR.execute(new ClientDLRSubmitter());
        		execClientDLR.schedule(new ClientDLRSubmitter(), 20, TimeUnit.SECONDS);
        		
        		execClientMOWAMessage.schedule(new ClientWAMessageSubmitter(), 10, TimeUnit.SECONDS);
        	} catch (Exception e) {
        		e.printStackTrace();
        	}
		}

		@SuppressWarnings("rawtypes")
		@Override
        public void sessionBindRequested(Long sessionId, SmppSessionConfiguration sessionConfiguration, final BaseBind bindRequest) throws SmppProcessingException {
            // test name change of sessions
            // this name actually shows up as thread context....
            sessionConfiguration.setName("Application.SMPP." + sessionConfiguration.getSystemId());
            
            String systemId = sessionConfiguration.getSystemId();
            String password = sessionConfiguration.getPassword();
            String remoteIpAddress = sessionConfiguration.getHost();
            String clientId = userApiSMPPSMSPooler.getClientId(systemId);
            
			LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServer - DefaultSmppServerHandler", "sessionBindRequested", false, false, false, "", 
					"Incoming binding request - systemId: " + systemId + ", password: " + password + ", remoteIpAddress: " + remoteIpAddress, null);				
            
            // Verify systemId, password and remoteHost
			JSONObject jsonValidation = userApiSMPPSMSPooler.isValidSysId(systemId, password, remoteIpAddress);
			LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo - DefaultSmppServerHandler", "sessionBindRequested", false, false, false, "", 
					"jsonValidation: " + jsonValidation.toString(), null);				
			
			if(jsonValidation.getInt("status") != 0){
	            // Save to smpp bind attempt log
	        	smsTransactionOperationPooler.saveSMPPBindAttempt(String.valueOf(sessionId), LocalDateTime.now(), systemId, remoteIpAddress, "BINDING REQUESTED", "FAILED TO BIND. INVALID SYSID/PASSWORD/REMOTE IP ADDRESS.");

				LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo - DefaultSmppServerHandler", "sessionBindRequested", false, false, false, "", 
						"Failed to validate! jsonValidation: " + jsonValidation.toString(), null);				
				throw new SmppProcessingException(SmppConstants.STATUS_INVSYSID, null);
			} else {
	            // Save to smpp bind attempt log
	        	smsTransactionOperationPooler.saveSMPPBindAttempt(String.valueOf(sessionId), LocalDateTime.now(), systemId, remoteIpAddress, "BINDING REQUESTED", "PROCESSING");

				LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServerNeo - DefaultSmppServerHandler", "sessionBindRequested", false, false, false, "", 
						"systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress + ", clientId: " + clientId + " success to validate.", null);				
			}
        }

        @Override
        public void sessionCreated(Long sessionId, SmppServerSession session, BaseBindResp preparedBindResponse) throws SmppProcessingException {
            logger.info("Session created: {}", session);
            
            String systemId = session.getConfiguration().getSystemId();
            //String password = session.getConfiguration().getPassword();
            String remoteIpAddress = session.getConfiguration().getHost();
            String clientId = userApiSMPPSMSPooler.getClientId(systemId);
            
            // Assign sessionId
            String blastmeSessionId = systemId + "-" + tool.generateUniqueID();
            
            // Put session into mapSession
            mapSession.put(blastmeSessionId, session);
            
            // Name the session with blastmessSessionId
            session.getConfiguration().setName(blastmeSessionId);
            
            // Save to smpp bind attempt log
        	smsTransactionOperationPooler.saveSMPPBindAttempt(String.valueOf(sessionId), LocalDateTime.now(), systemId, remoteIpAddress, "BINDING SESSION CREATED", "SUCCESS - SESSION CREATED");

			LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServerNeo - DefaultSmppServerHandler", "sessionCreated", false, false, false, "", 
					"Session created fro systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress + ", clientId: " + 
					clientId + " -> blastmeSessionId: " + blastmeSessionId + ". Session NAME: " + session.getConfiguration().getName(), null);				
            
			session.serverReady(new BlastMeSmppSessionHandler(session, blastmeSessionId, remoteIpAddress, clientId, tool, smppEnquiryLinkPooler, clientPropertyPooler, clientBalancePooler, smsTransactionOperationPooler, rabbitMqPooler, rabbitMqConnection));
        }

        @Override
        public void sessionDestroyed(Long sessionId, SmppServerSession session) {
            logger.info("Session destroyed: {}", session);
            // print out final stats
            if (session.hasCounters()) {
                logger.info(" final session rx-submitSM: {}", session.getCounters().getRxSubmitSM());
            }

            // Save to smpp bind attempt log
        	smsTransactionOperationPooler.saveSMPPBindAttempt(String.valueOf(sessionId), LocalDateTime.now(), session.getConfiguration().getSystemId(), session.getConfiguration().getHost(), "BINDING DESTROY", "SUCCESS - SESSION DESTROYED");

            // make sure it's really shutdown
            session.destroy();
        }
        
        class ClientDLRSubmitter implements Runnable {

			private static final String DLRQUEUE = "SMPP_DLR_TLS";
			private static final int clientDlrTPS = 100;
			
			private RabbitMQPooler rabbitMqPooler;
			private Channel channel;

			private RedisPooler redisPooler;
			private RedisCommands<String, String> redisCommand;
        	
			public ClientDLRSubmitter() {
				try {
					rabbitMqPooler = new RabbitMQPooler();
				
					// Initiate redisPooler
					redisPooler = new RedisPooler();
					redisCommand = redisPooler.redisInitiateConnection();
				
					Connection connection = rabbitMqPooler.getConnection();
					channel = rabbitMqPooler.getChannel(connection);
					
					channel.queueDeclare(DLRQUEUE, true, false, false, null);
					
					// Initiate SMPPDLRPooler
					new SMPPDLRPooler();
					
					LoggingPooler.doLog(logger, "INFO", "BlastMeSMPPServer - ClientDLRSubmmiter", "ClientDLRSubmmiter", false, false, false, "", 
							"Successfully initialize channel rabbitMq to queueName " + DLRQUEUE, null);
				} catch (IOException e) {
					LoggingPooler.doLog(logger, "INFO", "BlastMeSMPPServer - ClientDLRSubmmiter", "ClientDLRSubmmiter", true, false, true, "", 
							"Failed to initialize channle rabbitMq to queueName " + DLRQUEUE, e);
				}
			}
			
	        private void sendDeliveryReceipt(SmppSession session, String messageId, Address mtDestinationAddress, Address mtSourceAddress, byte[] shortMessage, byte dataCoding) {

	            DeliverSm deliver = new DeliverSm();
	            deliver.setEsmClass(SmppConstants.ESM_CLASS_MT_SMSC_DELIVERY_RECEIPT);
	            deliver.setSourceAddress(mtDestinationAddress);
	            deliver.setDestAddress(mtSourceAddress);
	            deliver.setDataCoding(dataCoding);
	            try {
	                deliver.setShortMessage(shortMessage);
	            } catch (Exception e) {
	            	e.printStackTrace();
	            }
	            sendRequestPdu(session, messageId, deliver);
	        }

	        @SuppressWarnings("rawtypes")
			private void sendRequestPdu(SmppSession session, String messageId, DeliverSm deliver) {
	            try {
	                WindowFuture<Integer,PduRequest,PduResponse> future = session.sendRequestPdu(deliver, 10000, false);
	                
	                String clientResp = "";
	                if (!future.await()) {
	                	clientResp = "Failed to receiver DELIVER_SM_RESP within specified time";
	                } else if (future.isSuccess()) {
	                	DeliverSmResp deliverSmResp = (DeliverSmResp)future.getResponse();
	                	clientResp = "deliver_sm_resp: commandStatus [" + deliverSmResp.getCommandStatus() + "=" + deliverSmResp.getResultMessage() + "]";
	                } else {
	                    clientResp = "Failed to properly receive deliver_sm_resp: " + future.getCause();
	                }
					LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServer - ClientDLRSubmmiter", "sendRequestPdu", false, false, false, "", 
							clientResp, null);
					
					// Update transaction DLR for clientResponse
					smsTransactionOperationPooler.insertTransactionDLRClientResponse(messageId, clientResp);
					LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServer - ClientDLRSubmmiter", "sendRequestPdu", false, false, false, "", 
							"Client response is saved to table transacion_sms_dlr.", null);
	            } catch (Exception e) {
	            	e.printStackTrace();
					LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServer - ClientDLRSubmmiter", "sendRequestPdu", true, false, false, "", 
							"Failed to send PDU to client. Error occured.", e);
	            }
	        }

			
			private void proccessClientDLR(String queueMessage) {
				LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServer - ClientDLRSubmmiter", "proccessClientDLR", false, false, false, "", 
						"Processing queue DRL with message: " + queueMessage, null);
				
				try{
					JSONObject jsonMessage = new JSONObject(queueMessage);
					
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
					
					LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServer - ClientDLRSubmmiter", "proccessClientDLR", false, false, false, messageId, 
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
					
					LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServer - ClientDLRSubmmiter", "proccessClientDLR", false, false, false, messageId, 
							"messageId: " + messageId + ", theSMS: " + theSMS, null);
					
					// Get the SMPPSession
					SmppServerSession theSession = null;
					
					if (mapSession.containsKey(sysSessionId)) {
						theSession = mapSession.get(sysSessionId);
					} else {
						// Cari mapSession dengan sysSessionId starts with systemId
						for (Entry<String, SmppServerSession> entry: mapSession.entrySet()) {
							if (entry.getKey().startsWith(theSysId)) {
								theSession = entry.getValue();
							}
						}
					}
					
					LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServer - ClientDLRSubmmiter", "proccessClientDLR", false, false, false, messageId, 
							"DLR sysId: " + theSysId + " -> found matching SMPPSession: " + theSession.getConfiguration().getName(), null);

					
					// Prepare the DLR
					int submitCount = 0;
					int deliveredCount = 0;
					byte deliveryState = SmppConstants.STATE_DELIVERED;
					if(status.equals("000")) {
						submitCount = 1;
						deliveredCount = 1;
						deliveryState = SmppConstants.STATE_DELIVERED;
					} else if(status.equals("002")) {
						submitCount = 1;
						deliveredCount = 0;
						deliveryState = SmppConstants.STATE_ACCEPTED;
					} else {
						submitCount = 1;
						deliveredCount = 0;
						deliveryState = SmppConstants.STATE_REJECTED;
					}
					
    				DeliveryReceipt dlrReceipt = new DeliveryReceipt(messageId, submitCount, deliveredCount, new DateTime(), new DateTime(), deliveryState, status, theSMS);
    									
					// Save to DB transaction_sms_dlr - saving db has to be before sendDeliveryReceipt
					smsTransactionOperationPooler.saveTransactionDLR(messageId, clientId, LocalDateTime.now(), dlrReceipt.toShortMessage(), status, "SMPP session name " + theSession.getConfiguration().getName());
					LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServer - ClientDLRSubmmiter", "proccessClientDLR", false, false, false, messageId, 
							"Data DLR saved in transaction_sms_dlr.", null);

    				Address moSourceAddress = new Address((byte)0x01, (byte)0x01, msisdn);
    				Address moDestinationAddress = new Address((byte)0x03, (byte)0x00, theClientSenderId);
    				byte dataCoding = (byte) 0x00;
    				
    				// Send DLR
    				sendDeliveryReceipt(theSession, messageId, moSourceAddress, moDestinationAddress, dlrReceipt.toShortMessage().getBytes(), dataCoding);
					LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServer - ClientDLRSubmmiter", "proccessClientDLR", false, false, false, messageId, 
							"Sending DLR with session: " + theSession.getConfiguration().getName() + ". DLR: " + dlrReceipt.toShortMessage(), null);
				} catch (Exception e) {
					LoggingPooler.doLog(logger, "INFO", "sendingDLR", "processDlrQueue", true, false, true, "", 
							"Failed to process the DLR message.", e);
					SMPPDLRPooler.sendSMPPPreviouslyFailedDLR("", queueMessage);
				}
			}
			
			private void readDLRQueue() {
				try{				
					LoggingPooler.doLog(logger, "INFO", "BlastMeSMPPServer - ClientDLRSubmmiter", "readDLRQueue", false, false, true, "", 
							"Reading queue " + DLRQUEUE + " for DLR...", null);
					
					// Guava rateLimiter
					RateLimiter rateLimiter = RateLimiter.create(clientDlrTPS);

					Consumer consumer = new DefaultConsumer(channel) {
					      @Override
					      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
					          throws IOException {
					        String message = new String(body, "UTF-8");
					        
							try{
								LoggingPooler.doLog(logger, "INFO", "BlastMeSMPPServer - ClientDLRSubmmiter", "readDLRQueue", false, false, false, "", 
										"Receive message: " + message, null);

								// Limit the speed
								rateLimiter.acquire();
								
								// Send to client via SMPPServer
								proccessClientDLR(message);
							} finally {
								LoggingPooler.doLog(logger, "INFO", "BlastMeSMPPServer - ClientDLRSubmmiter", "readDLRQueue", false, false, false, "", 
										"Done processing message: " + message, null);

									channel.basicAck(envelope.getDeliveryTag(), false);
							}
					      }
					};
					
					boolean autoAck = false; // If not finally exectued well, no ack to rabbitmq, message not gone
					channel.basicConsume(DLRQUEUE, autoAck, consumer);
				} catch (Exception e){
					LoggingPooler.doLog(logger, "INFO", "BlastMeSMPPServer - ClientDLRSubmmiter", "readDLRQueue", true, false, false, "", 
							"Failed to access queue " + DLRQUEUE, e);
				}
			}

			@Override
			public void run() {
				try {
					System.out.println("Starting DLR Submitter ...");
					readDLRQueue();				
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}        	
        }
    
        class ClientWAMessageSubmitter implements Runnable {
			private static final String MessageClientQueue = "SMPP_WA_CLIENT_MESSAGE";
			private static final int clientMessageTPS = 100;
			
			private RabbitMQPooler rabbitMqPooler;
			private Channel channel;

//			private RedisPooler redisPooler;
//			private RedisCommands<String, String> redisCommand;
        	
			public ClientWAMessageSubmitter() {
				try {
					rabbitMqPooler = new RabbitMQPooler();
				
					// Initiate redisPooler
//					redisPooler = new RedisPooler();
//					redisCommand = redisPooler.redisInitiateConnection();
				
					Connection connection = rabbitMqPooler.getConnection();
					channel = rabbitMqPooler.getChannel(connection);
					
					channel.queueDeclare(MessageClientQueue, true, false, false, null);
					
					// Initiate SMPPDLRPooler
					new SMPPDLRPooler();
					
					LoggingPooler.doLog(logger, "INFO", "BlastMeSMPPServer - ClientMessageSubmitter", "ClientMessageSubmmiter", false, false, false, "", 
							"Successfully initialize channel rabbitMq to queueName " + MessageClientQueue, null);
				} catch (IOException e) {
					LoggingPooler.doLog(logger, "INFO", "BlastMeSMPPServer - ClientMessageSubmmiter", "ClientMessageSubmmiter", true, false, true, "", 
							"Failed to initialize channel rabbitMq to queueName " + MessageClientQueue, e);
				}
			}

			private void sendClientMessageRequestPdu(SmppSession session, String messageId, String sourceAddress, String destAddress, String message, String encoding) {
				try {
					byte[] textBytes = CharsetUtil.encode(message, CharsetUtil.CHARSET_GSM);
					
					if (encoding.equals("UCS2")) {
						textBytes = CharsetUtil.encode(message, CharsetUtil.CHARSET_UCS_2);
					}

				    DeliverSm deliver = new DeliverSm();

				    deliver.setSourceAddress(new Address((byte)0x03, (byte)0x00, sourceAddress));
				    deliver.setDestAddress(new Address((byte)0x01, (byte)0x01, destAddress));
				    
				    // Utk message > 255 pake message payload
				    if (textBytes != null && textBytes.length > 255) {
				    	deliver.addOptionalParameter(new Tlv(SmppConstants.TAG_MESSAGE_PAYLOAD, textBytes, "whatsapp_payload"));
				    } else {
				    	deliver.setShortMessage(textBytes); 	
				    }
				   
				    @SuppressWarnings("rawtypes")
					WindowFuture<Integer,PduRequest,PduResponse> future = session.sendRequestPdu(deliver, 10000, false);
				    if (!future.await()) {
				      logger.error("Failed to receive deliver_sm_resp within specified time");
				    } else if (future.isSuccess()) {
				      DeliverSmResp deliverSmResp = (DeliverSmResp)future.getResponse();
				      logger.info("deliver_sm_resp: commandStatus [" + deliverSmResp.getCommandStatus() + "=" + deliverSmResp.getResultMessage() + "]");
				    } else {
				      logger.error("Failed to properly receive deliver_sm_resp: " + future.getCause());
				    }
				 
				} catch (Exception e) {
					LoggingPooler.doLog(logger, "INFO", "BlastMeSMPPServer - ClientMessageSubmmiter", "sendClientMessageRequestPdu", true, false, true, "", 
							"Failed to send message to client. Error occured.", e);
				}
			}
			
			private void proccessClientWABAMessage(String queueMessage) {
				LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServer - ClientWABAMOSubmmiter", "proccessClientWABA", false, false, false, "", 
						"Processing queue WABA to SMPPClient with message: " + queueMessage, null);
				
				try{
					JSONObject jsonMessage = new JSONObject(queueMessage);
					
					String clientId = jsonMessage.getString("clientId");
					String waMessageId = jsonMessage.getString("message_id");
					String contactId = jsonMessage.getString("contact_id");
					String waId = "16502636146";
					String waMessage = queueMessage;
					
					// Get sysSessionId 
					LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServer - ClientDLRSubmmiter", "proccessClientMOWABAMessage", false, false, false, waMessageId, 
							"jsonClientIdToAccess: " + UserAPISMPPSMSPooler.jsonClientIdToAccess.toString(), null);
					
					JSONObject jsonDetail = UserAPISMPPSMSPooler.jsonClientIdToAccess.getJSONObject(clientId);
					LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServer - ClientDLRSubmmiter", "proccessClientMOWABAMessage", false, false, false, waMessageId, 
							"jsonDetail: " + jsonDetail.toString(), null);
					
					if (jsonDetail == null || jsonDetail.length() <= 0) {
						// Do nothing here, gak tahu mau dikirim ke session yang mana
						LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServer - ClientDLRSubmmiter", "proccessClientDLR", false, false, false, waMessageId, 
								"SysID is not found for clientID: " + clientId + ". DO NOTHING!", null);
					} else {
						// Process
						String theSysId = jsonDetail.getString("sysId");
						
						// Get sysSessionId
						SmppServerSession theSession = null;
						for (Entry<String, SmppServerSession> entry: mapSession.entrySet()) {
							if (entry.getKey().startsWith(theSysId)) {
								theSession = entry.getValue();
							}
						}

						LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServer - ClientDLRSubmmiter", "proccessClientDLR", false, false, false, waMessageId, 
								"clientId: " + clientId + " -> SMPP sysId: " + theSysId + " -> found matching SMPPSession: " + theSession.getConfiguration().getName(), null);

						// Send to client using the session
						sendClientMessageRequestPdu(theSession, waMessageId, contactId, waId, waMessage, "UTF-8");	
						LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServer - ClientDLRSubmmiter", "proccessClientDLR", false, false, false, waMessageId, 
								"Sending MO Message with session: " + theSession.getConfiguration().getName() + ". MO Message: " + waMessage, null);
					}
				} catch (Exception e) {
					LoggingPooler.doLog(logger, "INFO", "sendingDLR", "processDlrQueue", true, false, true, "", 
							"Failed to process the DLR message.", e);
					SMPPDLRPooler.sendSMPPPreviouslyFailedDLR("", queueMessage);
				}
			}
			
			private void readClientWAMOMessageQueue() {
				try{				
					LoggingPooler.doLog(logger, "INFO", "BlastMeSMPPServer - ClientDLRSubmmiter", "readClientMOMessageQueue", false, false, true, "", 
							"Reading queue " + MessageClientQueue + " for MO message to client...", null);
					
					// Guava rateLimiter
					RateLimiter rateLimiter = RateLimiter.create(clientMessageTPS);

					Consumer consumer = new DefaultConsumer(channel) {
					      @Override
					      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
					          throws IOException {
					        String message = new String(body, "UTF-8");
					        
							try{
								LoggingPooler.doLog(logger, "INFO", "BlastMeSMPPServer - ClientDLRSubmmiter", "readClientMOMessageQueue", false, false, false, "", 
										"Receive message: " + message, null);

								// Limit the speed
								rateLimiter.acquire();
								
								// Send to client via SMPPServer
								proccessClientWABAMessage(message);
							} finally {
								LoggingPooler.doLog(logger, "INFO", "BlastMeSMPPServer - ClientDLRSubmmiter", "readClientMOMessageQueue", false, false, false, "", 
										"Done processing message: " + message, null);

									channel.basicAck(envelope.getDeliveryTag(), false);
							}
					      }
					};
					
					boolean autoAck = false; // If not finally exectued well, no ack to rabbitmq, message not gone
					channel.basicConsume(MessageClientQueue, autoAck, consumer);
				} catch (Exception e){
					LoggingPooler.doLog(logger, "INFO", "BlastMeSMPPServer - ClientDLRSubmmiter", "readClientMOMessageQueue", true, false, false, "", 
							"Failed to access queue " + MessageClientQueue, e);
				}
			}

			@Override
			public void run() {
				try {
					System.out.println("Starting WhatsAPP SMPP MO Submitter ...");
					readClientWAMOMessageQueue();
				} catch (Exception e) {
					LoggingPooler.doLog(logger, "INFO", "BlastMeSMPPServer - ClientWAMessageSubmmiter", 
							"readClientMOMessageQueue", true, false, false, "", "Error to run WA Message MO Submitter", e);
				}
			}
        }
	}	
	
    public static class BlastMeSmppSessionHandler extends DefaultSmppSessionHandler {
        private WeakReference<SmppSession> sessionRef;
        
        private String systemId;
        private String remoteIpAddress;
        private String clientId;
        private String blastmeSessionId;
        
        // Supporting class
        Tool tool;
        SMPPEnquiryLinkPooler smppEnquiryLinkPooler;
        ClientPropertyPooler clientPropertyPooler;
        ClientBalancePooler clientBalancePooler;
        SMSTransactionOperationPooler smsTransactionOperationPooler;
		RabbitMQPooler rabbitMqPooler;
		Connection rabbitMqConnection;

        public BlastMeSmppSessionHandler(SmppSession session, String theBlastmeSessionId, String theRemoteIpAddress, String theClientId, Tool theTool, 
        		SMPPEnquiryLinkPooler theSmppEnquiryLinkPooler, ClientPropertyPooler theClientPropertyPooler, 
        		ClientBalancePooler theClientBalancePooler, SMSTransactionOperationPooler theSmsTransactionOperationPooler,
        		RabbitMQPooler theRabbitMqPooler, Connection theRabbitMqConnection) {
            this.sessionRef = new WeakReference<SmppSession>(session); 
            this.systemId = session.getConfiguration().getSystemId();
            this.remoteIpAddress = theRemoteIpAddress;
            this.clientId = theClientId;
            this.blastmeSessionId = theBlastmeSessionId;
            
            this.tool = theTool;
            this.smppEnquiryLinkPooler = theSmppEnquiryLinkPooler;
            this.clientPropertyPooler = theClientPropertyPooler;
            this.clientBalancePooler = theClientBalancePooler;
            this.smsTransactionOperationPooler = theSmsTransactionOperationPooler;
            this.rabbitMqPooler = theRabbitMqPooler;
            this.rabbitMqConnection = theRabbitMqConnection;
        }

		private Charset getCharsetByDataCoding(byte dataCoding) {
			Charset theCharSet = CharsetUtil.CHARSET_GSM; // DEFAULT is GSM7
			
        	// character encoding constants
            /** SMSC Default Alphabet (default) */
            //public static final byte CHAR_ENC_DEFAULT = 0x00;
            /** IA5 (CCITT T.50)/ASCII (ANSI X3.4) */
            //public static final byte CHAR_ENC_IA5 = 0x01;
            /** Octet unspecified (8-bit binary) defined for TDMA and/ or CDMA but not defined for GSM */
            //public static final byte CHAR_ENC_8BITA = 0x02;
            /** Latin 1 (ISO-8859-1) */
            //public static final byte CHAR_ENC_LATIN1 = 0x03;
            /** Octet unspecified (8-bit binary) ALL TECHNOLOGIES */
            //public static final byte CHAR_ENC_8BIT = 0x04;
            /** JIS (X 0208-1990) */
            //public static final byte CHAR_ENC_JIS = 0x05;
            /** Cyrllic (ISO-8859-5) */
            //public static final byte CHAR_ENC_CYRLLIC = 0x06;
            /** Latin/Hebrew (ISO-8859-8) */
            //public static final byte CHAR_ENC_HEBREW = 0x07;
            /** UCS2 (ISO/IEC-10646) */
            //public static final byte CHAR_ENC_UCS2 = 0x08;
            /** Pictogram Encoding */
            //public static final byte CHAR_ENC_PICTO = 0x09;
            /** ISO-2022-JP (Music Codes) */
            //public static final byte CHAR_ENC_MUSIC = 0x0A;
            /** Reserved: 0x0B */
            //public static final byte CHAR_ENC_RSRVD = 0x0B;
            /** Reserved: 0x0C */
            //public static final byte CHAR_ENC_RSRVD2 = 0x0C;
            /** Extended Kanji JIS(X 0212-1990) */
            //public static final byte CHAR_ENC_EXKANJI = 0x0D;
            /** KS C 5601 */
            //public static final byte CHAR_ENC_KSC5601 = 0x0E;
            /** Reserved: 0x0F */
            //public static final byte CHAR_ENC_RSRVD3 = 0x0F;
			
			try {
				switch(dataCoding) {
					case (byte) 0x00:	theCharSet = CharsetUtil.CHARSET_GSM;
										break;
//					case (byte) 0x01:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//										break;
//					case (byte) 0x02:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//										break;
//					case (byte) 0x03:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//										break;
					case (byte) 0x04:	theCharSet = CharsetUtil.CHARSET_UCS_2;
										break;
//					case (byte) 0x05:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//										break;
					case (byte) 0x06:	theCharSet = CharsetUtil.CHARSET_UCS_2;
										break;
					case (byte) 0x07:	theCharSet = CharsetUtil.CHARSET_UCS_2;
										break;
					case (byte) 0x08:	theCharSet = CharsetUtil.CHARSET_UCS_2;
										break;
//					case (byte) 0x09:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//										break;
//					case (byte) 0x0A:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//										break;
//					case (byte) 0x0B:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//										break;
//					case (byte) 0x0C:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//										break;
					case (byte) 0x0D:	theCharSet = CharsetUtil.CHARSET_UCS_2;
										break;
//					case (byte) 0x0E:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//										break;
//					case (byte) 0x0F:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//										break;
					default:			theCharSet = CharsetUtil.CHARSET_GSM;
										break;
				}
				
    			LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServerNeo - SMPPIncomingTrxProcessor", "getCharsetByDataCoding - " + this.blastmeSessionId, false, false, false, "", 
    					"dataCoding: " + dataCoding + " -> charset: " + theCharSet, null);				
			} catch (Exception e) {
				e.printStackTrace();
				
    			LoggingPooler.doLog(logger, "DEBUG", "BlastmeSMPPServerNeo - SMPPIncomingTrxProcessor", "getCharsetByDataCoding - " + this.blastmeSessionId, true, false, false, "", 
    					"Failed to get charset of datacoding: " + dataCoding + ". Error occured.", e);				
				
			}
			
			return theCharSet;
		}
				
        private byte[] combineMessage(String origin, String destination, int messageId, int totalMessage, int messageNumber, byte[] shortMessagePart) {
        	byte[] combinedMessage = new byte[] {};
        	
        	int expirySeconds = 24 * 60 * 60;
        	
        	// Convert byte[] to string using base64 to make sure the consistency
        	String encodedByte = Base64.encodeBytes(shortMessagePart);
        	
        	// Tulis ke redis
        	String redisKey = "multipartsms-" + origin + "-" + destination + "-" + messageId + "-" + totalMessage + "-" + messageNumber;
        	String redisVal = encodedByte;
        	
        	redisPooler.redisSetWithExpiry(redisCommand, redisKey, redisVal, expirySeconds);

        	// Check if all messages are already in redis
        	boolean isComplete = false;
        	boolean wasExist = false;
        	
        	System.out.println("isComplete: " + isComplete);        	
        	for (int x = 1; x <= totalMessage; x++) {
        		System.out.println("x: " + x);
        		
            	String redisKeyIn = "multipartsms-" + origin + "-" + destination + "-" + messageId + "-" + totalMessage + "-" + x;
            	String redisValIn = redisPooler.redisGet(redisCommand, redisKeyIn);
            	
            	if (redisValIn == null) {
                	System.out.println(x + ".redisKeyIn: " + redisKeyIn + ", redisValIn IS NULL, DO BREAK!");
                	isComplete = false;
                	wasExist = false;
                	break;
            	} else {
                	System.out.println(x + ". redisKeyIn: " + redisKeyIn + ", redisValIn: " + redisValIn); 

                	// Yang exist sebelumnya harus true, dan x == totalMessage dan redisValIn bukan NILL
                	if (x == totalMessage && wasExist == true) {
                		isComplete = true;
                	}
                	wasExist = true;
                	
                	System.out.println(x + "wasExist: " + wasExist + ", isCOmplete: " + isComplete); 
            	}
        	}
        	
        	// If complete, join the message
        	System.out.println("Checking final ISCOMPLETE: " + isComplete);
        	if (isComplete == true) {            	
            	for (int x = 1; x <= totalMessage; x++) {
                	String redisKeyIn = "multipartsms-" + origin + "-" + destination + "-" + messageId + "-" + totalMessage + "-" + x;
                	String redisValIn = redisPooler.redisGet(redisCommand, redisKeyIn);
                	System.out.println(x + ". redisKeyIn: " + redisKeyIn + ". redisValIn: " + redisValIn);
                	
                	byte[] redisValByte = Base64.decode(redisValIn);
                	System.out.println(x + ". redisValByte: " + Arrays.toString(redisValByte));

                	System.out.println(x + ". combinedMessage: " + Arrays.toString(combinedMessage) + ", redisValByte: " + Arrays.toString(redisValByte));
                	System.out.println(x + ". combinedMessage length: " + combinedMessage.length + ", redisValByte length: " + redisValByte.length);
                	byte[] xByte = new byte[combinedMessage.length + redisValByte.length];
                	System.out.println(x + ". xByte: " + Arrays.toString(xByte));
                	
                	System.arraycopy(combinedMessage, 0, xByte, 0, combinedMessage.length);
                	System.arraycopy(redisValByte, 0, xByte, combinedMessage.length, redisValByte.length);
                	System.out.println("new xByte: " + Arrays.toString(xByte));
                	
                	combinedMessage = xByte; 
            	}
        	} else {
        		// Do nothing
        	}
        	
        	return combinedMessage; 
        }
        
        
        @SuppressWarnings("rawtypes")
		@Override
        public PduResponse firePduRequestReceived(PduRequest pduRequest) {
            SmppSession session = sessionRef.get();

            // Default value of response
            PduResponse response = pduRequest.createResponse();

            if(pduRequest.getCommandId() == SmppConstants.CMD_ID_ENQUIRE_LINK){
				EnquireLink enqResp = new EnquireLink();
				int seqNumber = pduRequest.getSequenceNumber();
				enqResp.setSequenceNumber(seqNumber);
				response = enqResp.createResponse();
				
				System.out.println("Enquire Link Request from " + session.getConfiguration().getSystemId() + " - " + 
				this.clientId + " - " + pduRequest.toString() + " - sequenceNumber: " + seqNumber);
				this.smppEnquiryLinkPooler.logEnquiryLink(clientId, this.blastmeSessionId, "ENQUIRE_LINK");
			} else if (pduRequest.getCommandId() == SmppConstants.CMD_ID_SUBMIT_SM) {
				try {
					this.smppEnquiryLinkPooler.logEnquiryLink(clientId, this.blastmeSessionId, "SUBMIT_SM");

	                // Create messageId
	                String messageId = tool.generateUniqueID();
	                
					// Check if message with UDH
					//DeliverSm moMessage = (DeliverSm) pduRequest;
	                SubmitSm mt = (SubmitSm) pduRequest;
					
					boolean isUdh = SmppUtil.isUserDataHeaderIndicatorEnabled(mt.getEsmClass());
					
					if (isUdh) {
						// Handle multipart sms
		                Address mtSourceAddress = mt.getSourceAddress();
		                String clientSenderId = mtSourceAddress.getAddress().trim();
		                		
		                Address mtDestinationAddress = mt.getDestAddress();
		                String msisdn = mtDestinationAddress.getAddress().trim();

						byte[] userDataHeader = GsmUtil.getShortMessageUserDataHeader(mt.getShortMessage());
						
						int thisMessageId = userDataHeader[3] & 0xff;
						int thisTotalMessages = userDataHeader[4] & 0xff;
						int thisMessageNumber = userDataHeader[5] & 0xff;
						
		                byte dataCoding = mt.getDataCoding();
						byte[] shortMessage = GsmUtil.getShortMessageUserData(mt.getShortMessage());
						
						
		    			Charset theCharset = getCharsetByDataCoding(dataCoding);
		    			
//		    			String messageEncoding = "GSM";
//		    			if (theCharset == CharsetUtil.CHARSET_GSM) {
//		    				messageEncoding = "GSM";
//		    			} else {
//		    				messageEncoding = "UCS2";
//		    			}
		    			
		    			String theSMSPart = CharsetUtil.decode(shortMessage, theCharset);						
						
		    			LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo - BlastMeSmppSessionHandler", "firePduRequestReceived", false, false, false, messageId, 
		    					"Incoming sms is with UDH. The messageId: " + thisMessageId + ", thisTotalMessages: " + thisTotalMessages + ", thisMessageNumber: " +
		    					thisMessageNumber + ", thisSMS: " + theSMSPart, null);		
		    			
		    			// Combine all shortMessage
		    			byte[] allSMSMessage = combineMessage(clientSenderId, msisdn, thisMessageId, thisTotalMessages, thisMessageNumber, shortMessage);
						
		    			if (allSMSMessage == null || allSMSMessage.length == 0) {
		    				// NULL, no complete yet the combination process
		    				// Do not process anything
		    			} else {
		    				// Complete the combination process
			                // Run thread smppIncomingTrxProcessor
			                System.out.println("Processing messageId: " + messageId);
			                Thread incomingTrxProcessor = new Thread(new SMPPIncomingTrxProcessor(messageId, systemId, remoteIpAddress, clientSenderId, msisdn, allSMSMessage, dataCoding, clientId, mtSourceAddress, mtDestinationAddress, clientPropertyPooler, clientBalancePooler, smsTransactionOperationPooler, rabbitMqPooler, rabbitMqConnection));
			                incomingTrxProcessor.start();
		    			}
		    			
		                // Send submitresponse
		                SubmitSmResp submitSmResp = mt.createResponse();
		                submitSmResp.setMessageId(messageId);
		                
		                response = submitSmResp;		    				
					} else {
		                Address mtSourceAddress = mt.getSourceAddress();
		                String clientSenderId = mtSourceAddress.getAddress().trim();
		                		
		                Address mtDestinationAddress = mt.getDestAddress();
		                String msisdn = mtDestinationAddress.getAddress().trim();
		                
		                byte dataCoding = mt.getDataCoding();
		                byte[] shortMessage = mt.getShortMessage();

		                // Run thread smppIncomingTrxProcessor
		                System.out.println("Processing messageId: " + messageId);
		                Thread incomingTrxProcessor = new Thread(new SMPPIncomingTrxProcessor(messageId, systemId, remoteIpAddress, clientSenderId, msisdn, shortMessage, dataCoding, clientId, mtSourceAddress, mtDestinationAddress, clientPropertyPooler, clientBalancePooler, smsTransactionOperationPooler, rabbitMqPooler, rabbitMqConnection));
		                incomingTrxProcessor.start();
		                
		                //sendDeliveryReceipt(session, mtDestinationAddress, mtSourceAddress, dataCoding);
		                //sendMoMessage(session, mtDestinationAddress, mtSourceAddress, shortMessage, dataCoding);
		                
		                // Send submitresponse
		                SubmitSmResp submitSmResp = mt.createResponse();
		                submitSmResp.setMessageId(messageId);
		                
		                response = submitSmResp;
					}
					
				} catch (Exception e) {
					e.printStackTrace();
				}
            }

            return response;
        }

        private void sendDeliveryReceipt(SmppSession session, Address mtDestinationAddress, Address mtSourceAddress, byte[] shortMessage, byte dataCoding) {

            DeliverSm deliver = new DeliverSm();
            deliver.setEsmClass(SmppConstants.ESM_CLASS_MT_SMSC_DELIVERY_RECEIPT);
            deliver.setSourceAddress(mtDestinationAddress);
            deliver.setDestAddress(mtSourceAddress);
            deliver.setDataCoding(dataCoding);
            try {
                deliver.setShortMessage(shortMessage);
            } catch (Exception e) {
            	e.printStackTrace();
            }
            sendRequestPdu(session, deliver);
        }

        @SuppressWarnings("unused")
		private void sendMoMessage(SmppSession session, Address moSourceAddress, Address moDestinationAddress, byte [] textBytes, byte dataCoding) {

            DeliverSm deliver = new DeliverSm();

            deliver.setSourceAddress(moSourceAddress);
            deliver.setDestAddress(moDestinationAddress);
            deliver.setDataCoding(dataCoding);
            try {
              deliver.setShortMessage(textBytes);
            } catch (Exception e) {
              logger.error("Error!", e);
            }

            sendRequestPdu(session, deliver);
        }

        @SuppressWarnings("rawtypes")
		private void sendRequestPdu(SmppSession session, DeliverSm deliver) {
            try {
                WindowFuture<Integer,PduRequest,PduResponse> future = session.sendRequestPdu(deliver, 10000, false);
                if (!future.await()) {
                    logger.error("Failed to receive deliver_sm_resp within specified time");
                } else if (future.isSuccess()) {
                   DeliverSmResp deliverSmResp = (DeliverSmResp)future.getResponse();
                    logger.info("deliver_sm_resp: commandStatus [" + deliverSmResp.getCommandStatus() + "=" + deliverSmResp.getResultMessage() + "]");
                } else {
                    logger.error("Failed to properly receive deliver_sm_resp: " + future.getCause());
                }
            } catch (Exception e) {
            	e.printStackTrace();
            }
        }
        
        // Class to execute incoming smpp transaction
        class SMPPIncomingTrxProcessor implements Runnable {
        	private String messageId;
        	private String systemId;
        	private String remoteIpAddress;
        	private String clientSenderId;
        	private String msisdn;
        	private byte[] bShortMessage;
        	private String shortMessage;
        	private byte dataCoding;
        	private String clientId;
        	private String messageEncoding = "GSM";
        	
        	private Address mtSourceAddress;
        	private Address mtDestinationAddress;
        	
        	// Session ID
        	private String sessionId;
        	
        	// SUPPORTING CLASSES
        	private ClientPropertyPooler clientPropertyPooler;
        	private ClientBalancePooler clientBalancePooler;
        	private SMSTransactionOperationPooler smsTransactionOperationPooler;
    		private RabbitMQPooler rabbitMqPooler;
    		private Connection rabbitMqConnection;

        	
    		public SMPPIncomingTrxProcessor(String theMessageId, String theSystemId, String theRemoteIpAddress, String theClientSenderId, String theMsisdn, 
    				byte[] theShortMessage, byte theDataCoding, String theClientId, Address theMtSourceAddress, Address theMtDestinationAddress, 
    				ClientPropertyPooler theClientPropertyPooler, ClientBalancePooler theClientBalancePooler, SMSTransactionOperationPooler theSmsTransactionOperationPooler,
    				RabbitMQPooler theRabbitMqPooler, Connection theRabbitMqConnection) {
    			this.messageId = theMessageId;
    			System.out.println("INITIATE msgId " + this.messageId);
    			this.systemId = theSystemId;
    			this.remoteIpAddress = theRemoteIpAddress;
    			this.clientSenderId = theClientSenderId;
    			this.msisdn = theMsisdn;
    			this.bShortMessage = theShortMessage;
    			this.dataCoding = theDataCoding;
    			this.clientId = theClientId;
    			this.mtSourceAddress = theMtSourceAddress;
    			this.mtDestinationAddress = theMtDestinationAddress;
    			
    			this.sessionId = sessionRef.get().getConfiguration().getName();
    			
    			this.clientPropertyPooler = theClientPropertyPooler;
    			this.clientBalancePooler = theClientBalancePooler;
    			this.smsTransactionOperationPooler = theSmsTransactionOperationPooler;
    			this.rabbitMqPooler = theRabbitMqPooler;
    			this.rabbitMqConnection = theRabbitMqConnection;
    			
    			Charset theCharset = getCharsetByDataCoding(dataCoding);
    			
    			if (theCharset == CharsetUtil.CHARSET_GSM) {
    				this.messageEncoding = "GSM";
    			} else {
    				this.messageEncoding = "UCS2";
    			}
    			
    			this.shortMessage = CharsetUtil.decode(bShortMessage, theCharset);
    			
    			LoggingPooler.doLog(logger, "DEBUG", "EliandrieSMPPServerNeo - SMPPIncomingTrxProcessor", "SMPPIncomingTrxProcessor - " + this.sessionId, false, false, false, messageId, 
    					"Incoming trx - messageId: " + this.messageId + ", clientSenderId: " + this.clientSenderId + ", msisdn: " + this.msisdn + 
    					", dataCoding" + this.dataCoding + ", bShortMessage: " + this.bShortMessage + ", charSet: " + theCharset + ", shortmessage: " +
    					this.shortMessage + ", clientId: " + this.clientId, null);				
    		}
    		
    		private Charset getCharsetByDataCoding(byte dataCoding) {
    			Charset theCharSet = CharsetUtil.CHARSET_GSM; // DEFAULT is GSM7
    			
            	// character encoding constants
                /** SMSC Default Alphabet (default) */
                //public static final byte CHAR_ENC_DEFAULT = 0x00;
                /** IA5 (CCITT T.50)/ASCII (ANSI X3.4) */
                //public static final byte CHAR_ENC_IA5 = 0x01;
                /** Octet unspecified (8-bit binary) defined for TDMA and/ or CDMA but not defined for GSM */
                //public static final byte CHAR_ENC_8BITA = 0x02;
                /** Latin 1 (ISO-8859-1) */
                //public static final byte CHAR_ENC_LATIN1 = 0x03;
                /** Octet unspecified (8-bit binary) ALL TECHNOLOGIES */
                //public static final byte CHAR_ENC_8BIT = 0x04;
                /** JIS (X 0208-1990) */
                //public static final byte CHAR_ENC_JIS = 0x05;
                /** Cyrllic (ISO-8859-5) */
                //public static final byte CHAR_ENC_CYRLLIC = 0x06;
                /** Latin/Hebrew (ISO-8859-8) */
                //public static final byte CHAR_ENC_HEBREW = 0x07;
                /** UCS2 (ISO/IEC-10646) */
                //public static final byte CHAR_ENC_UCS2 = 0x08;
                /** Pictogram Encoding */
                //public static final byte CHAR_ENC_PICTO = 0x09;
                /** ISO-2022-JP (Music Codes) */
                //public static final byte CHAR_ENC_MUSIC = 0x0A;
                /** Reserved: 0x0B */
                //public static final byte CHAR_ENC_RSRVD = 0x0B;
                /** Reserved: 0x0C */
                //public static final byte CHAR_ENC_RSRVD2 = 0x0C;
                /** Extended Kanji JIS(X 0212-1990) */
                //public static final byte CHAR_ENC_EXKANJI = 0x0D;
                /** KS C 5601 */
                //public static final byte CHAR_ENC_KSC5601 = 0x0E;
                /** Reserved: 0x0F */
                //public static final byte CHAR_ENC_RSRVD3 = 0x0F;
    			
    			try {
    				switch(dataCoding) {
    					case (byte) 0x00:	theCharSet = CharsetUtil.CHARSET_GSM;
    										break;
//    					case (byte) 0x01:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//    										break;
//    					case (byte) 0x02:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//    										break;
//    					case (byte) 0x03:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//											break;
    					case (byte) 0x04:	theCharSet = CharsetUtil.CHARSET_UCS_2;
											break;
//    					case (byte) 0x05:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//											break;
    					case (byte) 0x06:	theCharSet = CharsetUtil.CHARSET_UCS_2;
											break;
    					case (byte) 0x07:	theCharSet = CharsetUtil.CHARSET_UCS_2;
											break;
    					case (byte) 0x08:	theCharSet = CharsetUtil.CHARSET_UCS_2;
											break;
//    					case (byte) 0x09:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//											break;
//    					case (byte) 0x0A:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//											break;
//    					case (byte) 0x0B:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//											break;
//    					case (byte) 0x0C:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//											break;
    					case (byte) 0x0D:	theCharSet = CharsetUtil.CHARSET_UCS_2;
											break;
//    					case (byte) 0x0E:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//											break;
//    					case (byte) 0x0F:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//											break;
						default:			theCharSet = CharsetUtil.CHARSET_GSM;
											break;
    				}
    				
        			LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServerNeo - SMPPIncomingTrxProcessor", "getCharsetByDataCoding - " + this.sessionId, false, false, false, messageId, 
        					"dataCoding: " + dataCoding + " -> charset: " + theCharSet, null);				
    			} catch (Exception e) {
    				e.printStackTrace();
    				
        			LoggingPooler.doLog(logger, "DEBUG", "BlastmeSMPPServerNeo - SMPPIncomingTrxProcessor", "getCharsetByDataCoding - " + this.sessionId, true, false, false, messageId, 
        					"Failed to get charset of datacoding: " + dataCoding + ". Error occured.", e);				
    				
    			}
    			
    			return theCharSet;
    		}
    		
    		private boolean isSenderIdValid(String clientId, String clientSenderId){
    			boolean isValid = false;
    					
    			String senderIdId = clientSenderId.trim() + "-" + clientId.trim();
    			
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
    		
    		private boolean isRouteDefined(String clientId, String clientSenderId, String apiUsername, String telecomId){
    			boolean isDefined = false;
    			
    			String routeId = clientSenderId.trim() + "-" + clientId.trim() + "-" + apiUsername.trim() + "-" + telecomId.trim();
    			
    			if(RouteSMSPooler.jsonRouteSMSProperty.has(routeId)){
    				isDefined = true;
    			}
    			
    			System.out.println("ROUTE ID " + routeId + " is DEFINED.");
    			return isDefined;
    		}
    		
    		private void saveInitialData(String messageId, LocalDateTime receiverDateTime, String batchId, String receiverData, String receiverClientResponse, 
    				String receiverclientIpAddress, LocalDateTime clientResponseDateTime, LocalDateTime trxDateTime, String msisdn, String message, String countryCode, String prefix, 
    				String telecomId, String trxStatus, String receiverType, String clientSenderIdId, String clientSenderId, String clientId, String apiUserName, 
    				double clientUnitPrice, String currency, String messageEncoding, int messageLength, int smsCount, String deliveryStatus){
    			try{
    				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMddHHmmssSSS");

    				LocalDateTime now = LocalDateTime.now();
    				this.smsTransactionOperationPooler.saveInitialSMPPData(messageId, now, "SMPPAPI", receiverData, receiverClientResponse, 
    						this.remoteIpAddress, now, now, msisdn.trim(), message.trim(), countryCode, prefix, telecomId, trxStatus, "SMPP", clientSenderIdId, clientSenderId, this.clientId, 
    						this.systemId, clientUnitPrice, currency, messageEncoding, messageLength, smsCount);
    				
    				LoggingPooler.doLog(logger, "DEBUG", "BlastmeSMPPServerNeo - SMPPIncomingTrxProcessor", "saveInitialData - " + this.sessionId, false, false, false, messageId, 
    						"Successfully save Initial Data to Database.", null);	

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
    				jsonRedis.put("sysSessionId", this.sessionId);

    				String redisKey = "trxdata-" + messageId.trim();
    				String redisVal = jsonRedis.toString();
    				
    				redisPooler.redisSetWithExpiry(redisCommand, redisKey, redisVal, expiry);
    				LoggingPooler.doLog(logger, "DEBUG", "BlastmeSMPPServerNeo - SMPPIncomingTrxProcessor", "saveInitialData - " + this.sessionId, false, false, false, messageId, 
    						"Successfully save Initial Data to Database and REDIS.", null);	
    			} catch (Exception e) {
    				e.printStackTrace();
    				LoggingPooler.doLog(logger, "DEBUG", "BlastmeSMPPServerNeo - SMPPIncomingTrxProcessor", "saveInitialData - " + this.sessionId, true, false, false, messageId, 
    						"Failed saving initial data. Error occured.", e);	
    			}
    		}    		
    		
    		private int getMessageLength(String encoding, String message){
    			int length = 300;
    			
    			int count = 0;
    			
    			if (encoding.equals("GSM")) {
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
    			} else {
    				// UCS2
    				count = message.length();
    			}
    			
    			if(count > 0){
    				length = count;
    			}
    			
    			return length;
    		}    	
    		
    		private int getSmsCount(String message, String encoding){
    			if(encoding.equals("GSM")){
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

    		private void doProcessTheSMS() {
    			System.out.println("PROCESSING msgId " + this.messageId);
    			String errorCode = "002"; // DEFAULT errCode
    			String deliveryStatus = "ACCEPTED";
    			byte deliveryState = SmppConstants.STATE_ACCEPTED;
    			
    			String prefix = "";
    			String telecomId = "";
    			String countryCode = "";
    			String clientSenderIdId = "";
    			String clientCurrency = "";
    			Double clientPricePerSubmit = 0.00;
    			
    			try {
    				LocalDateTime incomingDateTime = LocalDateTime.now();
    				
    				// Validate clientSenderId
    				if (isSenderIdValid(this.clientId, this.clientSenderId) == true) {
    					// clientSenderId is valid
    					clientSenderIdId = clientSenderId.trim() + "-" + this.clientId.trim();
    					
    					// Validate msisdn
    					JSONObject jsonMsisdn = isPrefixValid(this.msisdn);
    					
    					if (jsonMsisdn.getBoolean("isValid") == true) {
    						// Prefix is VALID
    						// Set MSISDN property
    						prefix = jsonMsisdn.getString("prefix");
    						telecomId = jsonMsisdn.getString("telecomId");
    						countryCode = jsonMsisdn.getString("countryCode");
    						
    						// Validate ROUTE
							if(isRouteDefined(this.clientId, this.clientSenderId, this.systemId, telecomId) == true){
								// ROUTE IS DEFINED
								
								// Validate balance
								// Get client currecnt
								clientCurrency = this.clientPropertyPooler.getCurrencyId(this.clientId.trim());
								
								// Check business model, prepaid or postpaid from clientPropertyPooler
								String businessModel = this.clientPropertyPooler.getBusinessMode(this.clientId).trim();
								System.out.print("Business model: " + businessModel);
								
								// routeId = clientSenderIdId + "-" + telecomId
								String routeId = clientSenderId + "-" + this.clientId + "-" + this.systemId + "-" + telecomId;
								JSONObject jsonRoute = RouteSMSPooler.jsonRouteSMSProperty.getJSONObject(routeId);
								
								// Get client price from jsonRoute
								clientPricePerSubmit = jsonRoute.getDouble("clientPricePerSubmit");

								boolean isBalanceEnough = true;
								if(businessModel.equals("PREPAID")){
									double divisionBalance = clientBalancePooler.getClientBalance(clientId);
									
									double minimumUnit = eliandrieBalanceThresholdSMS;
									
									if (clientCurrency.trim().equals("IDR")) {
										minimumUnit = eliandrieBalanceThresholdRupiah;
									} else if (clientCurrency.trim().equals("USD")) {
										minimumUnit = eliandrieBalanceThresholdUSD;
									} else if (clientCurrency.trim().equals("EUR")) {
										minimumUnit = eliandrieBalanceThresholdEuro;
									} else if (clientCurrency.trim().equals("RMB")) {
										minimumUnit = eliandrieBalanceThresholdRMB;
									}
									
									if(divisionBalance > minimumUnit && divisionBalance > clientPricePerSubmit){
										isBalanceEnough = true;
									} else {
										isBalanceEnough = false;
									}
									System.out.println("CHECK BALANCE msgId " + this.messageId);
								}
								
								// If balance is enough (will always be enough for postpaid)
								if (isBalanceEnough == true) {
									// BALANCE IS ENOUGH
									errorCode = "002";
									deliveryStatus = "ACCEPTED";
									deliveryState = SmppConstants.STATE_ACCEPTED;
		    						LoggingPooler.doLog(logger, "DEBUG", "BlastmeSMPPServerNeo - SMPPIncomingTrxProcessor", "doProcessTheSMS - " + this.sessionId, false, false, false, messageId, 
		    								"MSISDN: " + msisdn + ". MESSAGE IS ACCEPTED. errCode: " + errorCode + ", deliveryStatus: " + deliveryStatus, null);	
									
		    						// Save initial data and send to SMPP_INCOMING queue to be processed by router
		    						
								} else {
									// BALANCE IS NOT ENOUGH
									System.out.println(messageId + ". ROUTE IS NOT DEFINED.");
									errorCode = "122";								// ROUTE NOT DEFINED
									deliveryStatus = "REJECTED";
									deliveryState = SmppConstants.STATE_REJECTED;
		    						LoggingPooler.doLog(logger, "DEBUG", "BlastmeSMPPServerNeo - SMPPIncomingTrxProcessor", "doProcessTheSMS - " + this.sessionId, false, false, false, messageId, 
		    								"MSISDN: " + msisdn + ". BALANCE IS NOT ENOUGH. errCode: " + errorCode + ", deliveryStatus: " + deliveryStatus, null);	
								}
							} else {
								// ROUTE IS NOT DEFINED
								System.out.println(messageId + ". ROUTE IS NOT DEFINED.");
								errorCode = "900";								// ROUTE NOT DEFINED
								deliveryStatus = "REJECTED";
								deliveryState = SmppConstants.STATE_REJECTED;
	    						LoggingPooler.doLog(logger, "DEBUG", "BlastmeSMPPServerNeo - SMPPIncomingTrxProcessor", "doProcessTheSMS - " + this.sessionId, false, false, false, messageId, 
	    								"MSISDN: " + msisdn + ". ROUTE IS NOT DEFINED. errCode: " + errorCode + ", deliveryStatus: " + deliveryStatus, null);	
							}
    					} else {
    						// Prefix is NOT VALID
    						System.out.println(messageId + " PREFIX IS NOT VALID");									

    						// Prefix is not valid
    						errorCode = "113";									// UNREGISTERED PREFIX
    						deliveryStatus = "REJECTED";
    						deliveryState = SmppConstants.STATE_REJECTED;
    						LoggingPooler.doLog(logger, "DEBUG", "BlastmeSMPPServerNeo - SMPPIncomingTrxProcessor", "doProcessTheSMS - " + this.sessionId, false, false, false, messageId, 
    								"MSISDN: " + msisdn + ". PREFIX IS NOT DEFINED. errCode: " + errorCode + ", deliveryStatus: " + deliveryStatus, null);	
    					}
    				} else {
    					// clientSenderId is NOT VALID
						System.out.println(messageId + " SENDERID IS INVALID!!!");									

						// senderId is NOT valid
						//deliveryState = SmppConstants.STATE_REJECTED; 	// INVALID SENDERID
						errorCode = "121";								// INVALID SENDERID															
						deliveryStatus = "REJECTED";
						deliveryState = SmppConstants.STATE_REJECTED;
						LoggingPooler.doLog(logger, "DEBUG", "BlastmeSMPPServerNeo - SMPPIncomingTrxProcessor", "doProcessTheSMS - " + this.sessionId, false, false, false, messageId, 
								"MSISDN: " + msisdn + ". SENDERID IS NOT DEFINED. errCode: " + errorCode + ", deliveryStatus: " + deliveryStatus, null);	
					}
    				
    				// Get messageLength & smsCount
    				int messageLength = getMessageLength(messageEncoding, shortMessage);
    				int smsCount = getSmsCount(shortMessage, messageEncoding);
    				
    				// Save initial data
    				System.out.println("Save " + this.messageId);
    				// String messageId, LocalDateTime receiverDateTime, String batchId, String receiverData, 
    				// String receiverClientResponse, String receiverclientIpAddress, LocalDateTime clientResponseDateTime, 
    				// LocalDateTime trxDateTime, String msisdn, String message, String countryCode, String prefix, 
    				// String telecomId, String trxStatus, String receiverType, String clientSenderIdId, String clientSenderId, 
    				// String clientId, String apiUserName, double clientUnitPrice, String currency, String messageEncoding, 
    				// int messageLength, int smsCount, String deliveryStatus
    				String receiverData = "SMPP: clientSenderId: " + this.clientSenderId + ", msisdn: " + this.msisdn + ", message: " + this.shortMessage + ", dataCoding" + dataCoding;
    				String receiverResponse = "errorCode: " + errorCode + ", deliveryStatus: " + deliveryState;
    				LocalDateTime responseDateTime = LocalDateTime.now();
    				
    				saveInitialData(this.messageId, incomingDateTime, this.sessionId, receiverData, 
    						receiverResponse, this.remoteIpAddress, responseDateTime, incomingDateTime, msisdn, shortMessage, countryCode, 
    						prefix, telecomId, errorCode, "SMPP", clientSenderIdId, clientSenderId, clientId, this.systemId, 
    						clientPricePerSubmit, clientCurrency, messageEncoding, messageLength, smsCount, deliveryStatus);
    				
    				// Process sending DLR - ONLY Failed one. ACCEPTED one will get DR after processed in ROUTER
    				if (!errorCode.trim().startsWith("00")) {
        				int submitCount = 0;
        				if (errorCode.trim().startsWith("00")) {
        					submitCount = 1;
        				}
        				DeliveryReceipt dlrReceipt = new DeliveryReceipt(messageId, submitCount, 0, new DateTime(), new DateTime(), deliveryState, errorCode, shortMessage);
        				
        				Address moSourceAddress = this.mtDestinationAddress;
        				Address moDestinationAddress = this.mtSourceAddress;
        				sendDeliveryReceipt(sessionRef.get(), moSourceAddress, moDestinationAddress, dlrReceipt.toShortMessage().getBytes(), this.dataCoding);
    					LoggingPooler.doLog(logger, "DEBUG", "BlastmeSMPPServerNeo - SMPPIncomingTrxProcessor", "doProcessTheSMS - " + this.sessionId, false, false, false, messageId, 
    							"Sending DLR with session: " + sessionRef.get().getConfiguration().getName() + ". DLR: " + dlrReceipt.toShortMessage(), null);	
    				}

    				// SUBMIT TO QUEUE SMPP_INCOMING - Need specific channel per thread, do open new channel and close it after
					// Submit to Queue for further process
    				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMddHHmmssSSS");
    				
    				JSONObject jsonIncoming = new JSONObject();
    				
					jsonIncoming.put("messageId", messageId);
					jsonIncoming.put("receiverDateTime", incomingDateTime.format(formatter));
					jsonIncoming.put("transactionDateTime", responseDateTime.format(formatter));
					jsonIncoming.put("msisdn", msisdn);
					jsonIncoming.put("message", shortMessage);
					jsonIncoming.put("telecomId", telecomId);
					jsonIncoming.put("countryCode", countryCode);
					jsonIncoming.put("prefix", prefix);
					jsonIncoming.put("errorCode", errorCode);
					jsonIncoming.put("apiUserName", this.systemId);
					jsonIncoming.put("clientSenderIdId", clientSenderIdId); // senderIdId-clientId
					jsonIncoming.put("clientSenderId", clientSenderId);
					jsonIncoming.put("clientId", this.clientId);
					jsonIncoming.put("apiUserName", this.systemId);
					jsonIncoming.put("clientIpAddress", this.remoteIpAddress);
					jsonIncoming.put("receiverType", "SMPP"); // SMPP and HTTP only
					jsonIncoming.put("smsChannel", "SMPP");
					jsonIncoming.put("sysSessionId", this.sessionId);
	            	jsonIncoming.put("messageLength", getMessageLength(messageEncoding, shortMessage));
	            	jsonIncoming.put("messageCount", getSmsCount(shortMessage, messageEncoding));
	            	jsonIncoming.put("clientPricePerSubmit", clientPricePerSubmit);

					LoggingPooler.doLog(logger, "DEBUG", "BlastmeSMPPServerNeo - SMPPIncomingTrxProcessor", "doProcessTheSMS - " + this.sessionId, false, false, false, messageId, 
							"jsonIncoming: " + jsonIncoming.toString(), null);				

					Channel channel = rabbitMqPooler.getChannel(rabbitMqConnection);
					
					channel.queueDeclare(SMSQueueName, true, false, false, null);
					channel.basicPublish("", SMSQueueName, MessageProperties.PERSISTENT_TEXT_PLAIN, jsonIncoming.toString().getBytes());
					LoggingPooler.doLog(logger, "DEBUG", "BlastmeSMPPServerNeo - SMPPIncomingTrxProcessor", "doProcessTheSMS - " + this.sessionId, false, false, false, messageId, 
							"jsonIncoming: " + jsonIncoming.toString() + " published SUCCESSfully!", null);
					
					channel.close();

    				
    			} catch (Exception e) {
    				e.printStackTrace();
    				
					LoggingPooler.doLog(logger, "DEBUG", "BlastmeSMPPServerNeo - SMPPIncomingTrxProcessor", "doProcessTheSMS - " + this.sessionId, true, false, false, messageId, 
							"Failed to process incoming sms message. Error occured.", e);
    			}
    		}
    		
    		@Override
    		public void run() {
    			System.out.println("RUNNING msgId " + this.messageId);
    			doProcessTheSMS();    			
    		}
        	
        }
        
    }
	    
}
