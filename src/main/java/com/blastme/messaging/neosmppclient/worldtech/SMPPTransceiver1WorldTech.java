package com.blastme.messaging.neosmppclient.worldtech;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.json.JSONObject;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.RedisPooler;
import com.blastme.messaging.toolpooler.SMSTransactionOperationPooler;
import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.SmppBindType;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.SmppSessionConfiguration;
import com.cloudhopper.smpp.impl.DefaultSmppClient;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.EnquireLink;
import com.cloudhopper.smpp.pdu.EnquireLinkResp;
import com.cloudhopper.smpp.pdu.PduRequest;
import com.cloudhopper.smpp.pdu.PduResponse;
import com.cloudhopper.smpp.pdu.SubmitSm;
import com.cloudhopper.smpp.pdu.SubmitSmResp;
import com.cloudhopper.smpp.type.Address;
import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import io.lettuce.core.api.sync.RedisCommands;

public class SMPPTransceiver1WorldTech implements Runnable {
	private static Logger logger;
	private static RedisPooler redisPooler;
	
	private static int windowSize = 1;
	private static String sessionName = "1WORLDTECH";
	private static String hostIPAddress = "185.150.202.116";
	private static int hostPort = 9001;
	private static String userName = "Artamayadr";
	private static String password = "artdr$53";


	SmppSession session0 = null;
	public String bindNumber = "";
	
	// RabbitMQPooler
	public RabbitMQPooler rabbitMqPooler = null;
	public Connection connectionRabbit = null;
	public Channel channelRabbitConsume = null;
	public String queueName = "TRCV_1WORLDTECH";
	public int tpsPerSession = 10;
	
	public SMSTransactionOperationPooler smsTransactionOperationPooler;
	
	public SMPPTransceiver1WorldTech(String bindNo) {
		this.bindNumber = bindNo;
		
		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());

		// Setup logger
		logger = LogManager.getLogger("SMPP_CLIENT");

		// Initiate LoggingPooler
		new LoggingPooler();

		try {
			// Initiate RedisPooler
			redisPooler = new RedisPooler();
			//redisCommand = redisPooler.redisInitiateConnection();
			
			// Initiate RabbitMQPooler
			rabbitMqPooler = new RabbitMQPooler();
			connectionRabbit = rabbitMqPooler.getConnection();
			channelRabbitConsume = rabbitMqPooler.getChannel(connectionRabbit);

			smsTransactionOperationPooler = new SMSTransactionOperationPooler();
		} catch (Exception e) {
			e.printStackTrace();

			System.exit(-1);
		}
	}

	public void BindAndTransceiving() {
		LoggingPooler.doLog(logger, "DEBUG", "1WORLDTECH - BindNo " + bindNumber, "BindAndTransceiving", false, false, true, "", 
				"Executing thread bindNumber: " + bindNumber, null);
		
		//
		// setup 3 things required for any session we plan on creating
		//

		// to enable automatic expiration of requests, a second scheduled executor
		// is required which is what a monitor task will be executed with - this
		// is probably a thread pool that can be shared with between all client bootstraps
		ScheduledThreadPoolExecutor monitorExecutor = (ScheduledThreadPoolExecutor)Executors.newScheduledThreadPool(1, new ThreadFactory() {
			private AtomicInteger sequence = new AtomicInteger(0);
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setName("GreembeeSmppClientSessionWindowMonitorPool-" + sequence.getAndIncrement());
				return t;
			}
		});

		// a single instance of a client bootstrap can technically be shared
		// between any sessions that are created (a session can go to any different
		// number of SMSCs) - each session created under
		// a client bootstrap will use the executor and monitorExecutor set
		// in its constructor - just be *very* careful with the "expectedSessions"
		// value to make sure it matches the actual number of total concurrent
		// open sessions you plan on handling - the underlying netty library
		// used for NIO sockets essentially uses this value as the max number of
		// threads it will ever use, despite the "max pool size", etc. set on
		// the executor passed in here
		DefaultSmppClient clientBootstrap = new DefaultSmppClient(Executors.newCachedThreadPool(), 1, monitorExecutor);

		//
		// setup configuration for a client session
		//
		DefaultSmppSessionHandler sessionHandler = new ClientSmppSessionHandler();

		SmppSessionConfiguration config0 = new SmppSessionConfiguration();
		config0.setWindowSize(windowSize);
		config0.setName(sessionName);
		config0.setType(SmppBindType.TRANSCEIVER);
		config0.setHost(hostIPAddress);
		config0.setPort(hostPort);
		config0.setConnectTimeout(10000);
		config0.setSystemId(userName);
		config0.setPassword(password);
		config0.getLoggingOptions().setLogBytes(true);
		// to enable monitoring (request expiration)
		config0.setRequestExpiryTimeout(30000);
		config0.setWindowMonitorInterval(15000);
		config0.setCountersEnabled(true);

		//
		// create session, enquire link, submit an sms, close session
		//
		session0 = null;

		try {
			// create session a session by having the bootstrap connect a
			// socket, send the bind request, and wait for a bind response
			session0 = clientBootstrap.bind(config0, sessionHandler);

			if (session0 != null) {
				// Binding success
				LoggingPooler.doLog(logger, "INFO", "1WORLDTECH - bindNumber " + bindNumber, "BindAndTransceiving", false, false, true, "", 
						"Binding to Greembee P2P SUCCESS. Bind Number: " + bindNumber, null);
				
				// Send enquiry link periodically as different thread
				ExecutorService executorEnquiry = Executors.newFixedThreadPool(1);
				executorEnquiry.execute(new LinkEnquirer(session0, bindNumber));
				
				// Read queue transceiver 1WorldTech
				channelRabbitConsume.queueDeclare(queueName, true, false, false, null);	
				channelRabbitConsume.basicQos(1);
				LoggingPooler.doLog(logger, "INFO", "1WORLDTECH - bindNumber " + bindNumber, "BindAndTransceiving", false, false, true, "", 
						"Reading queue " + queueName + " is ready, waiting for message ... ", null);
				
				// Guava rateLimiter
				RateLimiter rateLimiter = RateLimiter.create(tpsPerSession);

				Consumer consumer = new DefaultConsumer(channelRabbitConsume) {
				      @Override
				      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
				          throws IOException {
				        String message = new String(body, "UTF-8");
				        
						try{
							LoggingPooler.doLog(logger, "INFO", "1WORLDTECH - bindNumber " + bindNumber, "BindAndTransceiving", false, false, true, "", 
									"Receive message: " + message, null);

							// Throttle
							rateLimiter.acquire();

							LoggingPooler.doLog(logger, "DEBUG", "1WORLDTECH - bindNumber " + bindNumber, "BindAndTransceiving", false, false, true, "", 
									"Transmit to Greembee P2P bindNumber: " + bindNumber + ", message: " + message, null);

							// Process the message from queue
							// Transmit should be in multi thread for faster performance
							if (!session0.isClosed() || !session0.isUnbinding()) {
								session0 = clientBootstrap.bind(config0, sessionHandler);
							}
							
					        //Transmit(theSession, message);
							Runnable theHit = new SMPPSubmitter(session0, bindNumber, message);							
							Thread thread = new Thread(theHit);
							thread.start();
						} catch(Exception e) {
							LoggingPooler.doLog(logger, "INFO", "1WORLDTECH - bindNumber " + bindNumber, "BindAndTransceiving", false, false, true, "", 
									"Failed to read queue. Error occur.", e);
						} finally {
							LoggingPooler.doLog(logger, "INFO", "1WORLDTECH - bindNumber " + bindNumber, "BindAndTransceiving", false, false, true, "", 
									"Done processing message: " + message, null);

							channelRabbitConsume.basicAck(envelope.getDeliveryTag(), false);
						}
				      }
				};

				boolean autoAck = false; // If not finally exectued well, no ack to rabbitmq, message not gone
				channelRabbitConsume.basicConsume(queueName, autoAck, consumer);
				
			} else {
				// Binding failed.
				LoggingPooler.doLog(logger, "INFO", "1WORLDTECH - bindNumber " + bindNumber, "BindAndTransceiving", false, false, true, "", 
						"Bindint to GREEMBEE FAILED.", null);
			}
			
			
		} catch (Exception e) {
			LoggingPooler.doLog(logger, "DEBUG", "1WORLDTECH - bindNumber " + bindNumber, "BindAndTransceiving", true, false, true, "", 
					"Failed to bind to GREEMBEE P2P. Error occur.", e);
		}
	}
	
	@Override
	public void run() {
		BindAndTransceiving();
	}
	
	public static class ClientSmppSessionHandler extends DefaultSmppSessionHandler {
        public ClientSmppSessionHandler() {
        }

        @Override
        public void firePduRequestExpired(@SuppressWarnings("rawtypes") PduRequest pduRequest) {
        	LoggingPooler.doLog(logger, "DEBUG", "1WORLDTECH", "BindAndTransceiving", false, false, true, "", 
    				"PDU request expired.", null);
        }

        @Override
        public PduResponse firePduRequestReceived(@SuppressWarnings("rawtypes") PduRequest pduRequest) {
            PduResponse response = pduRequest.createResponse();

            LoggingPooler.doLog(logger, "DEBUG", "1WORLDTECH", "BindAndTransceiving", false, false, true, "", 
    				"Incoming PDU request.", null);
            
            return response;
        }
        
    }
	
	public static class LinkEnquirer implements Runnable {
		private SmppSession sessionX = null;
		private String bindNumber = "";
		
		private int enquiryGap = 10; // 10 seconds
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yy-MM-dd HH:mm:ss.SSS");
		
		private RedisCommands<String, String> redisCommand;
		
		public LinkEnquirer(SmppSession sessionOrig, String threadName) {
			this.sessionX = sessionOrig;
			this.bindNumber = threadName;
			
			redisCommand = redisPooler.redisInitiateConnection();
		}

		private void DoLinkEnquiring() {
			try {
				for(;;) {
					EnquireLinkResp enquireLinkResp1 = sessionX.enquireLink(new EnquireLink(), 10000);
					LoggingPooler.doLog(logger, "DEBUG", "1WORLDTECH - bindNumber " + bindNumber, "BindAndTransceiving", false, false, true, "", 
							"enquire_link_resp #1: commandStatus [" + enquireLinkResp1.getCommandStatus() + "=" + enquireLinkResp1.getResultMessage() + "]", null);	
				
					if (enquireLinkResp1.getCommandStatus() == 0) {
						// Enquiry success - put into redis
						String redisKey = "1WORLDTECH_" + bindNumber;
						String redisVal = LocalDateTime.now().format(formatter);
						
						redisPooler.redisSet(redisCommand, redisKey, redisVal);
					}
					
					Thread.sleep(enquiryGap * 1000);
				}
			} catch (Exception e) {
				LoggingPooler.doLog(logger, "DEBUG", "1WORLDTECH - bindNumber " + bindNumber, "BindAndTransceiving", true, false, true, "", 
						"Failed to send enquiry. Error occur.", e);
			} 
		}
		
		@Override
		public void run() {
			DoLinkEnquiring();
		}
	}
	
	private class SMPPSubmitter implements Runnable {
		private SmppSession session0;
		private String queueMessage;
		private String bindNumber;
		
		public SMPPSubmitter(SmppSession theSession, String bindNo, String theQueueMessage) {
			this.session0 = theSession;
			this.queueMessage = theQueueMessage;
			this.bindNumber = bindNo;
		}
		
		private void Transmit(SmppSession theSession, String bindNumber, String queueMessage){
			try{				
				// Convert queueMessage to JSON
				JSONObject jsonMessage = new JSONObject(queueMessage);
				
				// Extract jsonMessage
				String theMessageId = jsonMessage.getString("messageId");
				String theMsisdn = jsonMessage.getString("msisdn");
				String theMessage = jsonMessage.getString("message");
				String theVendorSenderId = jsonMessage.getString("vendorSenderId");
				//String theTelecomId = jsonMessage.getString("telecomId");
				//String theApiUserName = jsonMessage.getString("apiUserName");
				String theVendorId = jsonMessage.getString("routedVendorId");
				//String theVendorParameter = jsonMessage.getString("vendorParameter");
				//String theEncoding = jsonMessage.getString("encoding");
				//String theEncoding = "UCS2";
				
	            //byte[] textBytes = CharsetUtil.encode(theMessage, CharsetUtil.CHARSET_UTF_8);
				//byte[] textBytes = CharsetUtil.encode(theMessage, CharsetUtil.CHARSET_UCS_2);
				byte[] textBytes = CharsetUtil.encode(theMessage, CharsetUtil.CHARSET_UCS_2);
	            
	            SubmitSm submit0 = new SubmitSm();

	            // add delivery receipt
	            submit0.setRegisteredDelivery(SmppConstants.REGISTERED_DELIVERY_SMSC_RECEIPT_REQUESTED);
	            submit0.setSourceAddress(new Address((byte)0x03, (byte)0x00, theVendorSenderId));
	            submit0.setDestAddress(new Address((byte)0x01, (byte)0x01, theMsisdn));
	            submit0.setDataCoding((byte) 0x08);
	            
	            submit0.setShortMessage(textBytes);

	            LoggingPooler.doLog(logger, "INFO", "1WORLDTECH - bindNumber " + bindNumber, "BindAndTransceiving", false, false, true, theMessageId, 
						"PDU to send: " + submit0.toString(), null);
	            
	            // start sending datetime
	    		LocalDateTime startSendingDateTime = LocalDateTime.now();

	    		// send the message
                SubmitSmResp submitResp = theSession.submit(submit0, 30000);
				LoggingPooler.doLog(logger, "INFO", "1WORLDTECH - bindNumber " + bindNumber, "BindAndTransceiving", false, false, true, theMessageId, 
						"Transmitting - messageId: " + theMessageId + ", msisdn: " + theMsisdn + ", senderId: " + theVendorId + 
						" using session " + this.session0.getConfiguration().getName(), null);

	    		// done sedning datetime
                LocalDateTime endSendingDateTime = LocalDateTime.now();
	    		
                // insert into transaction vendor
                String vendorRequest = "SMPP: vendorSenderId: " + theVendorSenderId + ", msisdn: " + theMsisdn + ", short message: " + theMessage;
                String vendorMessageId = submitResp.getMessageId();
                String vendorResponse = submitResp.getResultMessage();
                
                smsTransactionOperationPooler.saveTransactionVendor(theMessageId, theVendorId, startSendingDateTime, vendorRequest, 
                		endSendingDateTime, vendorResponse, vendorMessageId, startSendingDateTime, "");
				LoggingPooler.doLog(logger, "INFO", "1WORLDTECH - bindNumber " + bindNumber, "BindAndTransceiving", false, false, true, theMessageId, 
						"Transmitting - Save to database transaction vendor is DONE.", null);
				
				LoggingPooler.doLog(logger, "INFO", "1WORLDTECH - bindNumber " + bindNumber, "BindAndTransceiving", false, false, true, theMessageId, 
						"Done Transmitting! Submitting duration: " + ChronoUnit.MILLIS.between(startSendingDateTime, endSendingDateTime) + " milliseconds.", null);					
	        } catch (Exception e) {
				e.printStackTrace();				
				LoggingPooler.doLog(logger, "DEBUG", "1WORLDTECH - bindNumber " + bindNumber, "BindAndTransceiving", true, false, false, "", 
						"Failed to send message. ERROR OCCURED.", e);	
			} 
    	}
		
		@Override
		public void run() {
			Transmit(this.session0, this.bindNumber, this.queueMessage);				
		}			
	}
}
