package com.blastme.messaging.smsbulk.transceiver;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.SMSTransactionOperationPooler;
import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.SmppBindType;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.SmppSessionConfiguration;
import com.cloudhopper.smpp.impl.DefaultSmppClient;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.DeliverSm;
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

public class TransceiverSMPPBStill {
	private static Logger logger;
    
	private static String host = "203.129.6.116";
	private static int port = 4510;
	private static String sysId = "bstill2";
	private static String pass = "Bstill2@";
  
	private static String queueName = "TRCV_ART_02";
  
	private static int sessionCount = 1;
	private static double theTpsPerSession = 50.00;
	
	private ExecutorService executorX = Executors.newFixedThreadPool(sessionCount); // 3 sessions

	public TransceiverSMPPBStill() {
		new LoggingPooler();
		
		try {
			// Setup logger
			logger = LogManager.getLogger("TRANSCEIVER_SMPP_BSTILL");

			LoggingPooler.doLog(logger, "INFO", "TransceiverSMPPBSTILL", "TransceiverSMPPBSTILL", false, false, false, "", "Module TransceiverSMPPBSTILL is running.", null);				
		} catch (Exception e) {
			e.printStackTrace();
			LoggingPooler.doLog(logger, "INFO", "TransceiverSMPPBSTILL", "TransceiverSMPPBSTILL", true, false, false, "", 
					"Failed to run module TransceiverSMPPBSTILL. Error occured.", e);	
			
			System.exit(-1);
		}		
	}
	
	private void startTheEngine() {
		try {
            //ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newCachedThreadPool();
            
            // to enable automatic expiration of requests, a second scheduled executor
            // is required which is what a monitor task will be executed with - this
            // is probably a thread pool that can be shared with between all client bootstraps
            ScheduledThreadPoolExecutor monitorExecutor = (ScheduledThreadPoolExecutor)Executors.newScheduledThreadPool(1, new ThreadFactory() {
                private AtomicInteger sequence = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setName("SmppClientSessionWindowMonitorPool-" + sequence.getAndIncrement());
                    return t;
                }
            });
            
            DefaultSmppClient[] clientBootStrapArr = new DefaultSmppClient[sessionCount];
            DefaultSmppSessionHandler[] sessionHandlerArr = new DefaultSmppSessionHandler[sessionCount];
            
            SmppSessionConfiguration config0 = new SmppSessionConfiguration();
            config0.setWindowSize(1);
            config0.setType(SmppBindType.TRANSCEIVER);
            config0.setHost(host);
            config0.setPort(port);
            config0.setConnectTimeout(30000);
            config0.setSystemId(sysId);
            config0.setPassword(pass);
            config0.getLoggingOptions().setLogBytes(true);
            // to enable monitoring (request expiration)
            config0.setRequestExpiryTimeout(30000);
            config0.setWindowMonitorInterval(15000);
            config0.setCountersEnabled(true);	

            for (int x = 0; x < sessionCount; x++) {
            	clientBootStrapArr[x] = new DefaultSmppClient(Executors.newCachedThreadPool(), 1, monitorExecutor);
            	sessionHandlerArr[x] = new ClientSmppSessionHandler();
            	executorX.execute(new Transmitter(queueName, clientBootStrapArr[x], sessionHandlerArr[x], config0, "SMPPBSTILLL-0" + x));
            }            
//            DefaultSmppClient clientBootstrap = new DefaultSmppClient(Executors.newCachedThreadPool(), 1, monitorExecutor);
//
//            DefaultSmppSessionHandler sessionHandler = new ClientSmppSessionHandler();			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		TransceiverSMPPBStill transceiver = new TransceiverSMPPBStill();
		transceiver.startTheEngine();
	}

    public static class ClientSmppSessionHandler extends DefaultSmppSessionHandler {
        @Override
        public void firePduRequestExpired(@SuppressWarnings("rawtypes") PduRequest pduRequest) {
            System.out.println("PDU request expired: " + pduRequest);
        }

		@Override
        public PduResponse firePduRequestReceived(@SuppressWarnings("rawtypes") PduRequest pduRequest) {
			LoggingPooler.doLog(logger, "DEBUG", "ClientSmppSessionHandler", "firePduRequestReceived", false, false, false, "", 
					"Incoming PDU requested: " + pduRequest, null);				

        	try{
        		// Create thread utk handle        		
        		if(pduRequest.getCommandId() == SmppConstants.CMD_ID_DELIVER_SM){
        			LoggingPooler.doLog(logger, "DEBUG", "ClientSmppSessionHandler", "firePduRequestReceived", false, false, false, "", 
        					"Incoming PDU requested is CMD_ID_DELIVER_SM", null);				
        			
					Runnable theProcessor = new PDUProcessor(pduRequest);							
					Thread thread = new Thread(theProcessor);
					thread.start();
        		}
        	} catch (Exception e) {
        		e.printStackTrace();
        	}
        	
            PduResponse response = pduRequest.createResponse();
            
            return response;
        }
		
		private class PDUProcessor implements Runnable{
			@SuppressWarnings("rawtypes")
			private PduRequest pduRequest;

			public PDUProcessor(@SuppressWarnings("rawtypes") PduRequest thePduRequest) {
				this.pduRequest = thePduRequest;
			}

			private void processThePDU() {
				try {
	    			// Convert into deliverSm
	    			DeliverSm deliverSm = (DeliverSm) this.pduRequest;
	    			byte[] shortMessage = deliverSm.getShortMessage();
	    			String theDRContent = CharsetUtil.decode(shortMessage, CharsetUtil.CHARSET_UCS_2);
	    			LoggingPooler.doLog(logger, "DEBUG", "ClientSmppSessionHandler", "firePduRequestReceived", false, false, false, "", 
	    					"theDRContent: " + theDRContent, null);				
	    			
//	    			DeliveryReceipt dr = DeliveryReceipt.parseShortMessage(theDRContent, DateTimeZone.forID("Asia/Jakarta")); 
//	    			
//	    			String messageId = dr.getMessageId();
//	    			String state = "ACCEPTD";
//	    			//int countSubmitted = dr.getSubmitCount();
//	    			//int countDelivered = dr.getDeliveredCount();
//	    			byte status = dr.getState();
//
//	    			if (status == SmppConstants.STATE_ACCEPTED) {
//	    				state = "ACCEPTD";
//	    			} else if (status == SmppConstants.STATE_DELIVERED) {
//	    				state = "DELIVERED";
//	    			} else if (status == SmppConstants.STATE_ENROUTE) {
//	    				state = "ENROUTE";
//	    			} else if (status == SmppConstants.STATE_EXPIRED) {
//	    				state = "EXPIRED";
//	    			} else if (status == SmppConstants.STATE_REJECTED) {
//	    				state = "REJECTED";
//	    			} else if (status == SmppConstants.STATE_UNDELIVERABLE) {
//	    				state = "UNDELIVERABLE";
//	    			} else {
//	    				state = "UNKNOWN";
//	    			}
//	    			
//	    			LoggingPooler.doLog(logger, "DEBUG", "ClientSmppSessionHandler", "firePduRequestReceived", false, false, false, messageId, 
//	    					"state: " + state + ", theDRContent: " + theDRContent, null);
	    		} catch (Exception e) {
					e.printStackTrace();
				}	
    		}
			
			@Override
			public void run() {
				processThePDU();				
			}			
		}
        
    }
    
    public static class Transmitter implements Runnable{
    	//private SmppSession session0;
    	private DefaultSmppClient clientBootstrap;
    	private DefaultSmppSessionHandler sessionHandler;
    	private SmppSessionConfiguration config0;
    	private String sessionName;
    	private SmppSession theSession;
    	
    	private String queueName;    	
    	
    	private RabbitMQPooler rabbitMqPooler;

    	private SMSTransactionOperationPooler smsTransactionOperationPooler;
    	
    	private Connection connectionRabbit;
    	private Channel channelRabbitConsume;
    	    	
    	public Transmitter(String theQueueName, DefaultSmppClient clientBootstrap, DefaultSmppSessionHandler sessionHandler, SmppSessionConfiguration config0, String sessionName) {
    		//this.session0 = session0;
    		this.queueName = theQueueName;
    		this.clientBootstrap = clientBootstrap;
    		this.sessionHandler = sessionHandler;
    		this.config0 = config0;
    		this.sessionName = sessionName;
    		
    		try {
    			// Initiate RabbitMQPooler
    			rabbitMqPooler = new RabbitMQPooler();
    			connectionRabbit = rabbitMqPooler.getConnection();
    			channelRabbitConsume = rabbitMqPooler.getChannel(connectionRabbit);
    			
    			// Initate smsTransactionOperationPooler
    			smsTransactionOperationPooler = new SMSTransactionOperationPooler();
        		
				LoggingPooler.doLog(logger, "DEBUG", "TransceiverSMPPBSTILL", "Transmitter", false, false, false, "", 
						"Transmitter - TransceiverSMPPBSTILL is READY.", null);				
        		
    		} catch (Exception e) {
    			e.printStackTrace();
				LoggingPooler.doLog(logger, "DEBUG", "TransceiverSMPPBSTILL", "Transmitter", true, false, false, "", 
						"Transmitter - TransceiverSMPPBSTILL is FAILED to INITIATE.", e);				
    		}
		}
    	
    	private SmppSession Bind() {
    		SmppSession session0 = null;
    		
    		try {
                // create session a session by having the bootstrap connect a
                // socket, send the bind request, and wait for a bind response
                session0 = clientBootstrap.bind(config0, sessionHandler);
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    		
    		return session0;
    	}

		private void readQueue() {
			try{
				channelRabbitConsume.queueDeclare(queueName, true, false, false, null);	
				channelRabbitConsume.basicQos(1);
				LoggingPooler.doLog(logger, "INFO", "TransceiverSMPPBSTILL", "readQueue - " + queueName, false, false, true, "", 
						"Reading queue " + queueName + " is ready, waiting for message ... ", null);
				
				// Guava rateLimiter
				RateLimiter rateLimiter = RateLimiter.create(theTpsPerSession);

				Consumer consumer = new DefaultConsumer(channelRabbitConsume) {
				      @Override
				      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
				          throws IOException {
				        String message = new String(body, "UTF-8");
				        
						try{
							LoggingPooler.doLog(logger, "INFO", "TransceiverSMPPBSTILL", "readQueue - " + queueName, false, false, true, "", 
									"Receive message: " + message, null);

							// Throttle
							rateLimiter.acquire();

							LoggingPooler.doLog(logger, "INFO", "TransceiverSMPPBSTILL", "readQueue - " + queueName, false, false, true, "", 
									"Transmit to session: " + theSession.getConfiguration().getName() + ", message: " + message, null);

							// Process the message from queue
							// Transmit should be in multi thread for faster performance
							if (!theSession.isClosed() || !theSession.isUnbinding()) {
								theSession = Bind();
							}
							
					        //Transmit(theSession, message);
							Runnable theHit = new SMPPSubmitter(theSession, message);							
							Thread thread = new Thread(theHit);
							thread.start();
						} finally {
							LoggingPooler.doLog(logger, "INFO", "TransceiverSMPPBSTILL", "readQueue - " + queueName, false, false, true, "", 
									"Done processing message: " + message, null);

							channelRabbitConsume.basicAck(envelope.getDeliveryTag(), false);
						}
				      }
				};

				boolean autoAck = false; // If not finally exectued well, no ack to rabbitmq, message not gone
				channelRabbitConsume.basicConsume(queueName, autoAck, consumer);
			} catch (Exception e){
				e.printStackTrace();
				LoggingPooler.doLog(logger, "INFO", "TransceiverSMPPBSTILL", "readQueue - " + queueName, true, false, false, "", 
						"Failed to access queue " + queueName, e);
			}
		}

		@Override
		public void run() {
			try {
				theSession = Bind();
				
				if (theSession != null) {
					theSession.getConfiguration().setName(this.sessionName);
					LoggingPooler.doLog(logger, "INFO", "TransceiverSMPPBSTILL", "run", false, false, true, "", 
							"Binding session: " + this.sessionName, null);

					readQueue();
				} else {
					LoggingPooler.doLog(logger, "INFO", "TransceiverSMPPBSTILL", "readQueue - " + queueName, false, false, false, "", 
							"Failed to bind. Check network connection to vendor.", null);
				}
			} catch (Exception e) {
				e.printStackTrace();
				LoggingPooler.doLog(logger, "INFO", "TransceiverSMPPBSTILL", "readQueue - " + queueName, true, false, false, "", 
						"Failed to access queue " + queueName, e);
			}
		}   
		
		private class SMPPSubmitter implements Runnable {
			private SmppSession session0;
			private String queueMessage;
			
			public SMPPSubmitter(SmppSession theSession, String theQueueMessage) {
				this.session0 = theSession;
				this.queueMessage = theQueueMessage;				
			}
			
			private void Transmit(SmppSession theSession, String queueMessage){
				// queueMessage: {"messageId": "messageId", "msisdn": "6281234567890", "message": "Chandra coba kirim SMS", "vendorSenderId": "MITSUBISHI",
				// "telecomId": "62001", "vendorParameter": "", "routedVendorId": "DMY190326"}
//				jsonQueuedMessage.put("messageId", messageId);
//				jsonQueuedMessage.put("msisdn", msisdn);
//				jsonQueuedMessage.put("message", theSMS);
//				jsonQueuedMessage.put("vendorSenderId", vendorSenderId);
//				jsonQueuedMessage.put("telecomId", telecomId);
//				jsonQueuedMessage.put("vendorParameter", vendorParameter);
//				jsonQueuedMessage.put("routedVendorId", routedVendorId);		
				
				try{				
					// Convert queueMessage to JSON
					JSONObject jsonMessage = new JSONObject(queueMessage);
					
					// Extract jsonMessage
					String theMessageId = jsonMessage.getString("messageId");
					String theMsisdn = jsonMessage.getString("msisdn");
					String theMessage = jsonMessage.getString("message");
					String theVendorSenderId = jsonMessage.getString("vendorSenderId");
					//String theTelecomId = jsonMessage.getString("telecomId");
					String theVendorId = jsonMessage.getString("routedVendorId");
					//String theEncoding = "UCS2";
					
		            //byte[] textBytes = CharsetUtil.encode(theMessage, CharsetUtil.CHARSET_UTF_8);
					byte[] textBytes = CharsetUtil.encode(theMessage, CharsetUtil.CHARSET_UCS_2);
					//byte[] textBytes = CharsetUtil.encode(theMessage, CharsetUtil.CHARSET_UTF_8);
//		            if (theEncoding.equals("UCS2")) {
//		            	textBytes = CharsetUtil.encode(theMessage, CharsetUtil.CHARSET_UCS_2);
//		            }
		            
		            SubmitSm submit0 = new SubmitSm();

		            // add delivery receipt
		            submit0.setRegisteredDelivery(SmppConstants.REGISTERED_DELIVERY_SMSC_RECEIPT_REQUESTED);
		            submit0.setSourceAddress(new Address((byte)0x03, (byte)0x00, theVendorSenderId));
		            submit0.setDestAddress(new Address((byte)0x01, (byte)0x01, theMsisdn));
		            submit0.setDataCoding((byte) 0x08);
		            
		            //submit0.setDataCoding((byte) 0x08); // DEFAULT ENCODING
		            //submit0.setDataCoding(SmppConstants.DATA_CODING_UCS2);
		            //if (theEncoding.equals("UCS2") ) {
		            //	submit0.setDataCoding((byte) 0x08);
		            	//submit0.setDataCoding(SmppConstants.DATA_CODING_UCS2);
		            //}
		            
		            submit0.setShortMessage(textBytes);

		            LoggingPooler.doLog(logger, "INFO", "TransceiverSMPPBSTILL", "Transmitting: " + theSession.getConfiguration().getName(), false, false, true, theMessageId, 
							"PDU to send: " + submit0.toString(), null);
		            
		            // start sending datetime
		    		LocalDateTime startSendingDateTime = LocalDateTime.now();

		    		// send the message
	                SubmitSmResp submitResp = theSession.submit(submit0, 30000);
					LoggingPooler.doLog(logger, "INFO", "TransceiverSMPPBSTILL", "Transmitting: " + theSession.getConfiguration().getName(), false, false, true, theMessageId, 
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
					LoggingPooler.doLog(logger, "INFO", "TransceiverSMPPBSTILL", "Transmitting: " + theSession.getConfiguration().getName(), false, false, true, theMessageId, 
							"Transmitting - Save to database transaction vendor is DONE.", null);
					
					LoggingPooler.doLog(logger, "INFO", "TransceiverSMPPBSTILL", "Transmitting: " + theSession.getConfiguration().getName(), false, false, true, theMessageId, 
							"Done Transmitting! Submitting duration: " + ChronoUnit.MILLIS.between(startSendingDateTime, endSendingDateTime) + " milliseconds.", null);					
		        } catch (Exception e) {
					e.printStackTrace();				
					LoggingPooler.doLog(logger, "DEBUG", "TransceiverSMPPBSTILL", "Transmitter: " + theSession.getConfiguration().getName(), true, false, false, "", 
							"Failed to send message. ERROR OCCURED.", e);	
					
					// Re-initiate SMPP Connection
					theSession = Bind();
				} 
	    	}
			
			@Override
			public void run() {
				Transmit(this.session0, this.queueMessage);				
			}			
		}
		
    }	
}
