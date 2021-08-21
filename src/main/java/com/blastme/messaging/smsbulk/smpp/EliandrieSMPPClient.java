package com.blastme.messaging.smsbulk.smpp;

import com.blastme.messaging.toolpooler.DataSource;
import com.blastme.messaging.toolpooler.Tool;
import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.commons.gsm.GsmUtil;
import com.cloudhopper.smpp.SmppSessionConfiguration;
import com.cloudhopper.smpp.SmppBindType;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.impl.DefaultSmppClient;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.type.Address;
import com.cloudhopper.smpp.util.DeliveryReceipt;
import com.cloudhopper.smpp.pdu.DeliverSm;
import com.cloudhopper.smpp.pdu.PduRequest;
import com.cloudhopper.smpp.pdu.PduResponse;
import com.cloudhopper.smpp.pdu.SubmitSm;
import com.cloudhopper.smpp.pdu.SubmitSmResp;
import com.cloudhopper.smpp.ssl.SslConfiguration;
import com.cloudhopper.smpp.tlv.Tlv;

import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.dbcp2.BasicDataSource;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EliandrieSMPPClient {
    private static final Logger logger = LoggerFactory.getLogger(EliandrieSMPPClient.class);
    
    //private static String host = "smpp.sipintar.co.id";
//    private static String host = "52.74.245.195";
//    private static int port = 2776;

//    private static String host = "localhost";
//    private static int port = 2775;

    private static String host = "neosmpp.pastisystem.com";
    private static int port = 2775;
    
//    private static String host = "localhost";
//    private static int port = 2776;
    
    //private static String sysId = "max89039";
    //private static String pass = "tS52rg!y";
    
    //private static String sysId = "maxcm899";
    //private static String pass = "aHS0uYXB";
    
    private static String sysId = "king9388"; 
    private static String pass = "eWdlOzE5";
    
    //private static String sysId = "alienics";
    //private static String pass = "M9ICwTB1";
    		
    //private static String sysId = "tstax01";
    //private static String pass = "aO3o9YTX";
    
//    private static String host = "smpp.sipintar.co.id";
//    private static int port = 2778;
//    private static String sysId = "msgb17d8";
//    private static String pass = "ZL8v0ANB";
    
    // pcudemo
    // demo123
    
    // tstax01x
    // aO3o9YTX	

//    private static String host = "localhost";
//    private static int port = 2776;
//    private static String sysId = "chan02";
//    private static String pass = "chan02";

//    private static String host = "smpp.sipintar.co.id";
//    private static int port = 2776;
//    private static String sysId = "s1u7980";
//    private static String pass = "feg4KExf";

    //private static String senderId = "*WABA*";
    private static String senderId = "CHNAUTOGEN";
    private static String msisdn = "8615123456789";
    //private static String msisdn = "33679364376";
    
//    private static String senderId = "Facebook";
//    private static String msisdn = "628111253636";
    
    private static int sessionCount = 1;
    private static int messageCountPerSession = 1;
    
    private static int appId = 0;
    private static Tool tool;

    static SmppSession session0;
    
    private final static ExecutorService execCrut = Executors.newFixedThreadPool(sessionCount);
    
    static public void main(String[] args) throws Exception {
    	if (args.length == 8) {
    		host = args[0];
    		port = Integer.parseInt(args[1]);
    		sysId = args[2];
    		pass = args[3];
    		
    		senderId = args[4];
    		msisdn = args[5];
    		
    		sessionCount = Integer.parseInt(args[6]);
    		messageCountPerSession = Integer.parseInt(args[7]);
    	}
    	

    		System.out.println("Host: " + host + ", port: " + port + ", sysId: " + sysId + ", pass: " + pass + ", senderId: " + senderId +
    				", msisdn: " + msisdn);
    		
        	// Sending the message
    		tool = new Tool();
    		
        	Random rand = new Random();
        	
        	appId = rand.nextInt(1000) + 5;
        	
            ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newCachedThreadPool();
            
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
            
            DefaultSmppClient clientBootstrap = new DefaultSmppClient(Executors.newCachedThreadPool(), 1, monitorExecutor);
            
            DefaultSmppSessionHandler sessionHandler = new ClientSmppSessionHandler();

            SmppSessionConfiguration config0 = new SmppSessionConfiguration();
            config0.setWindowSize(1);
            config0.setName("Tester.Session.0");
            config0.setType(SmppBindType.TRANSCEIVER);
            config0.setHost(host);
            config0.setPort(port);
            config0.setConnectTimeout(1000000);
            config0.setBindTimeout(10000);
            config0.setSystemId(sysId);
            config0.setPassword(pass);
            config0.getLoggingOptions().setLogBytes(true);
            // to enable monitoring (request expiration)
            //config0.setRequestExpiryTimeout(300000);
            config0.setRequestExpiryTimeout(-1);
            config0.setCountersEnabled(true);
//            SslConfiguration sslConfig = new SslConfiguration();
//        	config0.setUseSsl(true);
//        	config0.setSslConfiguration(sslConfig);

            session0 = null;

            try {
                session0 = clientBootstrap.bind(config0, sessionHandler);
                
                System.out.println("Press any key to start transmitting ...");
                System.in.read();

                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yy-MM-dd HH:mm:ss.SSS");
        		LocalDateTime nowStartAll = LocalDateTime.now();
                System.out.println("ALL Start Date Time: " + nowStartAll.format(formatter));

                for(int x = 0; x < sessionCount; x++){
                	execCrut.execute(new transmitter("Session-" + x, clientBootstrap, sessionHandler, config0));
                }

//        		LocalDateTime nowEndAll = LocalDateTime.now();
//                System.out.println("ALL End Date Time: " + nowEndAll.format(formatter));
//    
//                long seconds = nowStartAll.until( nowEndAll, ChronoUnit.SECONDS);
//                System.out.println("Duration ALL (seconds): " + seconds);
//                System.out.println("Transactions per second ALL (per session): " + (messageCountPerSession * sessionCount)/seconds);

                System.out.println("Press any key to unbind and close sessions");
                System.in.read();
                System.in.read();
                
                session0.unbind(10000);
            } catch (Exception e) {
            	e.printStackTrace();
                //logger.error("", e);
            }

            if (session0 != null) {
                logger.info("Cleaning up session... (final counters)");
                if (session0.hasCounters()) {
                    System.out.println("tx-enquireLink: " + session0.getCounters().getTxEnquireLink());
                    System.out.println("tx-submitSM: " + session0.getCounters().getTxSubmitSM());
                    System.out.println("tx-deliverSM: " + session0.getCounters().getTxDeliverSM());
                    System.out.println("tx-dataSM: " + session0.getCounters().getTxDataSM());
                    System.out.println("rx-enquireLink: " + session0.getCounters().getRxEnquireLink());
                    System.out.println("rx-submitSM: " + session0.getCounters().getRxSubmitSM());
                    System.out.println("rx-deliverSM: " + session0.getCounters().getRxDeliverSM());
                    System.out.println("rx-dataSM: " + session0.getCounters().getRxDataSM());
                }
                
                session0.destroy();
                // alternatively, could call close(), get outstanding requests from
                // the sendWindow (if we wanted to retry them later), then call shutdown()
            }

            // this is required to not causing server to hang from non-daemon threads
            // this also makes sure all open Channels are closed to I *think*
            System.out.println("Shutting down client bootstrap and executors...");
            clientBootstrap.destroy();
            executor.shutdownNow();
            monitorExecutor.shutdownNow();
            
            System.out.println("Done. Exiting");        		
    }

    /**
     * Could either implement SmppSessionHandler or only override select methods
     * by extending a DefaultSmppSessionHandler.
     */
    public static class ClientSmppSessionHandler extends DefaultSmppSessionHandler {
    	BasicDataSource bdsBawah;
    	
        public ClientSmppSessionHandler() {
            super(logger);
            
            bdsBawah = DataSource.getInstance().getBds();
        }

        @SuppressWarnings("rawtypes")
		@Override
        public void firePduRequestExpired(PduRequest pduRequest) {
            System.out.println("PDU request expired: " + pduRequest);
        }

        @SuppressWarnings("rawtypes")
		@Override
        public PduResponse firePduRequestReceived(PduRequest pduRequest) {
        	System.out.println("firePduRequestReceiver: " + pduRequest);
        	
        	DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        	
        	try{
        		if(pduRequest.getCommandId() == SmppConstants.CMD_ID_DELIVER_SM){
        			LocalDateTime nowDate = LocalDateTime.now();
        			
        			System.out.println(nowDate.format(formatter) + ". AppId: " + appId + ". firePduRequestReceiver - shortmessage: " + CharsetUtil.decode(((DeliverSm) pduRequest).getShortMessage(), CharsetUtil.CHARSET_GSM));
        			
        			// Convert into deliverSm
        			DeliverSm deliverSm = (DeliverSm) pduRequest;
        			byte[] shortMessage = deliverSm.getShortMessage();
        			
        			String theDRContent = CharsetUtil.decode(shortMessage, CharsetUtil.CHARSET_GSM);
        			if (deliverSm.hasOptionalParameter(SmppConstants.TAG_MESSAGE_PAYLOAD)) {
            			theDRContent = CharsetUtil.decode(deliverSm.getOptionalParameter(SmppConstants.TAG_MESSAGE_PAYLOAD).getValue(), CharsetUtil.CHARSET_GSM);
        			}
        			System.out.println("theDRContent: " + theDRContent);
        			
        			DeliveryReceipt dr = DeliveryReceipt.parseShortMessage(theDRContent, DateTimeZone.forID("Asia/Jakarta")); 
        			
        			String messageId = dr.getMessageId();
        			String state = "ACCEPTD";
        			int countSubmitted = dr.getSubmitCount();
        			int countDelivered = dr.getDeliveredCount();

        			if(countDelivered > 0){
        				state = "DLVRD";
        			}
        			
        			System.out.println("theMessageId: " + messageId + ", submitCount: " + countSubmitted + ", deliveredCount: " + countDelivered);
        			
        			// Database
//        			Connection connection = null;
//        			Statement statement = null;
//        			try{
//            			connection = bdsBawah.getConnection();
//            			
//            			LocalDateTime now = LocalDateTime.now();
//            			String query = "INSERT INTO load_test_smpp_client_dlr(message_id, status_code, date_time, message) VALUES ('" + messageId + "', '" + state + "', '" + now.format(formatter) + "', '" + theDRContent + "')";
//            			
//            			statement = connection.createStatement();
//            			statement.execute(query);
//            			
//            			System.out.println(messageId + ". DLR is saved");
//        			} catch (Exception ex) {
//        				ex.printStackTrace();
//        			} finally {
//        				try{
//        					if(statement != null)
//        						statement.close();
//        					if(connection != null)
//        						connection.close();
//        				} catch (Exception ex1) {
//        					ex1.printStackTrace();
//        				}
//        			}
        		}
        	} catch (Exception e) {
        		e.printStackTrace();
        	}
        	
            PduResponse response = pduRequest.createResponse();
            
            return response;
        }
        
    }
    
    public static class transmitter implements Runnable{
    	//private SmppSession session0;
    	private String threadId;
    	private DefaultSmppClient clientBootstrap;
    	private DefaultSmppSessionHandler sessionHandler;
    	private SmppSessionConfiguration config0;
    	
    	private BasicDataSource bdsLagi;
    	    	
    	public transmitter(String threadId, DefaultSmppClient clientBootstrap, DefaultSmppSessionHandler sessionHandler, SmppSessionConfiguration config0) {
    		//this.session0 = session0;
    		this.threadId = threadId;
    		this.clientBootstrap = clientBootstrap;
    		this.sessionHandler = sessionHandler;
    		this.config0 = config0;
    		
    		bdsLagi = DataSource.getInstance().getBds();
		}

    	private static byte[][] splitUnicodeMessage(byte[] aMessage, Integer maximumMultipartMessageSegmentSize) {
    		final byte UDHIE_HEADER_LENGTH = 0x05;
    		final byte UDHIE_IDENTIFIER_SAR = 0x00;
    		final byte UDHIE_SAR_LENGTH = 0x03;

    		// determine how many messages have to be sent
    		int numberOfSegments = aMessage.length / maximumMultipartMessageSegmentSize;
    		int messageLength = aMessage.length;
    		if (numberOfSegments > 255) {
    			numberOfSegments = 255;
    			messageLength = numberOfSegments * maximumMultipartMessageSegmentSize;
    		}
    		if ((messageLength % maximumMultipartMessageSegmentSize) > 0) {
    			numberOfSegments++;
    		}

    		// prepare array for all of the msg segments
    		byte[][] segments = new byte[numberOfSegments][];

    		int lengthOfData;

    		// generate new reference number
    		byte[] referenceNumber = new byte[1];
    		new Random().nextBytes(referenceNumber);

    		// split the message adding required headers
    		for (int i = 0; i < numberOfSegments; i++) {
    			if (numberOfSegments - i == 1) {
    				lengthOfData = messageLength - i * maximumMultipartMessageSegmentSize;
    			} else {
    				lengthOfData = maximumMultipartMessageSegmentSize;
    			}
    			// new array to store the header
    			segments[i] = new byte[6 + lengthOfData];

    			// UDH header
    			// doesn't include itself, its header length
    			segments[i][0] = UDHIE_HEADER_LENGTH;
    			// SAR identifier
    			segments[i][1] = UDHIE_IDENTIFIER_SAR;
    			// SAR length
    			segments[i][2] = UDHIE_SAR_LENGTH;
    			// reference number (same for all messages)
    			segments[i][3] = referenceNumber[0];
    			// total number of segments
    			segments[i][4] = (byte) numberOfSegments;
    			// segment number
    			segments[i][5] = (byte) (i + 1);
    			// copy the data into the array
    			System.arraycopy(aMessage, (i * maximumMultipartMessageSegmentSize), segments[i], 6, lengthOfData);
    		}
    		return segments;
    	}
    	
		private void transmitting(){
			//Statement statement = null;
			//Connection connection = null;
			
			String text160 = "";
			try{
				// Prepare database connection ***
				//connection = bdsLagi.getConnection();
				
	            // create session a session by having the bootstrap connect a
	            // socket, send the bind request, and wait for a bind response
	            //SmppSession session0 = clientBootstrap.bind(config0, sessionHandler);
				//session0 = clientBootstrap.bind(config0, sessionHandler);

	            text160 = "CHandra testing SMS 123456.";
	            //text160 = "早上好，世界。 睡得好吗";
	            //text160 = "上帝的爱是最好的爱。";
	            //String text160 = appId + ". " + tool.generateUniqueID() + ". Lorem [ipsum] dolor sit amet, consectetur adipiscing elit.";
//	            text160 = tool.generateUniqueID() + ". Lorem [ipsum] dolor sit amet, consectetur adipiscing elit.";
	            //text160 = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc velit ligula, facilisis tempus quam convallis, finibus malesuada ipsum. Quisque bibendum scelerisque arcu. Maecenas aliquam tellus at maximus tristique. Donec dignissim vel enim ac viverra. Vestibulum sapien justo, lobortis eget finibus vel, pulvinar id diam. Aliquam lacinia, dolor in ullamcorper ornare, dolor odio consequat turpis, in maximus ex elit sed nibh. Nulla facilisi. Nulla at ultrices elit, a vestibulum orci. Nulla iaculis fermentum nisl in dictum. In molestie justo purus, vel congue lectus aliquam nec. Maecenas in nisl odio. Vestibulum at tortor dapibus, lobortis ipsum egestas, aliquet eros. Duis commodo convallis consequat. Praesent vel diam quis leo bibendum bibendum vel eu est. Ut at metus et lacus dictum efficitur.";
//	            text160 = "xxx Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc velit ligula, facilisis tempus quam convallis, finibus malesuada ipsum. Quisque bibendum scelerisque arcu. Maecenas aliquam tellus at maximus tristique. Donec dignissim vel enim ac viverra. Vestibulum sapien justo, lobortis eget finibus vel, pulvinar id diam. Aliquam lacinia, dolor in ullamcorper ornare, dolor odio consequat turpis, in maximus ex elit sed nibh. Nulla facilisi. Nulla at ultrices elit, a vestibulum orci. Nulla iaculis fermentum nisl in dictum. In molestie justo purus, vel congue lectus aliquam nec. Maecenas in nisl odio. Vestibulum at tortor dapibus, lobortis ipsum egestas, aliquet eros. Duis commodo convallis consequat. Praesent vel diam quis leo bibendum bibendum vel eu est. "
//	            		+ "Ut at metus et lacus dictum efficitur. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc velit ligula, facilisis tempus quam convallis, finibus malesuada ipsum. Quisque bibendum scelerisque arcu. Maecenas aliquam tellus at maximus tristique. Donec dignissim vel enim ac viverra. Vestibulum sapien justo, lobortis eget finibus vel, pulvinar id diam. Aliquam lacinia, dolor in ullamcorper ornare, dolor odio consequat turpis, in maximus ex elit sed nibh. Nulla facilisi. Nulla at ultrices elit, a vestibulum orci. Nulla iaculis fermentum nisl in dictum. In molestie justo purus, vel congue lectus aliquam nec. Maecenas in nisl odio. Vestibulum at tortor dapibus, lobortis ipsum egestas, aliquet eros. Duis commodo convallis consequat. Praesent vel diam quis leo bibendum bibendum vel eu est."
//	            		+ "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc velit ligula, facilisis tempus quam convallis, finibus malesuada ipsum. Quisque bibendum scelerisque arcu. Maecenas aliquam tellus at maximus tristique. Donec dignissim vel enim ac viverra. Vestibulum sapien justo, lobortis eget finibus vel, pulvinar id diam. Aliquam lacinia, dolor in ullamcorper ornare, dolor odio consequat turpis, in maximus ex elit sed nibh. Nulla facilisi. Nulla at ultrices elit, a vestibulum orci. Nulla iaculis fermentum nisl in dictum. In molestie justo purus, vel congue lectus aliquam nec. Maecenas in nisl odio. Vestibulum at tortor dapibus, lobortis ipsum egestas, aliquet eros. Duis commodo convallis consequat. Praesent vel diam quis leo bibendum bibendum vel eu est. "
//	            		+ "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc velit ligula, facilisis tempus quam convallis, finibus malesuada ipsum. Quisque bibendum scelerisque arcu. Maecenas aliquam tellus at maximus tristique. Donec dignissim vel enim ac viverra. Vestibulum sapien justo, lobortis eget finibus vel, pulvinar id diam. Aliquam lacinia, dolor in ullamcorper ornare, dolor odio consequat turpis, in maximus ex elit sed nibh. Nulla facilisi. Nulla at ultrices elit, a vestibulum orci. Nulla iaculis fermentum nisl in dictum. In molestie justo purus, vel congue lectus aliquam nec. Maecenas in nisl odio. Vestibulum at tortor dapibus, lobortis ipsum egestas, aliquet eros. Duis commodo convallis consequat. Praesent vel diam quis leo bibendum bibendum vel eu est. "
//	            		+ "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc velit ligula, facilisis tempus quam convallis, finibus malesuada ipsum. Quisque bibendum scelerisque arcu. Maecenas aliquam tellus at maximus tristique. Donec dignissim vel enim ac viverra. Vestibulum sapien justo, lobortis eget finibus vel, pulvinar id diam. Aliquam lacinia, dolor in ullamcorper ornare, dolor odio consequat turpis, in maximus ex elit sed nibh. Nulla facilisi. Nulla at ultrices elit, a vestibulum orci. Nulla iaculis fermentum nisl in dictum. In molestie justo purus, vel congue lectus aliquam nec. Maecenas in nisl odio. Vestibulum at tortor dapibus, lobortis ipsum egestas, aliquet eros. Duis commodo convallis consequat. Praesent vel diam quis leo bibendum bibendum vel eu est. "
//	            		+ "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc velit ligula, facilisis tempus quam convallis, finibus malesuada ipsum. Quisque bibendum scelerisque arcu. Maecenas aliquam tellus at maximus tristique. Donec dignissim vel enim ac viverra. Vestibulum sapien justo, lobortis eget finibus vel, pulvinar id diam. Aliquam lacinia, dolor in ullamcorper ornare, dolor odio consequat turpis, in maximus ex elit sed nibh. Nulla facilisi. Nulla at ultrices elit, a vestibulum orci. Nulla iaculis fermentum nisl in dictum. In molestie justo purus, vel congue lectus aliquam nec. Maecenas in nisl odio. Vestibulum at tortor dapibus, lobortis ipsum egestas, aliquet eros. Duis commodo convallis consequat. Praesent vel diam quis leo bibendum bibendum vel eu est. "
//	            		+ "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc velit ligula, facilisis tempus quam convallis, finibus malesuada ipsum. Quisque bibendum scelerisque arcu. Maecenas aliquam tellus at maximus tristique. Donec dignissim vel enim ac viverra. Vestibulum sapien justo, lobortis eget finibus vel, pulvinar id diam. Aliquam lacinia, dolor in ullamcorper ornare, dolor odio consequat turpis, in maximus ex elit sed nibh. Nulla facilisi. Nulla at ultrices elit, a vestibulum orci. Nulla iaculis fermentum nisl in dictum. In molestie justo purus, vel congue lectus aliquam nec. Maecenas in nisl odio. Vestibulum at tortor dapibus, lobortis ipsum egestas, aliquet eros. Duis commodo convallis consequat. Praesent vel diam quis leo bibendum bibendum vel eu est. "
//	            		+ "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc velit ligula, facilisis tempus quam convallis, finibus malesuada ipsum. Quisque bibendum scelerisque arcu. Maecenas aliquam tellus at maximus tristique. Donec dignissim vel enim ac viverra. Vestibulum sapien justo, lobortis eget finibus vel, pulvinar id diam. Aliquam lacinia, dolor in ullamcorper ornare, dolor odio consequat turpis, in maximus ex elit sed nibh. Nulla facilisi. Nulla at ultrices elit, a vestibulum orci. Nulla iaculis fermentum nisl in dictum. In molestie justo purus, vel congue lectus aliquam nec. Maecenas in nisl odio. Vestibulum at tortor dapibus, lobortis ipsum egestas, aliquet eros. Duis commodo convallis consequat. Praesent vel diam quis leo bibendum bibendum vel eu est. "
//	            		+ "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc velit ligula, facilisis tempus quam convallis, finibus malesuada ipsum. Quisque bibendum scelerisque arcu. Maecenas aliquam tellus at maximus tristique. Donec dignissim vel enim ac viverra. Vestibulum sapien justo, lobortis eget finibus vel, pulvinar id diam. Aliquam lacinia, dolor in ullamcorper ornare, dolor odio consequat turpis, in maximus ex elit sed nibh. Nulla facilisi. Nulla at ultrices elit, a vestibulum orci. Nulla iaculis fermentum nisl in dictum. In molestie justo purus, vel congue lectus aliquam nec. Maecenas in nisl odio. Vestibulum at tortor dapibus, lobortis ipsum egestas, aliquet eros. Duis commodo convallis consequat. Praesent vel diam quis leo bibendum bibendum vel eu est. "
//	            		+ "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc velit ligula, facilisis tempus quam convallis, finibus malesuada ipsum. Quisque bibendum scelerisque arcu. Maecenas aliquam tellus at maximus tristique. Donec dignissim vel enim ac viverra. Vestibulum sapien justo, lobortis eget finibus vel, pulvinar id diam. Aliquam lacinia, dolor in ullamcorper ornare, dolor odio consequat turpis, in maximus ex elit sed nibh. Nulla facilisi. Nulla at ultrices elit, a vestibulum orci. Nulla iaculis fermentum nisl in dictum. In molestie justo purus, vel congue lectus aliquam nec. Maecenas in nisl odio. Vestibulum at tortor dapibus, lobortis ipsum egestas, aliquet eros. Duis commodo convallis consequat. Praesent vel diam quis leo bibendum bibendum vel eu est. "
//	            		+ "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc velit ligula, facilisis tempus quam convallis, finibus malesuada ipsum. Quisque bibendum scelerisque arcu. Maecenas aliquam tellus at maximus tristique. Donec dignissim vel enim ac viverra. Vestibulum sapien justo, lobortis eget finibus vel, pulvinar id diam. Aliquam lacinia, dolor in ullamcorper ornare, dolor odio consequat turpis, in maximus ex elit sed nibh. Nulla facilisi. Nulla at ultrices elit, a vestibulum orci. Nulla iaculis fermentum nisl in dictum. In molestie justo purus, vel congue lectus aliquam nec. Maecenas in nisl odio. Vestibulum at tortor dapibus, lobortis ipsum egestas, aliquet eros. Duis commodo convallis consequat. Praesent vel diam quis leo bibendum bibendum vel eu est. "
//	            		+ "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc velit ligula, facilisis tempus quam convallis, finibus malesuada ipsum. Quisque bibendum scelerisque arcu. Maecenas aliquam tellus at maximus tristique. Donec dignissim vel enim ac viverra. Vestibulum sapien justo, lobortis eget finibus vel, pulvinar id diam. Aliquam lacinia, dolor in ullamcorper ornare, dolor odio consequat turpis, in maximus ex elit sed nibh. Nulla facilisi. Nulla at ultrices elit, a vestibulum orci. Nulla iaculis fermentum nisl in dictum. In molestie justo purus, vel congue lectus aliquam nec. Maecenas in nisl odio. Vestibulum at tortor dapibus, lobortis ipsum egestas, aliquet eros. Duis commodo convallis consequat. Praesent vel diam quis leo bibendum bibendum vel eu est. "
//	            		+ "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc velit ligula, facilisis tempus quam convallis, finibus malesuada ipsum. Quisque bibendum scelerisque arcu. Maecenas aliquam tellus at maximus tristique. Donec dignissim vel enim ac viverra. Vestibulum sapien justo, lobortis eget finibus vel, pulvinar id diam. Aliquam lacinia, dolor in ullamcorper ornare, dolor odio consequat turpis, in maximus ex elit sed nibh. Nulla facilisi. Nulla at ultrices elit, a vestibulum orci. Nulla iaculis fermentum nisl in dictum. In molestie justo purus, vel congue lectus aliquam nec. Maecenas in nisl odio. Vestibulum at tortor dapibus, lobortis ipsum egestas, aliquet eros. Duis commodo convallis consequat. Praesent vel diam quis leo bibendum bibendum vel eu est. "
//	            		+ "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc velit ligula, facilisis tempus quam convallis, finibus malesuada ipsum. Quisque bibendum scelerisque arcu. Maecenas aliquam tellus at maximus tristique. Donec dignissim vel enim ac viverra. Vestibulum sapien justo, lobortis eget finibus vel, pulvinar id diam. Aliquam lacinia, dolor in ullamcorper ornare, dolor odio consequat turpis, in maximus ex elit sed nibh. Nulla facilisi. Nulla at ultrices elit, a vestibulum orci. Nulla iaculis fermentum nisl in dictum. In molestie justo purus, vel congue lectus aliquam nec. Maecenas in nisl odio. Vestibulum at tortor dapibus, lobortis ipsum egestas, aliquet eros. Duis commodo convallis consequat. Praesent vel diam quis leo bibendum bibendum vel eu est. yyy";
	            //text160 = "{\"msisdn\": \"628111253636\", \"templateId\": \"2342\", \"parameterCount\": 1, \"parameter\": \"{\\\"0\\\": \\\"Chandra\\\"}\"}";
	            //text160 = "1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 xxx";
	            
	            // WABA Response Chat
//	            text160 = "{\"type\": \"wa_response\", "
//	            		+ "\"contentType\": \"image\", "
//	            		+ "\"msisdn\": \"" + msisdn + "\", "
//	            		+ "\"text\": \"Hi Jean. This is Chandra. I love jeep. Do you?\","
//	            		+ "\"binaryLink\": \"https://i.pinimg.com/564x/65/1b/fb/651bfb906a0b2281126cb4a532854a3d.jpg\"}";
	            
	            // WABA Push Template
	            //text160 = "{ \"type\": \"wa_push\", \"contentType\": \"text\", \"templateId\": \"code\", \"parameter\": {\"0\": \"102938\"}, \"msisdn\": \"628111253636\" }";
	            
	            //byte[] textBytes = CharsetUtil.encode(text160, CharsetUtil.CHARSET_GSM);
	            byte[] textBytes = CharsetUtil.encode(text160, CharsetUtil.CHARSET_UCS_2);
	            
	            // Client ini hanya kirimkan GSM saja, bukan UCS2

	            // Handle log message
	            byte theDataCoding = (byte) 0x08;
	            if (textBytes.length > 255) {
	            	// WA Message is using TLV and assumed length is more than 255
	            	SubmitSm submit0 = new SubmitSm();
	            	// Add TLV 
	            	submit0.addOptionalParameter(new Tlv(SmppConstants.TAG_MESSAGE_PAYLOAD, textBytes, "message_payload"));
	            	
	            	// add delivery receipt
		            //submit0.setRegisteredDelivery(SmppConstants.REGISTERED_DELIVERY_SMSC_RECEIPT_REQUESTED);
		            submit0.setSourceAddress(new Address((byte)0x03, (byte)0x00, senderId));
		            submit0.setDestAddress(new Address((byte)0x01, (byte)0x01, msisdn));
		            submit0.setDataCoding(theDataCoding);
		            
		            
		            //submit0.setDataCoding((byte) 0x00); // DEFAULT ENCODING
		            //submit0.setShortMessage(textBytes);
		            
		            SubmitSmResp submitResp = session0.submit(submit0, 30000);
		            
		            System.out.println("SUBMITING ONE MESSAGE - Done messageId: " + submitResp.getMessageId());
	            } else if (textBytes.length > 160) {
	            	System.out.println("TEXT LENGTH IS > 160");
		            // Split message per 134 byte
		            //byte[][] msgParts = splitUnicodeMessage(textBytes,134);
	            	Random random = new Random();
	            	int refNum = random.nextInt();
	            	byte[][] msgParts = GsmUtil.createConcatenatedBinaryShortMessages(textBytes, (byte)refNum);
		            
		            for (int i = 0; i < msgParts.length; i++) {
		    			SubmitSm submit0 = new SubmitSm();
		    			submit0.setEsmClass(SmppConstants.ESM_CLASS_UDHI_MASK);
		    			submit0.setRegisteredDelivery(SmppConstants.REGISTERED_DELIVERY_SMSC_RECEIPT_REQUESTED);
			            submit0.setSourceAddress(new Address((byte)0x03, (byte)0x00, senderId));
			            submit0.setDestAddress(new Address((byte)0x01, (byte)0x01, msisdn));
			            submit0.setDataCoding(theDataCoding); 
		    			submit0.setShortMessage(msgParts[i]);	

			            System.out.println("Sending msgPart[" + i + "]: " + Arrays.toString(msgParts[i]));
			            SubmitSmResp submitResp = session0.submit(submit0, 30000);
			            
			            System.out.println("SUBMITING MULTI MESSAGES - Done messageId: " + submitResp.getMessageId());
		    		}
	            } else {
		            SubmitSm submit0 = new SubmitSm();

		            // add delivery receipt
		            //submit0.setRegisteredDelivery(SmppConstants.REGISTERED_DELIVERY_SMSC_RECEIPT_REQUESTED);
		            submit0.setSourceAddress(new Address((byte)0x03, (byte)0x00, senderId));
		            submit0.setDestAddress(new Address((byte)0x01, (byte)0x01, msisdn));
		            submit0.setDataCoding(theDataCoding); 
		            submit0.setShortMessage(textBytes);
		            
		            SubmitSmResp submitResp = session0.submit(submit0, 30000);
		            
		            System.out.println("SUBMITING ONE MESSAGE - Done messageId: " + submitResp.getMessageId());
	            }

//	            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
//	    		LocalDateTime nowStart = LocalDateTime.now();
//
//	            System.out.println("Start Date Time: " + nowStart.format(formatter));
//	            for(int x = 0; x < messageCountPerSession; x++){
//	            	System.out.println(this.threadId + " - " + x + ". Sending message: " + text160);
//
//	            	LocalDateTime nowDate = LocalDateTime.now();
//	                SubmitSmResp submitResp = session0.submit(submit0, 30000);
//	                System.out.println(this.threadId + " - " + x + ". " + nowDate.format(formatter) + ". appId: " + appId + " . MessageID: " + submitResp.getMessageId() +  ",  submitResp: " + submitResp.getResultMessage());
//	                
//	                System.out.println(this.threadId + " - " + x + ". appId: " + appId + ". sendWindow.size: " + session0.getSendWindow().getSize());
//	                
//	                // Save to DB
//	                String query = "INSERT INTO load_test_smpp_client(date_time, a_number, b_number, message, response, sys_id, message_id) VALUES ('" + LocalDateTime.now().format(formatter) + "', '" + senderId + 
//	                		"', '" + msisdn + "', '" + text160 + "', '" + submitResp.toString() + "', '" + sysId + "', '" + submitResp.getMessageId()  + "')";
//	                
//	                statement = connection.createStatement();
//	                
//	                statement.execute(query);
//	                
//	                System.out.println(this.threadId + " - " + x + ". appId: " + appId + ". Save to DB.");
//	            }
//	    		LocalDateTime nowEnd = LocalDateTime.now();
//	            System.out.println("End Date Time: " + nowEnd.format(formatter));
//
//	            long seconds = nowStart.until( nowEnd, ChronoUnit.SECONDS);
//	            if(seconds == 0){
//	            	seconds = 1;
//	            }
//	            System.out.println("Duration (seconds): " + seconds);
//	            
//	            System.out.println("Transactions per second (per session): " + messageCountPerSession/seconds);
	            
	            System.out.println("DONE Sending Message(s)");
	        } catch (com.cloudhopper.smpp.type.SmppTimeoutException ex){
	        	ex.printStackTrace();
	        	System.out.println("NO RESPONSE ERROR. MESSAGE: " + text160);
	        } catch (Exception e) {
				e.printStackTrace();				
			} finally {
//				try{
//					if(statement != null)
//						statement.close();
//					if(connection != null)
//						connection.close();
//				} catch (Exception ex) {
//					ex.printStackTrace();
//				}
			}
    	}

		@Override
		public void run() {
			transmitting();
		}    	
    }
    
}