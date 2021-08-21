package com.blastme.messaging.smsbulk.transceiver;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.json.JSONObject;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.DataSource;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.RedisPooler;
import com.blastme.messaging.toolpooler.Tool;
import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import io.lettuce.core.api.sync.RedisCommands;

public class TransceiverTelinVoiceBlastWithThrottlingTelin {
	private static Logger logger;	

	private static Tool tool;
	private static String thisQueueName;
	private static String thisLoggingName;
	private static int tps;
	
	private RabbitMQPooler rabbitMqPooler;
	private com.rabbitmq.client.Connection connectionRabbit;
	private Channel channelRabbit;
	
	private static RedisPooler redisPooler;
	private static RedisCommands<String, String> redisCommand;
	
	private Connection connection = null;
	private PreparedStatement statementUpdateTrxStatus = null;
	
	public TransceiverTelinVoiceBlastWithThrottlingTelin(String queueName, int theTPS) {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("TRANSCEIVER_TELINVOICEBLAST");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate Tool
		tool = new Tool();
		
		// Initiate thisQueueName and thisLoggingName
		thisQueueName = queueName;
		thisLoggingName = "TRANSCEIVERTELINVOICEBLAST - " + thisQueueName;
		tps = theTPS;
		
		try {			
			// Initiate RabbitMQPooler
			rabbitMqPooler = new RabbitMQPooler();
			connectionRabbit = rabbitMqPooler.getConnection();
			channelRabbit = rabbitMqPooler.getChannel(connectionRabbit);
			
			// Initiate redis
			// Initiate RedisPooler
			redisPooler = new RedisPooler();
			redisCommand = redisPooler.redisInitiateConnection();
			
			// Initiate Database
            BasicDataSource bds = DataSource.getInstance().getBds();
            connection = bds.getConnection();
            
            // Initiate statementUpdateTrxStatus
            statementUpdateTrxStatus = connection.prepareStatement("update transaction_sms set status_code = ? where message_id = ?");
            
            // Load jsonAccIdTokenId to redis
			loadTelinAccIdTokenId();

    		LoggingPooler.doLog(logger, "INFO", thisLoggingName, "TransceiverTelinVoiceBlast", false, false, true, "", 
    				"Application TransceiverTelinVoiceBlast is running ...", null);
		} catch (Exception e) {
			e.printStackTrace();
			
			System.out.println("Failed to run the Transceiver. Exception is raise.");
			System.exit(-1);
		}
	}
	
	private void loadTelinAccIdTokenId() {
		// URL: https://AC4cbc10ff1034289e76a0d288919af71c:cc7daae3ade65aab133930d1c489a230@telin.restcomm.com/restcomm/2012-04-24/Accounts/AC4cbc10ff1034289e76a0d288919af71c/SMS/Messages
		// [accID]:[tokenID] = AC4cbc10ff1034289e76a0d288919af71c:cc7daae3ade65aab133930d1c489a230
		// Lay in between https:// and @
		try {
            Statement statementAcc = connection.createStatement();
            String query = "select api_username, telin_url from transceiver_telin_property where is_active = true";
            
            ResultSet resultSet = statementAcc.executeQuery(query);
            
            while(resultSet.next()){
            	// Get combinedAccIdTokenId
            	String completeUrl = resultSet.getString("telin_url");
            	String combinedAccIdTokenId = StringUtils.substringBetween(completeUrl, "https://", "@");
        		LoggingPooler.doLog(logger, "DEBUG", thisLoggingName, "loadTelinAccIdTokenId", false, false, false, "", 
        				"completeUrl: " + completeUrl + ", combinedAccIdTokenId: " + combinedAccIdTokenId, null);	

        		String[] splitedCombinedAccToken = combinedAccIdTokenId.split(":");
        		String accId = splitedCombinedAccToken[0];
        		String tokenId = splitedCombinedAccToken[1];
        		
        		// Put into jsonDetail
            	JSONObject jsonDetail = new JSONObject();

            	jsonDetail.put("accId", accId);
            	jsonDetail.put("tokenId", tokenId);
            	
            	// Put into redis
            	String redisKey = "telinvoicetransceiver-" + resultSet.getString("api_username");
            	String redisVal = jsonDetail.toString();
            	
            	redisPooler.redisSet(redisCommand, redisKey, redisVal);
        		LoggingPooler.doLog(logger, "DEBUG", thisLoggingName, "loadTelinAccIdTokenId", false, false, false, "", 
        				"Load voice transceiver accId & tokenId to redis - apiUsername: " + resultSet.getString("api_username") + 
        				", redis value: " + redisVal, null);	
            }	
		} catch (Exception e) {
			e.printStackTrace();
			
    		LoggingPooler.doLog(logger, "DEBUG", thisLoggingName, "loadTelinAccIdTokenId", true, false, false, "", 
    				"Failed to load to redis TELIN Acc ID and Token ID. Error occured.", e);	
		}
	}
	
	private void readQueue(){
		try{
			channelRabbit.queueDeclare(thisQueueName, true, false, false, null);
			channelRabbit.basicQos(50);
			
			// Guava rateLimiter
			RateLimiter rateLimiter = RateLimiter.create(tps);
			
			LoggingPooler.doLog(logger, "INFO", thisLoggingName, "readQueue", false, false, true, "", 
					"Reading queue " + thisQueueName + " is ready, set transmitting speed to " + tps + " tps. Waiting for message ... ", null);
			
			Consumer consumer = new DefaultConsumer(channelRabbit) {
			      @Override
			      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
			          throws IOException {
			        String message = new String(body, "UTF-8");
			        
					try{
						LoggingPooler.doLog(logger, "INFO", thisLoggingName, "readQueue", false, false, true, "", 
								"Receive message: " + message, null);

						// Hit TELIN
						rateLimiter.acquire();
						LoggingPooler.doLog(logger, "INFO", thisLoggingName, "readQueue", false, false, true, "", 
								"Hitting Telin Voice API: " + message, null);
						
						Runnable theHit = new hitTelin(thisQueueName, message, statementUpdateTrxStatus);
						
						Thread thread = new Thread(theHit);
						thread.start();						
					} finally {
						LoggingPooler.doLog(logger, "INFO", thisLoggingName, "readQueue", false, false, true, "", 
								"Done processing message: " + message, null);

						channelRabbit.basicAck(envelope.getDeliveryTag(), false);
					}
			      }
			};

			boolean autoAck = false; // If not finally exectued well, no ack to rabbitmq, message not gone
			channelRabbit.basicConsume(thisQueueName, autoAck, consumer);
		} catch (Exception e){
			LoggingPooler.doLog(logger, "INFO", thisLoggingName, "readQueue", true, false, false, "", 
					"Failed to access queue " + thisQueueName, e);
		}
	}
	
	public static void main(String[] args) {
		//String queueTelin = "TELIN01";
		String queueTelin = args[0];
		String tpsTelin = args[1];
		int theTPS = Integer.parseInt(tpsTelin);
		
		TransceiverTelinVoiceBlastWithThrottlingTelin cpass = new TransceiverTelinVoiceBlastWithThrottlingTelin(queueTelin, theTPS);
		
		System.out.println("Starting Transceiver TELIN for VOICE BLAST INDOSAT by reading queue " + queueTelin);
		cpass.readQueue();		
		System.out.println("Transceiver TELIN for VOICE BLAST INDOSAT is RUNNING with queue " + queueTelin);
	}
	
	
	
	
	
	
	// -- Hit TELIN thread! -- //
	private class hitTelin implements Runnable {
		private String theQueueName;
		private String theMessage;
		private String loggingName;
		
		private PreparedStatement theStatementUpdateTrxStatus;
		
		public hitTelin(String queueName, String message, PreparedStatement statementUpdateTrxStatus) {
			this.theQueueName = queueName;
			this.theMessage = message;
			this.loggingName = "TransceiverTelinVOICEBLAST - " + theQueueName;
			
			this.theStatementUpdateTrxStatus = statementUpdateTrxStatus;
		}

		//"To=628119692829" -d "From=622125070001" -d "Body=Test Thank you
		private List<NameValuePair> composeParameter(String messageId, String accId, String tokenId, String from, String msisdn, String message){
			List<NameValuePair> listParams = new ArrayList<NameValuePair>();

			try{
				// Development - Remove it for production
				//from = "622125070001";
				//from = "MITSUBISHI";
				String urlCallback = "http://sms.sipintar.co.id:8898/telincb/voiceotp";
				
				// Default TELIN
//				String urlApp = "https://ACd60c3ce0c042d3588ff1597731ec4830:de0254297ee5fb844b1dd830ba7f8e8a@jatis.neuapix.com/visual-designer/"
//						+ "services/apps/AP1f6eca9735b347eea1262daeb3d5b6c7/controller?SipHeader_X-message=" + URLEncoder.encode(message, "UTF-8");
//				if (accId.trim().length() > 0 && tokenId.trim().length() > 0) {
//					urlApp = "https://" + accId + ":" + tokenId + "@jatis.neuapix.com/visual-designer/"
//							+ "services/apps/AP1f6eca9735b347eea1262daeb3d5b6c7/controller?SipHeader_X-message=" + URLEncoder.encode(message, "UTF-8");
//				}

//				// INDOSAT
//				String urlApp = "https://ACd60c3ce0c042d3588ff1597731ec4830:de0254297ee5fb844b1dd830ba7f8e8a@telin.neuapix.com/visual-designer/"
//						+ "services/apps/APb95284d9cb504830826bce0356cd54e2/controller?SipHeader_X-message=" + URLEncoder.encode(message, "UTF-8");
//				if (accId.trim().length() > 0 && tokenId.trim().length() > 0) {
//					urlApp = "https://" + accId + ":" + tokenId + "@telin.neuapix.com/visual-designer/services/"
//							+ "apps/APb95284d9cb504830826bce0356cd54e2/controller?SipHeader_X-message=" + URLEncoder.encode(message, "UTF-8");
//				}

				// ATLASAT
				//String urlApp = "https://ACd28d50e74cb76f2bd143044164fb09e7:4b7a5bc000682b815e3f96a5835ab7f8@telin.neuapix.com/visual-designer/"
				//		+ "services/apps/AP5e22f1aa1fc147aeb6ea481103eeb541/controller?SipHeader_X-message=" + URLEncoder.encode(message, "UTF-8");
				
				// TELIN SECOND ACCOUNT
				String urlApp = "https://ACa484ce50e351b76d1315f133316ac323:4b7a5bc000682b815e3f96a5835ab7f8@telin.neuapix.com/visual-designer/"
						+ "services/apps/AP83315a38a5b640dc822a1533a0069ab8/controller?SipHeader_X-message=" + URLEncoder.encode(message, "UTF-8");
//				String urlApp = "https://ACa484ce50e351b76d1315f133316ac323:4b7a5bc000682b815e3f96a5835ab7f8@telin.neuapix.com/visual-designer/"
//				+ "services/apps/AP24fb2368894940f992621e07f1779637/controller?SipHeader_X-message=" + URLEncoder.encode(message, "UTF-8");

				
//				if (accId.trim().length() > 0 && tokenId.trim().length() > 0) {
//					urlApp = "https://" + accId + ":" + tokenId + "@telin.neuapix.com/visual-designer/services/"
//							+ "apps/AP5e22f1aa1fc147aeb6ea481103eeb541/controller?SipHeader_X-message=" + URLEncoder.encode(message, "UTF-8");
//				}

				listParams.add(new BasicNameValuePair("From", from));
				listParams.add(new BasicNameValuePair("To", msisdn));
				listParams.add(new BasicNameValuePair("Url", urlApp));
				listParams.add(new BasicNameValuePair("StatusCallback", urlCallback));
			} catch (Exception e) {
				e.printStackTrace();
				LoggingPooler.doLog(logger, "DEBUG", this.loggingName, "composeGetParameter", true, false, false, messageId, 
						"Failed to compose GET parameter message.", e);
			}
			
			return listParams;
		}
		
		private void updateTransactionStatus(String messageId, String trxStatus) {
			try {
				this.theStatementUpdateTrxStatus.setString(1, trxStatus);
				this.theStatementUpdateTrxStatus.setString(2, messageId);
				
				int impactedNumber = this.theStatementUpdateTrxStatus.executeUpdate();
				
				if (impactedNumber > 0) {
					LoggingPooler.doLog(logger, "DEBUG", this.loggingName, "updateTransactionStatus", false, false, false, messageId, 
							"Updating transaction status is SUCCESS. Impacted row: " + impactedNumber, null);			
				} else {
					LoggingPooler.doLog(logger, "DEBUG", this.loggingName, "updateTransactionStatus", false, false, false, messageId, 
							"Updating transaction status is FAILED. Impacted row: " + impactedNumber, null);			
				}
			} catch (Exception e) {
				e.printStackTrace();
				
				LoggingPooler.doLog(logger, "DEBUG", this.loggingName, "updateTransactionStatus", true, false, false, messageId, 
						"Failed to update transaction status. Error occured", e);			
			}
		}
		
		private void saveMapMessageidSid(String messageId, String callSid) {
			try {
				// Map callSid to messageId
				String redisKey_mapCallSidToMsgId = "mapCallSidToMsgId-" + callSid;
				String redisVal_mapCallSidTomsgId = messageId;
				
				redisPooler.redisSet(redisCommand, redisKey_mapCallSidToMsgId, redisVal_mapCallSidTomsgId);
				
				// Map messageId to callSid
				String redisKey_mapMsgIdToCallSid = "mapMsgIdToCallSid-" + messageId;
				String redisVal_mapMsgIdToCallSid = callSid;
				
				redisPooler.redisSet(redisCommand, redisKey_mapMsgIdToCallSid, redisVal_mapMsgIdToCallSid);
			} catch (Exception e) {
				e.printStackTrace();
				LoggingPooler.doLog(logger, "DEBUG", this.loggingName, "saveMapMessageIdSid", true, false, false, messageId, 
						"Failed to save mapping messageId and callSid to redis. Error occured.", e);								
			}
		}
		
		private String getTelinURL(String messageId, String accId, String tokenId, String apiUserName){
			String theURL = "";
			
			try{
				// Default pake account chandrapawitra02
				//theURL = "https://ACd60c3ce0c042d3588ff1597731ec4830:de0254297ee5fb844b1dd830ba7f8e8a@jatis.neuapix.com/restcomm/2012-04-24/Accounts/"
				//		+ "ACd60c3ce0c042d3588ff1597731ec4830/Calls";
				
//				// Voice Indosat TELIN
//				theURL = "https://ACd28d50e74cb76f2bd143044164fb09e7:4b7a5bc000682b815e3f96a5835ab7f8@telin.neuapix.com/restcomm/"
//						+ "2012-04-24/Accounts/ACd28d50e74cb76f2bd143044164fb09e7/Calls";
//				
//				if (accId.trim().length() > 0 || tokenId.trim().length() > 0) {
//					theURL = "https://" + accId + ":" + tokenId + "@telin.neuapix.com/restcomm/2012-04-24/Accounts/" + accId + "/Calls";
//				} 				
//				
				// Voice Indosat ATLASAT
				//theURL = "https://ACd28d50e74cb76f2bd143044164fb09e7:4b7a5bc000682b815e3f96a5835ab7f8@telin.neuapix.com/restcomm/2012-04-24/Accounts/ACd28d50e74cb76f2bd143044164fb09e7/Calls";
				
				// Voice TELIN SECOND ACCOUNT
				theURL = "https://ACa484ce50e351b76d1315f133316ac323:4b7a5bc000682b815e3f96a5835ab7f8@telin.neuapix.com/restcomm/2012-04-24/Accounts/ACa484ce50e351b76d1315f133316ac323/Calls";

				
//				if (accId.trim().length() > 0 || tokenId.trim().length() > 0) {
//					theURL = "https://" + accId + ":" + tokenId + "@telin.neuapix.com/restcomm/2012-04-24/Accounts/" + accId + "/Calls";
//				} 				
				
				LoggingPooler.doLog(logger, "DEBUG", this.loggingName, "getTelinURL", false, false, false, messageId, 
						"apiUserName: " + apiUserName + ", accId: " + accId + ", tokenId: " + tokenId + ", theURL: " + theURL, null);				
			} catch (Exception e) {
				e.printStackTrace();
				LoggingPooler.doLog(logger, "DEBUG", this.loggingName, "getTelinURL", true, false, false, messageId, 
						"Failed to compose URL to hit to TELIN. Error occured.", e);				
			}			
			
			return theURL;
		}

		private void processQueueData(){
			System.out.println("Thread Incoming Message: " + theMessage);
						
//			jsonQueuedMessage.put("messageId", messageId);
//			jsonQueuedMessage.put("msisdn", msisdn);
//			jsonQueuedMessage.put("message", theSMS);
//			jsonQueuedMessage.put("vendorSenderId", vendorSenderId);
//			jsonQueuedMessage.put("telecomId", telecomId);
//			jsonQueuedMessage.put("vendorParameter", vendorParameter);
//			jsonQueuedMessage.put("routedVendorId", routedVendorId);	
			
			try{
				JSONObject jsonMessage = new JSONObject(theMessage);
				
				LoggingPooler.doLog(logger, "DEBUG", this.loggingName, "processQueueData", false, false, false, "", 
						"jsonMessage: " + jsonMessage.toString(), null);
				
				String messageId = jsonMessage.getString("messageId");
				String msisdn = jsonMessage.getString("msisdn");
				String theSMS = jsonMessage.getString("message");
				String vendorSenderId = jsonMessage.getString("vendorSenderId");
				String apiUserName = jsonMessage.getString("apiUserName");
				
				String accId = "";
				String tokenId = "";
				
				// Get accId and tokenId
				String redisKey = "telinvoicetransceiver-" + apiUserName.trim();
				String redisVal = redisPooler.redisGet(redisCommand, redisKey);
				
				if (redisVal.trim().length() > 0) {
					JSONObject jsonAccToken = new JSONObject(redisVal);
					
					accId = jsonAccToken.getString("accId").trim();
					tokenId = jsonAccToken.getString("tokenId").trim();
				}
				
				String telinUrl = getTelinURL(messageId, accId, tokenId, apiUserName);
				LoggingPooler.doLog(logger, "DEBUG", this.loggingName, "processQueueData", false, false, false, messageId, 
						"telinUrl: " + telinUrl, null);

				List<NameValuePair> listParameter = composeParameter(messageId, accId, tokenId, vendorSenderId, msisdn.trim(), theSMS.trim());
				LoggingPooler.doLog(logger, "DEBUG", this.loggingName, "processQueueData", false, false, false, messageId, 
						"getParameter: " + listParameter.toString(), null);
				
				if(telinUrl.trim().length() > 0){
					//HashMap<String, String> mapHit = tool.hitHTTPSPostFormUrl(messageId, url, listParameter, null);
					HashMap<String, String> mapHit = tool.hitHTTPSPostFormUrl(messageId, telinUrl, listParameter, null);
					LoggingPooler.doLog(logger, "DEBUG", this.loggingName, "processQueueData", false, false, false, messageId, 
							"mapHit: " + tool.convertHashMapToJSON(mapHit).toString(), null);
					
					if (mapHit.get("hitStatus").trim().equals("SUCCESS")) {
						String bodyResponse = mapHit.get("hitResultBody").trim();
						String callSID = StringUtils.substringBetween(bodyResponse, "<Sid>", "</Sid>");
						String telinStatus = StringUtils.substringBetween(bodyResponse, "<Status>", "</Status>");
						String blastmeStatus = "002";
						
						// Save map messageId to callSid to redis
						saveMapMessageidSid(messageId, callSID);
						
						// Update delivery data in table transaction
						if (telinStatus.trim().equals("QUEUED")) {
							// PENDING
							blastmeStatus = "002";
						} else if (telinStatus.trim().toLowerCase().equals("initiated")) {
							blastmeStatus = "005";
						} else if (telinStatus.trim().toLowerCase().equals("ringing")) {
							blastmeStatus = "006";
						} else if (telinStatus.trim().toLowerCase().equals("answered")) {
							blastmeStatus = "000";
						} 
						
						if (!blastmeStatus.trim().equals("002")) {
							updateTransactionStatus(messageId, blastmeStatus);
						}					

						LoggingPooler.doLog(logger, "DEBUG", this.loggingName, "processQueueData", false, false, false, messageId, 
								"Success to hit TELIN. Return status " + blastmeStatus, null);
					} else {
						updateTransactionStatus(messageId, "900");
						
						LoggingPooler.doLog(logger, "DEBUG", this.loggingName, "processQueueData", false, false, false, messageId, 
								"Failed to hit TELIN. Network issue. Return status 900", null);
					}				
				} else {
					LoggingPooler.doLog(logger, "DEBUG", this.loggingName, "processQueueData", false, false, false, messageId, 
							"FAILED to hit TELIN CPAAS. URL for the API UserName is NOT DEFINED.", null);
				}
				
			} catch (Exception e) {
				e.printStackTrace();
				LoggingPooler.doLog(logger, "DEBUG", this.loggingName, "processQueueData", true, false, false, "", 
						"Failed to process the data from queue " + theQueueName + ". Error occured.", e);			
			}	
		}		

		@Override
		public void run() {
			processQueueData();
		}
	}	
	
	
}
