package com.blastme.messaging.neosmppserver;

import java.lang.ref.WeakReference;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.json.JSONObject;

import com.blastme.messaging.toolpooler.ClientBalancePooler;
import com.blastme.messaging.toolpooler.ClientPropertyPooler;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.RedisPooler;
import com.blastme.messaging.toolpooler.RouteSMSPooler;
import com.blastme.messaging.toolpooler.SMSTransactionOperationPooler;
import com.blastme.messaging.toolpooler.SenderIdSMSPooler;
import com.blastme.messaging.toolpooler.TelecomPrefixPooler;
import com.cloudhopper.commons.charset.Charset;
import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.commons.util.windowing.WindowFuture;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.pdu.DeliverSm;
import com.cloudhopper.smpp.pdu.DeliverSmResp;
import com.cloudhopper.smpp.pdu.PduRequest;
import com.cloudhopper.smpp.pdu.PduResponse;
import com.cloudhopper.smpp.type.Address;
import com.cloudhopper.smpp.util.DeliveryReceipt;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

import io.lettuce.core.api.sync.RedisCommands;

class BlastmeNeoSMPPIncomingTrxProcessor implements Runnable {
	private WeakReference<SmppSession> sessionRef;
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
	
	private static Logger logger;
	private static String SMSQueueName = "SMPP_INCOMING";
	
	// SUPPORTING CLASSES
	private ClientPropertyPooler clientPropertyPooler;
	private ClientBalancePooler clientBalancePooler;
	private SMSTransactionOperationPooler smsTransactionOperationPooler;
	private RabbitMQPooler rabbitMqPooler;
	private Connection rabbitMqConnection;
	private RedisPooler redisPooler;
	private RedisCommands<String, String> redisCommand;
	
	public BlastmeNeoSMPPIncomingTrxProcessor(String theMessageId, String theSystemId, String theRemoteIpAddress, String theClientSenderId, String theMsisdn, 
			byte[] theShortMessage, byte theDataCoding, String theClientId, Address theMtSourceAddress, Address theMtDestinationAddress, 
			ClientPropertyPooler theClientPropertyPooler, ClientBalancePooler theClientBalancePooler, SMSTransactionOperationPooler theSmsTransactionOperationPooler,
			RabbitMQPooler theRabbitMqPooler, Connection theRabbitMqConnection, RedisPooler theRedisPooler, WeakReference<SmppSession> theSessionRef, Logger theLogger) {
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
		
		this.sessionRef = theSessionRef;
		this.sessionId = theSessionRef.get().getConfiguration().getName();
		
		this.clientPropertyPooler = theClientPropertyPooler;
		this.clientBalancePooler = theClientBalancePooler;
		this.smsTransactionOperationPooler = theSmsTransactionOperationPooler;
		this.rabbitMqPooler = theRabbitMqPooler;
		this.rabbitMqConnection = theRabbitMqConnection;
		this.redisPooler = theRedisPooler;
		redisCommand = redisPooler.redisInitiateConnection();
		
		logger = theLogger;
		
		Charset theCharset = getCharsetByDataCoding(dataCoding);
		
//		String messageEncoding = "GSM";
//		if (theCharset == CharsetUtil.CHARSET_GSM) {
//			messageEncoding = "GSM";
//		} else if (theCharset == CharsetUtil.CHARSET_GSM7) {
//			messageEncoding = "GSM";
//		} else if (theCharset == CharsetUtil.CHARSET_UTF_8) {
//			messageEncoding = "UCS2";
//		} else {
//			messageEncoding = "UCS2";
//		}
		
		// Get the shortMessage
		this.shortMessage = CharsetUtil.decode(bShortMessage, theCharset);
	
		LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPIncomingTrxProcessor", "BlastMeNeoSMPPIncomingTrxProcessor", false, false, false, messageId, 
				"Session ID: " + sessionId + ". Incoming trx - messageId: " + this.messageId + ", clientSenderId: " + this.clientSenderId + ", msisdn: " + this.msisdn + 
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
//				case (byte) 0x01:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//									break;
//				case (byte) 0x02:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//									break;
//				case (byte) 0x03:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//									break;
				case (byte) 0x04:	theCharSet = CharsetUtil.CHARSET_UCS_2;
									break;
//				case (byte) 0x05:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//									break;
				case (byte) 0x06:	theCharSet = CharsetUtil.CHARSET_UCS_2;
									break;
				case (byte) 0x07:	theCharSet = CharsetUtil.CHARSET_UCS_2;
									break;
				case (byte) 0x08:	theCharSet = CharsetUtil.CHARSET_UCS_2;
									break;
//				case (byte) 0x09:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//									break;
//				case (byte) 0x0A:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//									break;
//				case (byte) 0x0B:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//									break;
//				case (byte) 0x0C:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//									break;
				case (byte) 0x0D:	theCharSet = CharsetUtil.CHARSET_UCS_2;
									break;
//				case (byte) 0x0E:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//									break;
//				case (byte) 0x0F:	theCharSet = CharsetUtil.CHARSET_UCS_2;
//									break;
				default:			theCharSet = CharsetUtil.CHARSET_GSM;
									break;
			}
			
			LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPIncomingTrxProcessor", "getCharsetByDataCoding", false, false, false, messageId, 
					"sessionId: " + sessionId + " - dataCoding: " + dataCoding + " -> charset: " + theCharSet, null);				
		} catch (Exception e) {
			e.printStackTrace();
			
			LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPIncomingTrxProcessor", "getCharsetByDataCoding", true, false, false, messageId, 
					"sessionId: " + sessionId + " - Failed to get charset of datacoding: " + dataCoding + ". Error occured.", e);				
			
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
			jsonRedis.put("receiverType", receiverType); // SMPP and HTTP only
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
				return (int) Math.ceil((double) message.length() / (double) 153);
			}
		} else if(encoding.equals("UCS2")) {
			if(message.length() <= 70){
				return 1;
			} else {
				return (int) Math.ceil((double) message.length() / (double) 67);
			}
		} else if(encoding.equals("WHATSAPP")) {
			return 1;
		} else {
			return (int) Math.ceil((double) message.length() / (double) 67);
		}
	}
	
	@SuppressWarnings("rawtypes")
	private void sendRequestPdu(SmppSession session, DeliverSm deliver) {
        try {
            WindowFuture<Integer,PduRequest,PduResponse> future = session.sendRequestPdu(deliver, 10000, false);
            if (!future.await()) {
                LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPIncomingTrxProcessor", "sendRequestPdu", false, false, true, messageId, 
						"Failed to receive deliver_sm_resp within specified time.", null);
            } else if (future.isSuccess()) {
               DeliverSmResp deliverSmResp = (DeliverSmResp)future.getResponse();
              
               LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPIncomingTrxProcessor", "sendRequestPdu", false, false, true, messageId, 
                		"deliver_sm_resp: commandStatus [" + deliverSmResp.getCommandStatus() + "=" + deliverSmResp.getResultMessage() + "]", null);
            } else {
                LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPIncomingTrxProcessor", "sendRequestPdu", false, false, true, messageId, 
                		"Failed to properly receive deliver_sm_resp: " + future.getCause(), null);
            }
        } catch (Exception e) {
        	e.printStackTrace();
        	
        	LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPIncomingTrxProcessor", "sendRequestPdu", true, false, true, messageId, 
            		"Failed to send PDU to client. Error occurs.", e);
        }
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
	
	private void doProcessTheSMS() {
		System.out.println("PROCESSING msgId " + this.messageId);
		
		String errorCode = "002"; // DEFAULT errCode
		String deliveryStatus = "ACCEPTED";
		byte deliveryState = SmppConstants.STATE_ACCEPTED;
		byte esmeErrCode = SmppConstants.STATUS_OK;
		
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
						LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPIncomingTrxProcessor", "doProcessTheSMS", false, false, true, messageId, 
								"Session ID: " + sessionId + ". Business Mode: " + businessModel, null);
						
						// routeId = clientSenderIdId + "-" + telecomId
						String routeId = clientSenderId + "-" + this.clientId + "-" + this.systemId + "-" + telecomId;
						JSONObject jsonRoute = RouteSMSPooler.jsonRouteSMSProperty.getJSONObject(routeId);
						
						// Get client price from jsonRoute
						clientPricePerSubmit = jsonRoute.getDouble("clientPricePerSubmit");

						boolean isBalanceEnough = false; // DEFAULT IS FALSE - TO NOT SENDING WHEN ERROR HAPPENS.
						if(businessModel.equals("PREPAID")){
							// Real balance deduction is in ROUTER. NOT IN SMPP FRONT END.
							double divisionBalance = clientBalancePooler.getClientBalance(clientId);

							if(divisionBalance > clientPricePerSubmit){
								isBalanceEnough = true;
							} else {
								isBalanceEnough = false;
							}
							
							LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPIncomingTrxProcessor", "doProcessTheSMS", false, false, true, messageId, 
									"Session ID: " + sessionId + ". Checking initial balance - divisionBalance: " + divisionBalance, null);
						} else {
							isBalanceEnough = true; // POSTPAID.
						}
						
						// If balance is enough (will always be enough for postpaid)
						if (isBalanceEnough == true) {
							// BALANCE IS ENOUGH
							errorCode = "002";
							deliveryStatus = "ACCEPTED";
							deliveryState = SmppConstants.STATE_ACCEPTED;
							esmeErrCode = SmppConstants.STATUS_OK;
							
    						LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPIncomingTrxProcessor", "doProcessTheSMS", false, false, true, messageId, 
    								"Session ID: " + sessionId + ". BALANCE ENOUGH, MESSAGE IS ACCEPTED. errCode: " + errorCode + ", esmeErrCode: STATUS OK.", null);
    						// Save initial data and send to SMPP_INCOMING queue to be processed by router
    						
						} else {
							// BALANCE IS NOT ENOUGH
							errorCode = "122";								// BALANCE NOT ENOUGH
							deliveryStatus = "REJECTED";
							deliveryState = SmppConstants.STATE_REJECTED;
							esmeErrCode = SmppConstants.STATUS_SUBMITFAIL;
							
    						LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPIncomingTrxProcessor", "doProcessTheSMS", false, false, true, messageId, 
    								"Session ID: " + sessionId + ". BALANCE NOT ENOUGH. errCode: " + errorCode + ", esmeErrCode: SUBMITE FAILED.", null);
						}
					} else {
						// ROUTE IS NOT DEFINED
						errorCode = "900";									// ROUTE NOT DEFINED
						deliveryStatus = "REJECTED";
						deliveryState = SmppConstants.STATE_REJECTED;
						esmeErrCode = SmppConstants.STATUS_SUBMITFAIL;		
						
						LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPIncomingTrxProcessor", "doProcessTheSMS", false, false, true, messageId, 
								"Session ID: " + sessionId + ". ROUTE IS NOT DEFINED. errCode: " + errorCode + ", esmeErrCode: SUBMIT FAILED.", null);
					}
				} else {
					// Prefix is not valid
					errorCode = "113";									// UNREGISTERED PREFIX
					deliveryStatus = "REJECTED";
					deliveryState = SmppConstants.STATE_REJECTED;
					esmeErrCode = SmppConstants.STATUS_INVNUMDESTS;		// INVALID NUMBER DESTINATION
					
					LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPIncomingTrxProcessor", "doProcessTheSMS", false, false, true, messageId, 
							"Session ID: " + sessionId + ". PREFIX IS NOT DEFINED. errCode: " + errorCode + ", esmeErrCode: INVALID NUMBER DESTINATION.", null);
				}
			} else {
				// clientSenderId is NOT VALID
				errorCode = "121";										// INVALID SENDERID															
				deliveryStatus = "REJECTED";
				deliveryState = SmppConstants.STATE_REJECTED;
				esmeErrCode = SmppConstants.STATUS_INVSRCADR;
				
				LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPIncomingTrxProcessor", "doProcessTheSMS", false, false, true, messageId, 
						"Session ID: " + sessionId + ". SENDERID IS NOT DEFINED. errCode: " + errorCode + ", esmeErrCode: INVALID SOURCE ADDRESS.", null);
			}

			/* Check Vendor Whatsapp to change ENCODING sms count change to 1 */
			String [] vendorWhatsapp = {"ARAN20230314", "ARTP20230319", "ARTP20230126", "ARTP20230207",
					"NATH20230316", "PAIA20220704", "PATP20220704", "PAXT20220704", "SHST20230214", "WAFE21062321",
					"WATI20220701", "AR0220230329"};
			LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPIncomingTrxProcessor", "doProcessTheSMS",
					false, false, true, messageId,
							this.systemId + " - routingId: " + clientSenderId + "-" +
							clientId  + "-" + this.systemId + "-" + telecomId, null);

			String vendorId = RouteSMSPooler.getRoutedVendorId(messageId,
					clientSenderId + "-" + clientId  + "-" + this.systemId, telecomId.trim());
			LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPIncomingTrxProcessor", "doProcessTheSMS",
					false, false, true, messageId,
							this.systemId + " - vendorId: " + vendorId, null);

			boolean found = Arrays.asList(vendorWhatsapp).contains(vendorId);
			LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPIncomingTrxProcessor", "doProcessTheSMS",
					false, false, true, messageId,
							this.systemId + " - found: " + String.valueOf(found), null);

			if (found) {
				messageEncoding = "WHATSAPP";
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
			String receiverType = "SMPP";
			saveInitialData(this.messageId, incomingDateTime, this.sessionId, receiverData, 
					receiverResponse, this.remoteIpAddress, responseDateTime, incomingDateTime, msisdn, shortMessage, countryCode, 
					prefix, telecomId, errorCode, receiverType, clientSenderIdId, clientSenderId, clientId, this.systemId, 
					clientPricePerSubmit, clientCurrency, messageEncoding, messageLength, smsCount, deliveryStatus);
			
			// Process sending DLR - ONLY Failed one. ACCEPTED one will get DR after processed in ROUTER
			if (!errorCode.trim().startsWith("00")) {
				int submitCount = 0;
				if (errorCode.trim().startsWith("00")) {
					submitCount = 1;
				}
				DeliveryReceipt dlrReceipt = new DeliveryReceipt(messageId, submitCount, 0, new DateTime(), new DateTime(), deliveryState, esmeErrCode, shortMessage);
				
				Address moSourceAddress = this.mtDestinationAddress;
				Address moDestinationAddress = this.mtSourceAddress;
				
				sendDeliveryReceipt(sessionRef.get(), moSourceAddress, moDestinationAddress, dlrReceipt.toShortMessage().getBytes(), this.dataCoding);
				LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPIncomingTrxProcessor", "doProcessTheSMS - " + this.sessionId, false, false, false, messageId, 
						"Sending DLR with session: " + sessionRef.get().getConfiguration().getName() + ". DLR: " + dlrReceipt.toShortMessage(), null);	
			} else {
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
				jsonIncoming.put("receiverType", receiverType); // SMPP and HTTP only
				jsonIncoming.put("smsChannel", receiverType);
				jsonIncoming.put("sysSessionId", this.sessionId);
            	jsonIncoming.put("messageLength", messageLength);
            	jsonIncoming.put("messageCount", smsCount);
            	jsonIncoming.put("clientPricePerSubmit", clientPricePerSubmit);

				LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPIncomingTrxProcessor", "doProcessTheSMS - " + this.sessionId, false, false, false, messageId, 
						"jsonIncoming: " + jsonIncoming.toString(), null);				

				Channel channel = rabbitMqPooler.getChannel(rabbitMqConnection);
				
				channel.queueDeclare(SMSQueueName, true, false, false, null);
				channel.basicPublish("", SMSQueueName, MessageProperties.PERSISTENT_TEXT_PLAIN, jsonIncoming.toString().getBytes());
				LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPIncomingTrxProcessor", "doProcessTheSMS - " + this.sessionId, false, false, false, messageId, 
						"jsonIncoming: " + jsonIncoming.toString() + " published SUCCESSfully!", null);
				
				channel.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			
			LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPIncomingTrxProcessor", "doProcessTheSMS - " + this.sessionId, true, false, false, messageId, 
					"Failed to process incoming sms message. Error occured.", e);
		}
	}
	
	@Override
	public void run() {
		LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPIncomingTrxProcessor", "RUN", false, false, false, messageId, 
				"Session ID: " + sessionId + ". Processing the SMS.", null);
		
		doProcessTheSMS();    			
	}
	
}

