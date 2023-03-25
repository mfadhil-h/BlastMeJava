package com.blastme.messaging.neosmppserver;

import java.lang.ref.WeakReference;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import com.blastme.messaging.toolpooler.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.postgresql.util.Base64;

import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.BindTransceiverResp;
import com.cloudhopper.smpp.pdu.EnquireLink;
import com.cloudhopper.smpp.pdu.PduRequest;
import com.cloudhopper.smpp.pdu.PduResponse;
import com.cloudhopper.smpp.pdu.SubmitSm;
import com.rabbitmq.client.Connection;

import io.lettuce.core.api.sync.RedisCommands;

import com.cloudhopper.commons.charset.Charset;
import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.commons.gsm.GsmUtil;
import com.cloudhopper.smpp.pdu.SubmitSmResp;
import com.cloudhopper.smpp.tlv.Tlv;
import com.cloudhopper.smpp.type.Address;
import com.cloudhopper.smpp.util.SmppUtil;

public class BlastMeNeoSMPPSessionHandler extends DefaultSmppSessionHandler {
	private WeakReference<SmppSession> sessionRef;
	private static Logger logger;
	
	private String systemId;
	private String remoteIpAddress;
	private String clientId;
	private String blastmeSessionId;

	// Supporting class
	private Tool tool;
	private SMPPEnquiryLinkPooler smppEnquiryLinkPooler;
	private ClientPropertyPooler clientPropertyPooler;
	private ClientBalancePooler clientBalancePooler;
	private SMSTransactionOperationPooler smsTransactionOperationPooler;
	private RouteSMSPooler routeSMSPooler;
	private RabbitMQPooler rabbitMqPooler;
	private Connection rabbitMqConnection;
	private RedisPooler redisPooler;
	private RedisCommands<String, String> redisCommand;
	private final Gson gson =  new GsonBuilder().serializeSpecialFloatingPointValues().serializeNulls().create();

	public BlastMeNeoSMPPSessionHandler(SmppSession session, String theBlastmeSessionId, String theRemoteIpAddress,
			String theClientId, Tool theTool, SMPPEnquiryLinkPooler theSmppEnquiryLinkPooler,
			ClientPropertyPooler theClientPropertyPooler, ClientBalancePooler theClientBalancePooler,
			SMSTransactionOperationPooler theSmsTransactionOperationPooler, RouteSMSPooler routeSMSPooler,
			RabbitMQPooler theRabbitMqPooler, Connection theRabbitMqConnection, RedisPooler theRedisPooler,
			Logger theLogger) {
		this.sessionRef = new WeakReference<SmppSession>(session); 
		logger = theLogger;
		
		this.systemId = session.getConfiguration().getSystemId();
		this.remoteIpAddress = theRemoteIpAddress;
		this.clientId = theClientId;
		this.blastmeSessionId = theBlastmeSessionId;

		this.tool = theTool;
		this.smppEnquiryLinkPooler = theSmppEnquiryLinkPooler;
		this.clientPropertyPooler = theClientPropertyPooler;
		this.clientBalancePooler = theClientBalancePooler;
		this.smsTransactionOperationPooler = theSmsTransactionOperationPooler;
		this.routeSMSPooler = routeSMSPooler;
		this.rabbitMqPooler = theRabbitMqPooler;
		this.rabbitMqConnection = theRabbitMqConnection;
		this.redisPooler = theRedisPooler;
		redisCommand = redisPooler.redisInitiateConnection();
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
			
			LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPSessionHandler", "getCharsetByDataCoding - " + this.blastmeSessionId, false, false, false, "", 
					"dataCoding: " + dataCoding + " -> charset: " + theCharSet, null);				
		} catch (Exception e) {
			e.printStackTrace();
			
			LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPSessionHandler", "getCharsetByDataCoding - " + this.blastmeSessionId, true, false, false, "", 
					"Failed to get charset of datacoding: " + dataCoding + ". Error occurs.", e);				
			
		}
		
		return theCharSet;
	}
	
    private HashMap<String, String> combineMessage(String origin, String destination, String messageId, int byteId, int totalMessageCount, int currentCount, byte[] shortMessagePart) {
    	byte[] combinedMessage = new byte[] {};
		int currentIndex = 0;
		HashMap<String, String> result = new HashMap<String, String>();

		if (currentCount > 1) {
			String redisKeyIn = "multipartsms-" + origin + "-" + destination + "-" + byteId + "-" + totalMessageCount + "-" + (currentCount-1);

			String jsonRedisValCombine = redisPooler.redisGet(redisCommand, redisKeyIn);
			System.out.println("getting first messageID redisKeyIn: " + redisKeyIn + ". jsonRedisValCombine: " + jsonRedisValCombine);
			Type type = new TypeToken<HashMap<String, String>>(){}.getType();
			HashMap<String, String> mapRedisValCombine = gson.fromJson(jsonRedisValCombine, type);
			messageId =  mapRedisValCombine.get("first_message_id");
			System.out.println("getting first messageID messageId: " + messageId + ".");
		}
    	
    	int expirySeconds = 24 * 60 * 60;
    	
    	// Convert byte[] to string using base64 to make sure the consistency
    	String encodedByte = Base64.encodeBytes(shortMessagePart);
    	
    	// Tulis ke redis
    	String redisKey = "multipartsms-" + origin + "-" + destination + "-" + byteId + "-" + totalMessageCount + "-" + currentCount;
//    	String redisVal = encodedByte;

		HashMap<String, String> mapRedisVal = new HashMap<String, String>();
		mapRedisVal.put("encoded_byte", encodedByte);
		mapRedisVal.put("first_message_id", messageId);

		String jsonRedisVal = gson.toJson(mapRedisVal);

    	redisPooler.redisSetWithExpiry(redisCommand, redisKey, jsonRedisVal, expirySeconds);

    	// Check if all messages are already in redis
//    	boolean isComplete = false;
    	boolean isComplete = (totalMessageCount==currentCount);
    	boolean wasExist = false;
    	
    	System.out.println("isComplete: " + isComplete);
//    	for (int x = 1; x <= totalMessageCount; x++) {
//    		System.out.println("x: " + x);
//			currentIndex = x;
//
//        	String redisKeyIn = "multipartsms-" + origin + "-" + destination + "-" + byteId + "-" + totalMessageCount + "-" + x;
//        	String redisValIn = redisPooler.redisGet(redisCommand, redisKeyIn);
//
//        	if (redisValIn == null || redisValIn.equals("")) {
//            	System.out.println(x + ".redisKeyIn: " + redisKeyIn + ", redisValIn IS NULL, add next and DO BREAK!");
//
//
//				String redisKey = "multipartsms-" + origin + "-" + destination + "-" + byteId + "-" + totalMessageCount + "-" + x;
////				String redisVal = encodedByte;
//
//				HashMap<String, String> mapRedisVal = new HashMap<String, String>();
//				mapRedisVal.put("encoded_byte", encodedByte);
//				mapRedisVal.put("first_message_id", messageId);
//
//				String jsonRedisVal = gson.toJson(mapRedisVal);
//
//				redisPooler.redisSetWithExpiry(redisCommand, redisKey, jsonRedisVal, expirySeconds);
//
//				isComplete = x == totalMessageCount && wasExist;
//            	break;
//        	} else {
//            	System.out.println(x + ". redisKeyIn: " + redisKeyIn + ", redisValIn: " + redisValIn);
//
//				// Yang exist sebelumnya harus true, dan x == totalMessage dan redisValIn bukan NILL
//            	if (x == totalMessageCount && wasExist) {
//            		isComplete = true;
//            	}
//            	wasExist = true;
//
////				redisKeyIn = "multipartsms-" + origin + "-" + destination + "-" + byteId + "-" + totalMessageCount + "-" + x;
////
//				String jsonRedisValCombine = redisPooler.redisGet(redisCommand, redisKeyIn);
//				System.out.println("getting first messageID redisKeyIn: " + redisKeyIn + ". jsonRedisValCombine: " + jsonRedisValCombine);
//				Type type = new TypeToken<HashMap<String, String>>(){}.getType();
//				HashMap<String, String> mapRedisValCombine = gson.fromJson(jsonRedisValCombine, type);
//				messageId =  mapRedisValCombine.get("first_message_id");
//				System.out.println("getting first messageID messageId: " + messageId + ".");
//
//            	System.out.println(x + ". wasExist: " + wasExist + ", isComplete: " + isComplete);
//        	}
//    	}
    	
    	// If complete, join the message
    	System.out.println("Checking final ISCOMPLETE: " + isComplete);
    	if (isComplete) {
        	for (int x = 1; x <= totalMessageCount; x++) {
            	String redisKeyIn = "multipartsms-" + origin + "-" + destination + "-" + byteId + "-" + totalMessageCount + "-" + x;

            	String jsonRedisValCombine = redisPooler.redisGet(redisCommand, redisKeyIn);
            	System.out.println(x + ". redisKeyIn: " + redisKeyIn + ". jsonRedisValCombine: " + jsonRedisValCombine);
				Type type = new TypeToken<HashMap<String, String>>(){}.getType();
				HashMap<String, String> mapRedisValCombine = gson.fromJson(jsonRedisValCombine, type);
				String encodedByteRedis = mapRedisValCombine.get("encoded_byte");
            	byte[] redisValByte = Base64.decode(encodedByteRedis);
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
			encodedByte = Base64.encodeBytes(combinedMessage);
    	} else {
    		// Do nothing
    	}
		result.put("encoded_byte", encodedByte);
//		result.put("current_index", String.valueOf(currentIndex));
		result.put("first_message_id", messageId);
    	
    	return result;
    }
    
   
    public PduResponse firePduRequestReceived(@SuppressWarnings("rawtypes") PduRequest pduRequest) {
        SmppSession session = sessionRef.get();

		LoggingPooler.doLog(logger, "DEBUG", "BlastmeNeoSMPPSessionHandler", "firePduRequestReceived",
				false, false, true, "", this.blastmeSessionId + " - pduRequest: " + pduRequest.toString(), null);
		LoggingPooler.doLog(logger, "DEBUG", "BlastmeNeoSMPPSessionHandler", "firePduRequestReceived",
				false, false, true, "", this.blastmeSessionId + " - json pduRequest: " + gson.toJson(pduRequest), null);

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
		} else if (pduRequest.getCommandId() == SmppConstants.CMD_ID_BIND_TRANSCEIVER) {
			BindTransceiverResp resp = new BindTransceiverResp();
			resp.setSequenceNumber(pduRequest.getSequenceNumber());
			response = resp;
			
			LoggingPooler.doLog(logger, "DEBUG", "BlastmeNeoSMPPSessionHandler", "firePduRequestReceived", false, false, true, "", 
					this.blastmeSessionId + " - " + session.getConfiguration().getSystemId() + " - PDU Request: CMD_ID_BIND_TRANSCEIVER. Responded!", null);
			
		} else if (pduRequest.getCommandId() == SmppConstants.CMD_ID_SUBMIT_SM) {
			try {
				LoggingPooler.doLog(logger, "DEBUG", "BlastmeNeoSMPPSessionHandler", "firePduRequestReceived", false, false, true, "", 
						this.blastmeSessionId + " - " + session.getConfiguration().getSystemId() + " - PDU Request: CMD_ID_SUBMIT_SMS.", null);

				LoggingPooler.doLog(logger, "DEBUG", "BlastmeNeoSMPPSessionHandler", "firePduRequestReceived",
						false, false, true, "", this.blastmeSessionId + " - json pduRequest: " + gson.toJson(pduRequest), null);
				
				this.smppEnquiryLinkPooler.logEnquiryLink(clientId, this.blastmeSessionId, "SUBMIT_SM");

                // Create messageId
                String messageId = tool.generateUniqueID();
                LoggingPooler.doLog(logger, "INFO", "BlastmeNeoSMPPSessionHandler", "firePduRequestReceived", false, false, true, "", 
						this.blastmeSessionId + " - " + session.getConfiguration().getSystemId() + " - Assigning messageId: " + messageId, null);
                
				// Check if message with UDH
				//DeliverSm moMessage = (DeliverSm) pduRequest;
                SubmitSm mt = (SubmitSm) pduRequest;
				
				boolean isUdh = SmppUtil.isUserDataHeaderIndicatorEnabled(mt.getEsmClass());
				LoggingPooler.doLog(logger, "DEBUG", "BlastmeNeoSMPPSessionHandler", "firePduRequestReceived", false, false, true, "", 
						this.blastmeSessionId + " - " + session.getConfiguration().getSystemId() + " - isUDH (multipart sms): " + isUdh, null);

				Address mtSourceAddress = mt.getSourceAddress();
				String clientSenderId = mtSourceAddress.getAddress().trim();
				Address mtDestinationAddress = mt.getDestAddress();
				String msisdn = mtDestinationAddress.getAddress().trim();

				if (isUdh) {
					// Handle multipart sms

					byte[] userDataHeader = GsmUtil.getShortMessageUserDataHeader(mt.getShortMessage());
//					new String(bytes, StandardCharsets.UTF_16BE);
//					Charset.forName()
					// lets assume the latest value is current Index SMPP Multipart
					// second after latest value is max Index SMPP Multipart
					int[] headerEncode = new int[userDataHeader.length];
					int j=0;
					for (int i = (userDataHeader.length-1); i >= 0 ; i--) {
						LoggingPooler.doLog(logger, "DEBUG", "BlastmeNeoSMPPSessionHandler", "firePduRequestReceived", false, false, true, "",
								this.blastmeSessionId + " - " + session.getConfiguration().getSystemId() +
										" - index: " + i +
										" - real value: " + (userDataHeader[i]) +
										" - value encode: " + (userDataHeader[i] & 0xff), null);
						headerEncode[j] = userDataHeader[i] & 0xff;
						j++;
					}

					int byteId = headerEncode[2] & 0xff;
//					int thisTotalMessages = headerEncode[2] & 0xff; //Old assumption
//					int maxMessageCount = headerEncode[1] & 0xff;
					int maxMessageCount = headerEncode[1] & 0xff;
					int currentMessageCount = headerEncode[0] & 0xff;
//					int currentMessageCount = userDataHeader[6] & 0xff;
					LoggingPooler.doLog(logger, "DEBUG", "BlastmeNeoSMPPSessionHandler", "firePduRequestReceived", false, false, true, "",
							this.blastmeSessionId + " - " + session.getConfiguration().getSystemId() +
//									" - currentMessageCount: " + currentMessageCount+
									" - byteId: " + byteId+" - maxMessageCount: " + maxMessageCount+" - currentMessageCount: " + currentMessageCount, null);
					
	                byte dataCoding = mt.getDataCoding();
					byte[] shortMessage = GsmUtil.getShortMessageUserData(mt.getShortMessage());
					
					
	    			Charset theCharset = getCharsetByDataCoding(dataCoding);
	    			
//	    			String messageEncoding = "GSM";
//	    			if (theCharset == CharsetUtil.CHARSET_GSM) {
//	    				messageEncoding = "GSM";
//	    			} else if (theCharset == CharsetUtil.CHARSET_GSM7) {
//	    				messageEncoding = "GSM";
//	    			} else if (theCharset == CharsetUtil.CHARSET_UTF_8) {
//	    				messageEncoding = "UCS2";
//	    			} else {
//	    				messageEncoding = "UCS2";
//	    			}
	    			
	    			String theSMSPart = CharsetUtil.decode(shortMessage, theCharset);						
					LoggingPooler.doLog(logger, "DEBUG", "BlastmeNeoSMPPSessionHandler", "firePduRequestReceived", false, false, true, "",
							this.blastmeSessionId + " - " + session.getConfiguration().getSystemId() + " - theSMSPart: " + theSMSPart+ " - theSMSPart length: " + theSMSPart.length(), null);

	    			
	    			// Combine all shortMessage
					HashMap<String, String> result = combineMessage(clientSenderId, msisdn, messageId, byteId, maxMessageCount, currentMessageCount, shortMessage);
	    			byte[] allSMSMessage = Base64.decode(result.get("encoded_byte"));
					messageId = result.get("first_message_id");
//					String strCurrentIndex = result.get("current_index");
//					int currentMessageIndex = Integer.parseInt(strCurrentIndex);

//	    			if (maxMessageCount == currentMessageIndex && allSMSMessage.length != 0) {
	    			if (maxMessageCount == currentMessageCount && allSMSMessage.length != 0) {
//	    				// NULL, no complete yet the combination process
//	    				// Do not process anything
//	    			} else {
	    				// Complete the combination process
		                // Run thread smppIncomingTrxProcessor
	    				LoggingPooler.doLog(logger, "DEBUG", "BlastmeNeoSMPPSessionHandler", "firePduRequestReceived", false, false, true, messageId, 
		    					"Incoming sms is with UDH. The byteId: " + byteId + ", maxMessageCount: " + maxMessageCount + ", currentMessageCount: " +
										currentMessageCount + ", thisSMS: " + theSMSPart, null);
	    				
		                Thread incomingTrxProcessor = new Thread(new BlastmeNeoSMPPIncomingTrxProcessor(messageId, systemId, remoteIpAddress, clientSenderId, msisdn, allSMSMessage, 
		                		dataCoding, clientId, mtSourceAddress, mtDestinationAddress, clientPropertyPooler, clientBalancePooler, smsTransactionOperationPooler, rabbitMqPooler, 
		                		rabbitMqConnection, redisPooler, sessionRef, logger));
		                incomingTrxProcessor.start();

					}
					// Send submitresponse
					SubmitSmResp submitSmResp = mt.createResponse();
					submitSmResp.setMessageId(messageId);
					response = submitSmResp;
				} else {

					byte dataCoding = mt.getDataCoding();
	                byte[] shortMessage = mt.getShortMessage();

	                if (shortMessage.length == 0) {
	                	LoggingPooler.doLog(logger, "INFO", "BlastmeNeoSMPPSessionHandler", "firePduRequestReceived", false, false, true, messageId, 
		    					"Empty short message content. Checking the TLV.", null);	
	                	
		                // Get TLV message_payload
		                Tlv theTLV = mt.getOptionalParameter(Short.decode("0x0424"));
		                byte[] tlvContent = theTLV.getValue();
		                String strTlvContent = CharsetUtil.decode(tlvContent, "GSM");
		                LoggingPooler.doLog(logger, "INFO", "BlastmeNeoSMPPSessionHandler", "firePduRequestReceived", false, false, true, messageId, 
		    					"The TLV Content: " + strTlvContent, null);
		                
	                	// If shortMessage is not defined, use tlvContent
	                	shortMessage = tlvContent;
	                }
	                
	                // Run thread smppIncomingTrxProcessor
	                Thread incomingTrxProcessor = new Thread(new BlastmeNeoSMPPIncomingTrxProcessor(messageId, systemId, remoteIpAddress, clientSenderId, msisdn, shortMessage, 
	                		dataCoding, clientId, mtSourceAddress, mtDestinationAddress, clientPropertyPooler, clientBalancePooler, smsTransactionOperationPooler, rabbitMqPooler, 
	                		rabbitMqConnection, redisPooler, sessionRef, logger));
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
				
				LoggingPooler.doLog(logger, "DEBUG", "BlastmeNeoSMPPSessionHandler", "firePduRequestReceived", true, false, true, "", 
    					"Failed handling SUBMIT_SMS. Error occurs.", e);
			}
        }

        return response;
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
}
