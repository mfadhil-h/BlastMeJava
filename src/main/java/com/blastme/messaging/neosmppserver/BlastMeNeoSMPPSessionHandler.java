package com.blastme.messaging.neosmppserver;

import java.lang.ref.WeakReference;
import java.util.Arrays;

import org.apache.logging.log4j.Logger;
import org.postgresql.util.Base64;

import com.blastme.messaging.toolpooler.ClientBalancePooler;
import com.blastme.messaging.toolpooler.ClientPropertyPooler;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.RedisPooler;
import com.blastme.messaging.toolpooler.SMPPEnquiryLinkPooler;
import com.blastme.messaging.toolpooler.SMSTransactionOperationPooler;
import com.blastme.messaging.toolpooler.Tool;
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
	private RabbitMQPooler rabbitMqPooler;
	private Connection rabbitMqConnection;
	private RedisPooler redisPooler;
	private RedisCommands<String, String> redisCommand;

	public BlastMeNeoSMPPSessionHandler(SmppSession session, String theBlastmeSessionId, String theRemoteIpAddress, String theClientId, Tool theTool, 
			SMPPEnquiryLinkPooler theSmppEnquiryLinkPooler, ClientPropertyPooler theClientPropertyPooler, 
			ClientBalancePooler theClientBalancePooler, SMSTransactionOperationPooler theSmsTransactionOperationPooler,
			RabbitMQPooler theRabbitMqPooler, Connection theRabbitMqConnection, RedisPooler theRedisPooler, Logger theLogger) {
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
    
   
    public PduResponse firePduRequestReceived(@SuppressWarnings("rawtypes") PduRequest pduRequest) {
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
							
	    			
	    			// Combine all shortMessage
	    			byte[] allSMSMessage = combineMessage(clientSenderId, msisdn, thisMessageId, thisTotalMessages, thisMessageNumber, shortMessage);
					
	    			if (allSMSMessage == null || allSMSMessage.length == 0) {
	    				// NULL, no complete yet the combination process
	    				// Do not process anything
	    			} else {
	    				// Complete the combination process
		                // Run thread smppIncomingTrxProcessor
	    				LoggingPooler.doLog(logger, "DEBUG", "BlastmeNeoSMPPSessionHandler", "firePduRequestReceived", false, false, true, messageId, 
		    					"Incoming sms is with UDH. The messageId: " + thisMessageId + ", thisTotalMessages: " + thisTotalMessages + ", thisMessageNumber: " +
		    					thisMessageNumber + ", thisSMS: " + theSMSPart, null);	
	    				
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
	                Address mtSourceAddress = mt.getSourceAddress();
	                String clientSenderId = mtSourceAddress.getAddress().trim();
	                		
	                Address mtDestinationAddress = mt.getDestAddress();
	                String msisdn = mtDestinationAddress.getAddress().trim();
	                
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
}
