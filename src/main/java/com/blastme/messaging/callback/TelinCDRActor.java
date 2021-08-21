package com.blastme.messaging.callback;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RedisPooler;
import com.blastme.messaging.toolpooler.Tool;

import io.lettuce.core.api.sync.RedisCommands;

public class TelinCDRActor implements Job {
	private static Logger logger;
	private Tool tool;
	
	private static RedisPooler redisPooler;
	private static RedisCommands<String, String> redisCommand;

	//private Connection connection = null;
	private PreparedStatement pstmtGetFailedCall = null;

	public TelinCDRActor() {
		System.out.println("INTIATING TELIN CDR ACTOR. Getting and processing the CDR.");
	}

	private String getCallSid(String messageId) {
		String callSid = "";
		
		try {
			// Map messageId to callSid
			String redisKey = "mapMsgIdToCallSid-" + messageId;
			callSid = redisPooler.redisGet(redisCommand, redisKey);
			
			LoggingPooler.doLog(logger, "DEBUG", "TelinCDRActor", "getCallSid", false, false, false, messageId, 
					"Successfully get callSid. messageId: " + messageId + " -> callSid: " + callSid, null);					
		} catch (Exception e) {
			e.printStackTrace();
			LoggingPooler.doLog(logger, "DEBUG", "TelinCDRActor", "getCallSid", true, false, false, messageId, 
					"Failed to get callSid by messageId. Error occured.", e);					
		}
		
		return callSid;
	}
	
	private String composeCDRURL(String accId, String tokenId, String callSid, String clientGroupId) {
		try {
			if (clientGroupId.trim().equals("TAT2007091435")) {
				return "https://ACd28d50e74cb76f2bd143044164fb09e7:4b7a5bc000682b815e3f96a5835ab7f8@jatis.neuapix.com/restcomm/2012-04-24/Accounts/ACd28d50e74cb76f2bd143044164fb09e7/Calls/" + callSid + ".json";
			} else {
				return "https://" + accId + ":" + tokenId + "@jatis.neuapix.com/restcomm/2012-04-24/Accounts/" + accId + "/Calls/" + URLEncoder.encode(callSid + ".json", StandardCharsets.UTF_8.toString());
			}		
		} catch (Exception e) {
			e.printStackTrace();
			return "";
		}
	}
	
	private void doCallCDR() {
		try {
			// Do sending - get messages to send
			ResultSet resultSet = pstmtGetFailedCall.executeQuery();
			
			while (resultSet.next()) {
				String messageId = resultSet.getString("message_id");
				String apiUserName = resultSet.getString("api_username");
				String clientGroupId = resultSet.getString("client_group_id");
				
				LoggingPooler.doLog(logger, "DEBUG", "TelinCDRActor", "doCallCDR", false, false, false, messageId, 
						"Processing messageId: " + messageId + ", apiUserName: " + apiUserName + ", clientGroupId: " + clientGroupId, null);					

				// Get callSid by messageId
				String callSid = getCallSid(messageId);
				LoggingPooler.doLog(logger, "DEBUG", "TelinCDRActor", "doCallCDR", false, false, false, messageId, 
						"Voice Call apiUserName: " + apiUserName + " -> callSid: " + callSid, null);					
				
				if (apiUserName != null && apiUserName.trim().length() > 0) {
					// Get accId and tokenId
					// As in TELINVOICE transceiver
	            	String redisKey = "telinvoicetransceiver-" + apiUserName;
	            	String redisVal = redisPooler.redisGet(redisCommand, redisKey);
					LoggingPooler.doLog(logger, "DEBUG", "TelinCDRActor", "doCallCDR", false, false, false, messageId, 
							"Voice Call apiUserName: " + apiUserName + " -> redis jsonAccToken: " + redisVal, null);					
	            	
	            	String accId = "";
	            	String tokenId = "";
	            	if (redisVal.trim().length() > 0) {
	                	JSONObject jsonAccToken = new JSONObject(redisVal);
	                	
	                	accId = jsonAccToken.getString("accId");
	                	tokenId = jsonAccToken.getString("tokenId");
	            	}
					LoggingPooler.doLog(logger, "DEBUG", "TelinCDRActor", "doCallCDR", false, false, false, messageId, 
							"Voice Call apiUserName: " + apiUserName + " -> accId: " + accId + ", tokenId: " + tokenId, null);					
					
					// Compose URL
	            	String cdrUrl = composeCDRURL(accId, tokenId, callSid, clientGroupId);
					LoggingPooler.doLog(logger, "DEBUG", "TelinCDRActor", "doCallCDR", false, false, false, messageId, 
							"CDR URL: " + cdrUrl, null);
					
					// Map Header
					JSONObject jsonHeader = new JSONObject();
					
					// Hit CDR TELIN
					HashMap<String, String> mapHit = tool.hitHTTPSGetGeneral(messageId, cdrUrl, "", jsonHeader);
					LoggingPooler.doLog(logger, "DEBUG", "TelinCDRActor", "doCallCDR", false, false, false, messageId, 
							"mapHit: " + tool.convertHashMapToJSON(mapHit).toString(), null);
					
					String bodyResponse = mapHit.get("hitResultBody");
					LoggingPooler.doLog(logger, "DEBUG", "TelinCDRActor", "doCallCDR", false, false, false, messageId, 
							"hitResultBody: " + bodyResponse, null);
//					{
//						  "sid": "ID35ad777a64ad460bae257c256c66c7d1-CAaef5224a942e4eb2badd1473f7fa8b99",
//						  "InstanceId": "ID35ad777a64ad460bae257c256c66c7d1",
//						  "date_created": "Tue, 3 Mar 2020 17:45:40 +0000",
//						  "date_updated": "Tue, 3 Mar 2020 17:46:45 +0000",
//						  "account_sid": "ACd60c3ce0c042d3588ff1597731ec4830",
//						  "to": "628111253636",
//						  "from": "622180602341",
//						  "status": "canceled",
//						  "start_time": "2020-03-03T17:45:40.000Z",
//						  "end_time": "2020-03-03T17:46:45.000Z",
//						  "duration": 0,
//						  "price_unit": "USD",
//						  "direction": "outbound-api",
//						  "api_version": "2012-04-24",
//						  "caller_name": "622180602341",
//						  "uri": "/2012-04-24/Accounts/ACd60c3ce0c042d3588ff1597731ec4830/Calls/ID35ad777a64ad460bae257c256c66c7d1-CAaef5224a942e4eb2badd1473f7fa8b99.json",
//						  "subresource_uris": {
//						    "notifications": "/2012-04-24/Accounts/ACd60c3ce0c042d3588ff1597731ec4830/Calls/ID35ad777a64ad460bae257c256c66c7d1-CAaef5224a942e4eb2badd1473f7fa8b99/Notifications.json",
//						    "recordings": "/2012-04-24/Accounts/ACd60c3ce0c042d3588ff1597731ec4830/Calls/ID35ad777a64ad460bae257c256c66c7d1-CAaef5224a942e4eb2badd1473f7fa8b99/Recordings.json"
//						  }
//					}
					
					if (bodyResponse.trim().length() > 0) {
						JSONObject jsonResponse = new JSONObject(bodyResponse);
						String callStatus = jsonResponse.getString("status");
						
						if (callStatus.trim().toLowerCase().equals("canceled")) {
							// NO ANSWER
						} else if (callStatus.trim().toLowerCase().equals("faield")) {
							// REJECTED
							
						} 
					}
				}


			}
		} catch (Exception e) {
			e.printStackTrace();
			LoggingPooler.doLog(logger, "DEBUG", "TelinCDRActor", "doCallCDR", true, false, false, "", 
					"Failed to process calling CDR. Error occured.", e);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		try {
			JobDataMap data = context.getJobDetail().getJobDataMap();
			
			logger = (Logger) data.get("logger");
			tool = (Tool) data.get("tool");

			redisPooler = (RedisPooler) data.get("redisPooler");
			redisCommand = (RedisCommands<String, String>) data.get("redisCommand");
			
			//connection = (Connection) data.get("jdbcConnection");
			pstmtGetFailedCall = (PreparedStatement) data.get("pstmtGetFailedCall");
			
			doCallCDR();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
