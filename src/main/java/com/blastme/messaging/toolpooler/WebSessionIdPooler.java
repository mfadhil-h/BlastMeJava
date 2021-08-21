package com.blastme.messaging.toolpooler;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.json.JSONObject;

import com.blastme.messaging.configuration.Configuration;

import io.lettuce.core.api.sync.RedisCommands;

public class WebSessionIdPooler {
	private static Logger logger;
	private RedisPooler redisPooler;
	RedisCommands<String, String> syncCommands;

	public WebSessionIdPooler() {
		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("POOLER");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate RedisPooler
		redisPooler = new RedisPooler();
		syncCommands = redisPooler.redisInitiateConnection();
		
		LoggingPooler.doLog(logger, "INFO", "WebSessionIdPooler", "WebSessionIdPooler", false, false, false, "", 
				"Module WebSessionIdPooler is initiated and ready to serve.", null);				
	}

	public void setSessionId(String sessionId, String userName, String clientId, String clientName, String privilege){
		String redisKey = "sess-" + sessionId;
		JSONObject jsonRedis = new JSONObject();
		jsonRedis.put("username", userName);
		jsonRedis.put("clientId", clientId);
		jsonRedis.put("clientName", clientName);
		jsonRedis.put("privilege", privilege);
		
		String redisVal = jsonRedis.toString();
		
		redisPooler.redisSetWithExpiry(syncCommands, redisKey, redisVal, 24 * 60 * 60);
	}
	
	public JSONObject getSessionData(String sessionId){		
		System.out.println("getSessionData - sessionId: " + sessionId);
		String redisKey = "sess-" + sessionId;
		String redisVal = redisPooler.redisGet(syncCommands, redisKey);
		
		if(redisVal == null || redisVal.trim().length() == 0){
			LoggingPooler.doLog(logger, "INFO", "WebSessionIdPooler", "getSessionData", false, false, false, "", 
					"Session " + sessionId + " is expired.", null);		

			return null;
		} else {
			try{
				JSONObject jsonData = new JSONObject(redisVal);

				LoggingPooler.doLog(logger, "INFO", "WebSessionIdPooler", "getSessionData", false, false, false, "", 
						"Session " + sessionId + " -> " + jsonData.toString(), null);		

				return jsonData;
			} catch(Exception e) {
				LoggingPooler.doLog(logger, "INFO", "WebSessionIdPooler", "getSessionData", true, false, false, "", 
						"Failed to read redis for sessionId " + sessionId + ". Error occured.", e);		
				
				return null;
			}
		}			
	}
}
