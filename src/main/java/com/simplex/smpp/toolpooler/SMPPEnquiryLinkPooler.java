package com.simplex.smpp.toolpooler;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import com.simplex.smpp.configuration.Configuration;

import io.lettuce.core.api.sync.RedisCommands;

public class SMPPEnquiryLinkPooler {
	private static Logger logger;
		
	private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMddHHmmssSSS");
	
	private final RedisPooler redisPooler;
	private final RedisCommands<String, String> redisCommand;

    public SMPPEnquiryLinkPooler() {
		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("SMPP_SERVER");

		// Initiate LoggingPooler
		new LoggingPooler();

		// Initiate RedisPooler
		redisPooler = new RedisPooler();
		redisCommand = redisPooler.redisInitiateConnection();
				
		LoggingPooler.doLog(logger, "INFO", "SMPPEnquryLinkPooler", "SMPPEnquryLinkPooler", false, false, false, "", 
				"Module SMPPEnquryLinkPooler is initiated and ready to serve.", null);
	}
	
    public void logEnquiryLink(String clientId, String sessionId, String activity){
    	System.out.println("logEnquiryLink clientId: " + clientId + ", sessionId: " + sessionId);
    	try{
    		LocalDateTime now = LocalDateTime.now();

    		String redisKey = clientId + "-" + sessionId;
    		String redisVal = now.format(formatter) + "-" + activity;

            // 1 day from last operation
            int redisExpiry = 24 * 60 * 60;
            redisPooler.redisSetWithExpiry(redisCommand, redisKey, redisVal, redisExpiry);
    		
    		System.out.println("Data enquiry link clientId: " + clientId + ", sessionId: " + sessionId + " is saved");
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}
