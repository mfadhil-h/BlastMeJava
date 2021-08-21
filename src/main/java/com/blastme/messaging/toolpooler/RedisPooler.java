package com.blastme.messaging.toolpooler;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import com.blastme.messaging.configuration.Configuration;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

public class RedisPooler {
	private Logger logger;
	
	private RedisClient redisClient;
	public StatefulRedisConnection<String, String> redisConnection;
	//public RedisCommands<String, String> syncCommands;
	
	private static String redisAuth;
	private static String redisHost;
	private static int redisPort;
	
	public RedisPooler() {
		// Load logger configuration file
		new Configuration();
		
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());

		logger = LogManager.getLogger("REDISPOOLER");
		
		// Initiate redis property
		redisAuth = Configuration.getRedisAuth();
		redisHost = Configuration.getRedisHost();
		redisPort = Configuration.getRedisPort();
		
		LoggingPooler.doLog(logger, "INFO", "RedisPooler", "RedisPooler", false, false, false, "", "REDISPOOLER is initiated", null);
	}
	
	public RedisCommands<String, String> redisInitiateConnection(){
		if(redisConnection != null && redisConnection.isOpen()){
			// Do nothing, it is open and connected anyway
			RedisCommands<String, String> syncCommands = redisConnection.sync();
			
			LoggingPooler.doLog(logger, "DEBUG", "RedisPooler", "RedisPooler", false, false, false, "", 
					"redisConnection is ALREADY initiated. Do nothing to re-initiate.", null);
			
			return syncCommands;
		} else {
			// Initiate connection to server
			redisClient = RedisClient.create("redis://" + redisAuth + "@" + redisHost + ":" + Integer.toString(redisPort));
			redisConnection = redisClient.connect();
			RedisCommands<String, String> syncCommands = redisConnection.sync();
						
			LoggingPooler.doLog(logger, "DEBUG", "RedisPooler", "RedisPooler", false, false, false, "", 
					"redisConnection is NOT initiated yet. Initiating it!", null);
			
			return syncCommands;
		}
	}
	
	public StatefulRedisConnection<String, String> getRedisConnection(){
		return redisConnection;
	}
	
	public void redisSet(RedisCommands<String, String> syncCommands, String key, String value){
		syncCommands.set(key, value);

		
		LoggingPooler.doLog(logger, "DEBUG", "RedisPooler", "RedisPooler", false, false, false, "", 
				"Successfully set redis - key: " + key + ", value: " + value, null);			
	}
	
	public void redisSetWithExpiry(RedisCommands<String, String> syncCommands, String key, String value, int secondsExpiry){
		syncCommands.set(key, value);
		syncCommands.expire(key, secondsExpiry);

		LoggingPooler.doLog(logger, "DEBUG", "RedisPooler", "RedisPooler", false, false, false, "", 
				"Successfully set redis - key: " + key + ", value: " + value + ", expiry: " + Integer.toString(secondsExpiry), null);			
	}
	
	public void redisDel(RedisCommands<String, String> syncCommands, String key){
		syncCommands.del(key);

		LoggingPooler.doLog(logger, "DEBUG", "RedisPooler", "RedisPooler", false, false, false, "", 
				"Successfully delete redis - key: " + key, null);			
	}
	
	public String redisGet(RedisCommands<String, String> syncCommands, String key){
		String result = "";
		
		result = syncCommands.get(key);
		
		if (result == null) {
			result = "";
		}
		
		LoggingPooler.doLog(logger, "DEBUG", "RedisPooler", "RedisPooler", false, false, false, "", 
				"Successfully read redis - key: " + key, null);			
		return result;
	}
	
	public String redisGetDel(RedisCommands<String, String> syncCommands, String key){
		String result = "";
		
		result = syncCommands.get(key);
		syncCommands.del(key);
		
		LoggingPooler.doLog(logger, "DEBUG", "RedisPooler", "RedisPooler", false, false, false, "", 
				"Successfully read and delete redis - key: " + key, null);			
		
		return result;
	}
}
