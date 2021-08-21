package com.blastme.messaging.web.api;

import java.io.File;
import java.util.HashMap;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.json.JSONObject;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.Tool;

public class DoVerifyData implements Processor {
	private Logger logger;
	private Tool tool;

	public DoVerifyData() {
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());

		logger = LogManager.getLogger("WEBAPI");
		
		// Initiate tool
		tool = new Tool();

		LoggingPooler.doLog(logger, "INFO", "DoVerifyData", "DoVerifyData", false, false, false, "", "Module DoVerifyData is initiated and ready to serve ...", null);
	}

	@Override
	public void process(Exchange exchange) throws Exception {
		// Make HashMap with command as instruction header
		String dataIn = exchange.getIn().getBody(String.class);
		LoggingPooler.doLog(logger, "DEBUG", "DoVerifyData", "process", false, false, false, "", "Module VerifyData - incoming data is " + dataIn, null);

		// Hashmap to pass to next process
		HashMap<String, String> mapPars = new HashMap<String, String>();
		// dataIn will be in JSON format
		try{
			JSONObject jsonIncoming = new JSONObject(dataIn);
			
			// Required parameters
			String instruction = jsonIncoming.getString("instruction");
			
			if(instruction.trim().equals("login")){
				String userName = jsonIncoming.getString("username");
				String password = jsonIncoming.getString("password");
				String remoteIpAddrss = jsonIncoming.getString("remoteIpAddress");
				
				String verifyStatus = "000";
				
				// Put into mapPars
				mapPars.put("status", verifyStatus);
				mapPars.put("instruction",instruction);
				mapPars.put("userName", userName);
				mapPars.put("password", password);
				mapPars.put("remoteIpAddress", remoteIpAddrss);
			} else {
				String userName = jsonIncoming.getString("username");
				String sessionId = jsonIncoming.getString("sessionId");
				String remoteIpAddrss = jsonIncoming.getString("remoteIpAddress");
				
				String verifyStatus = "000";
				
				HashMap<String, String> mapJson = tool.convertJSONToHashMap(jsonIncoming);
				mapPars.putAll(mapJson);
				
				// Put into mapPars
				mapPars.put("status", verifyStatus);
				mapPars.put("instruction",instruction);
				mapPars.put("userName", userName);
				mapPars.put("sessionId", sessionId);
				mapPars.put("remoteIpAddress", remoteIpAddrss);
			}
		} catch(Exception e) {
			LoggingPooler.doLog(logger, "DEBUG", "DoVerifyData", "process", true, false, false, "", 
					"Failed to verify data. Error occured.", e);
			
			String verifyStatus = "700";
			
			// Put into mapPars
			mapPars.put("status", verifyStatus);
		}
		
		exchange.getOut().setBody(mapPars);
	}

}
