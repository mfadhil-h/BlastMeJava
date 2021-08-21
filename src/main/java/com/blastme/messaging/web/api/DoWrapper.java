package com.blastme.messaging.web.api;

import java.io.File;
import java.util.HashMap;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.json.JSONArray;
import org.json.JSONObject;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.Tool;

public class DoWrapper implements Processor {
	private Logger logger;
	private Tool tool;

	public DoWrapper() {
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());

		logger = LogManager.getLogger("WEBAPI");
		
		// Initiate tool
		tool = new Tool();
		
		LoggingPooler.doLog(logger, "INFO", "DoWrapper", "DoWrapper", false, false, false, "", "Module DoWrapper is initiated and ready to serve ...", null);		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void process(Exchange exchange) throws Exception {
		HashMap<String, String> mapIncoming = new HashMap<String, String>();
		
		mapIncoming = exchange.getIn().getBody(HashMap.class);
		LoggingPooler.doLog(logger, "INFO", "DoWrapper", "DoWrapper", false, false, false, "", 
				"mapIncoming: " + tool.convertHashMapToJSON(mapIncoming).toString(), null);
		
		// JSONResponse
		JSONObject jsonResp = new JSONObject();
		
		// Main parameters
		String trxFinalStatus = "000";
		
		// Check if mapIncoming has status
		if(mapIncoming.containsKey("status")){
			if(mapIncoming.get("status").trim().equals("000")){
				// Proses parameter lain.
				trxFinalStatus = mapIncoming.get("status").trim();
				// Put into jsonResp
				jsonResp.put("status", trxFinalStatus);
				
				if(mapIncoming.get("instruction").equals("login")){
					jsonResp.put("privilege", mapIncoming.get("privilege"));
					jsonResp.put("sessionId", mapIncoming.get("sessionId"));
					jsonResp.put("clientId", mapIncoming.get("clientId"));
					jsonResp.put("clientName", mapIncoming.get("clientName"));
				} else if(mapIncoming.get("instruction").equals("viewtransaction")){
					String theData = mapIncoming.get("data");
					if(theData.trim().length() > 0){
						JSONArray jsonData = new JSONArray(theData);
						jsonResp.put("data", jsonData);
					} else {
						jsonResp.put("data", new JSONArray());
					}
					
					String totalDocument = mapIncoming.get("totalDocument");
					jsonResp.put("totalDocument", Integer.parseInt(totalDocument));
				} else if(mapIncoming.get("instruction").equals("viewclient")){
					String theData = mapIncoming.get("data");
					if(theData.trim().length() > 0){
						JSONArray jsonData = new JSONArray(theData);
						jsonResp.put("data", jsonData);
					} else {
						jsonResp.put("data", new JSONArray());
					}
				} else if(mapIncoming.get("instruction").equals("viewemailsenderidlist")){
					String theData = mapIncoming.get("data");
					if(theData.trim().length() > 0){
						JSONArray jsonData = new JSONArray(theData);
						jsonResp.put("data", jsonData);
					} else {
						jsonResp.put("data", new JSONArray());
					}
				} else if(mapIncoming.get("instruction").equals("getemailsenderidproperty")){
					String theData = mapIncoming.get("data");
					if(theData.trim().length() > 0){
						JSONObject jsonData = new JSONObject(theData);
						jsonResp.put("data", jsonData);
					} else {
						jsonResp.put("data", new JSONObject());
					}
				} else if(mapIncoming.get("instruction").equals("viewemailbatch")){
					String theData = mapIncoming.get("data");
					if(theData.trim().length() > 0){
						JSONArray jsonData = new JSONArray(theData);
						jsonResp.put("data", jsonData);
					} else {
						jsonResp.put("data", new JSONArray());
					}
				} else if(mapIncoming.get("instruction").equals("viewuserapisms")){
					String theData = mapIncoming.get("data");
					if(theData.trim().length() > 0){
						JSONArray jsonData = new JSONArray(theData);
						jsonResp.put("data", jsonData);
					} else {
						jsonResp.put("data", new JSONArray());
					}
				}
			} else {
				// Failed, return the trxFinalStatus only
				trxFinalStatus = mapIncoming.get("status").trim();

				// Put into jsonResp
				jsonResp.put("status", trxFinalStatus);
			}
		} else {
			trxFinalStatus = "900";
			
			// Put into jsonResp
			jsonResp.put("status", trxFinalStatus);
		}
		
		// Send as output result
		String hasilFinal = jsonResp.toString();
		LoggingPooler.doLog(logger, "INFO", "DoWrapper", "DoWrapper", false, false, false, "", 
				"hasilFinal: " + hasilFinal, null);
		
		// Set response header content-type to be JSON
		exchange.getOut().setHeader("Content-Type", "application/json");
		
		exchange.getOut().setBody(hasilFinal);	
	}

}
