package com.blastme.messaging.web.api;

import java.io.File;
import java.util.TimeZone;

import org.apache.camel.main.Main;
import org.apache.camel.main.MainListenerSupport;
import org.apache.camel.main.MainSupport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.LoggingPooler;

public class WebAPI {
	private Main main;
	private static Logger logger;

	public static void main(String[] args) {
		// Load logger configuration file
		new Configuration();
		
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());

        logger = LogManager.getLogger("WEBAPI");

		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));
		
		// Initiate LoggingPooler
		new LoggingPooler();

		WebAPI apI = new WebAPI();
		try {
			apI.boot();
		} catch (Exception e) {
			LoggingPooler.doLog(logger, "INFO", "WebAPI", "main", true, false, true, "", "Error while laoding web API.", e);
		}
	}

	public void boot() throws Exception{
		main = new Main();
		
		//add route
		main.addRouteBuilder(new WebRouter());
		
		//add event listener
		main.addMainListener(new Events());
		
		main.run();
	}

	public static class Events extends MainListenerSupport{

		@Override
		public void afterStart(MainSupport main) {
			LoggingPooler.doLog(logger, "INFO", "WebAPI", "boot", false, false, true, "", "Web API System is now started!", null);
		}

		@Override
		public void beforeStop(MainSupport main) {
			LoggingPooler.doLog(logger, "INFO", "WebAPI", "boot", false, false, true, "", "Web API System is being stopped!", null);
		}
		
	}
}
