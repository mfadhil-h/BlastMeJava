package com.blastme.messaging.web.api;

import java.io.File;

import org.apache.camel.builder.RouteBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.LoggingPooler;

public class WebRouter extends RouteBuilder{
	private static Logger logger;
	
	// Blastme
	private static String urlIncomingEndPoint = "restlet:http://localhost:8001/webapi?restletMethods=post";

	public WebRouter() {
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());

		logger = LogManager.getLogger("WEBAPI");
		
		new LoggingPooler();

		LoggingPooler.doLog(logger, "INFO", "WebRouter", "WebRouter", false, false, false, "", 
				"API Url: " + urlIncomingEndPoint, null);
		
		LoggingPooler.doLog(logger, "INFO", "WebRouter", "WebRouter", false, false, false, "", 
				"Module WebRouter is intiated and ready to serve ... ", null);		
	}

	@Override
	public void configure() throws Exception {
		from(urlIncomingEndPoint)
		.process(new DoVerifyData())
		.log("Message after VerifyData: ${body}")
		.choice()
			.when().simple("${body[status]} == 000")
				.choice()
					.when().simple("${body[instruction]} == 'pushtoqueuesmsincoming'")
						.process(new DoTool())
						.log("Message after Login: ${body}")
				.end()
		.end()
		.process(new DoWrapper());		
	}

}
