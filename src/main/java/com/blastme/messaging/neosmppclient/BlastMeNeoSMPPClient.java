package com.blastme.messaging.neosmppclient;

import java.io.File;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.neosmppclient.greembee.SMPPTransceiverGreembeeP2P;
import com.blastme.messaging.toolpooler.LoggingPooler;

public class BlastMeNeoSMPPClient {
	private static Logger logger;
	
	public BlastMeNeoSMPPClient() {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());

		// Setup logger
		logger = LogManager.getLogger("SMPP_CLIENT");

		// Initiate LoggingPooler
		new LoggingPooler();
	}

	public static void main(String[] args) {
		// Run Greembee SMPPTransceiver
		int greembeeThread = 3;
		ExecutorService executorGreembee = Executors.newFixedThreadPool(greembeeThread);
		executorGreembee.execute(new SMPPTransceiverGreembeeP2P("01"));
		executorGreembee.execute(new SMPPTransceiverGreembeeP2P("02"));
		executorGreembee.execute(new SMPPTransceiverGreembeeP2P("03"));
	}
}
