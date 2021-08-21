package com.blastme.messaging.smsbulk.dlrreceiver;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.json.JSONObject;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.RedisPooler;
import com.blastme.messaging.toolpooler.TransactionRedisPooler;

import io.lettuce.core.api.sync.RedisCommands;

public class JatisMobileDLRReceiver {
	private Logger logger;
	private static int port = 8888;
	private static String path = "dr";
	private SimpleDateFormat sdf;
	private static String queueSMPP = "DLR_SMPP";
	private static String queueHTTP = "DLR_HTTP";
	
	private static RedisPooler redisPooler;
	private static RedisCommands<String, String> redisCommand;
	
	private TransactionRedisPooler transactionRedisPooler;
	
	public JatisMobileDLRReceiver() {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
        logger = LogManager.getLogger("DLR_RECEIVER_JATISMOBILE");
        
		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate SimpleDateFormat
		sdf = new SimpleDateFormat("yyMMddHHmmssSSS");
		
		// Initiate RedisPooler
		redisPooler = new RedisPooler();
		redisCommand = redisPooler.redisInitiateConnection();
		
		// Initiate RabbitMQPooler
		new RabbitMQPooler();		
		
		// Initiate transactionRedisPooler
		transactionRedisPooler = new TransactionRedisPooler();

		LoggingPooler.doLog(logger, "INFO", "JatisMobileDLRReceiver", "JatisMobileDLRReceiver", false, false, false, "", 
				"Starting Jatis Mobile HTTP DLR Receiver...", null);
	}
	
	private void startServer(){
		Server server = new Server(port);
		
		ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.setContextPath("/" + path);

		server.setHandler(context);
		context.addServlet(new ServletHolder(new HandlerServlet()), "/*");
		
		try {
			server.start();
			server.join();
			
			LoggingPooler.doLog(logger, "INFO", "JatisMobileDLRReceiver", "startServer", false, false, false, "", 
					"Service Status Receiver HTTP Server is started.", null);
		} catch (Exception e) {
			LoggingPooler.doLog(logger, "INFO", "JatisMobileDLRReceiver", "startServer", true, false, false, "", 
					"Service Status Receiver HTTP Server is started.", e);
			
			e.printStackTrace();
		}
	}
	
	private class HandlerServlet extends HttpServlet{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		private JSONObject extractQueryString(String queryString){
			JSONObject jsonPars = new JSONObject();
			
			String[] parameters = queryString.split("&");

			for(int x = 0; x < parameters.length; x++){
				String[] par = parameters[x].split("=");
				
				if(par.length > 1){
					jsonPars.put(par[0], par[1]);
				}
			}
			
			return jsonPars;
		}

		@Override
		protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
			// Get theURI for DR (it depicts which client)
			String theURI = req.getRequestURI();
			LoggingPooler.doLog(logger, "DEBUG", "JatisMobileDLRReceiver - HandlerServlet", "handleGetDR", false, false, false, "", 
					"Initial URI: " + theURI, null);			
			
			theURI = theURI.replace("/" + path + "/", "").replace("/", "");
			LoggingPooler.doLog(logger, "DEBUG", "JatisMobileDLRReceiver - HandlerServlet", "handleGetDR", false, false, false, "", 
					"theURI: " + theURI, null);			

			// Get remoteIpAddress
			String remoteIpAddress = req.getRemoteHost();
			LoggingPooler.doLog(logger, "DEBUG", "JatisMobileDLRReceiver - HandlerServlet", "handleGetDR", false, false, false, "", 
					"remoteIpAddress: " + remoteIpAddress, null);			

			// Get the strQueryString
			String queryString = req.getQueryString();
			LoggingPooler.doLog(logger, "DEBUG", "JatisMobileDLRReceiver - HandlerServlet", "handleGetDR", false, false, false, "", 
					"queryString: " + queryString, null);			
			
			try{
				// Change the queryString to jsonParameters
				JSONObject jsonParameters = new JSONObject();
				jsonParameters = extractQueryString(queryString);
				LoggingPooler.doLog(logger, "DEBUG", "JatisMobileDLRReceiver - HandlerServlet", "handleGetDR", false, false, false, "", 
						"jsonParameters: " + jsonParameters.toString(), null);	
				
				// Check mapping jatisMessageId to blastmeMessageId - as setuped in TransceiverJatisMobile
				String jatisMessageId = jsonParameters.getString("MessageId");
				String redisMapKey = "trxJatis-" + jatisMessageId;
				String blastmeMessageId = redisPooler.redisGet(redisCommand, redisMapKey);
				
				if(blastmeMessageId != null && blastmeMessageId.trim().length()  > 0){
					// Mapping Jatis Message ID - Blastme MessageID exists
					// Do the process
					// Get detail transaction from redis
					String detailTrx = transactionRedisPooler.getTrxRedisData(blastmeMessageId);
					LoggingPooler.doLog(logger, "DEBUG", "JatisMobileDLRReceiver - HandlerServlet", "handleGetDR", false, false, false, "", 
							"Jatis MessageID: " + jatisMessageId + " - messageId: " + blastmeMessageId + " - detail: " + detailTrx, null);	
					
					// Get receiverType to pass to which queue
					JSONObject jsonTrxData = new JSONObject(detailTrx);
					
					String receiverType = jsonTrxData.getString("receiverType");
					LoggingPooler.doLog(logger, "DEBUG", "JatisMobileDLRReceiver - HandlerServlet", "handleGetDR", false, false, false, "", 
							"Jatis MessageID: " + jatisMessageId + " - messageId: " + blastmeMessageId + " - receiverType: " + receiverType, null);
					
					String queueName = queueSMPP;
					if(receiverType.trim().equals("SMPP")){
						queueName = queueSMPP;
					} else if(receiverType.trim().equals("HTTP")){
						queueName = queueHTTP;
					}
					
					
				} else {
					// Mapping Jatis Message ID - Blastme MessageID does NOT exist
					
				}
			} catch(Exception e) {
				
			}
			

		}
	}

	public static void main(String[] args) {
		JatisMobileDLRReceiver drReceiver = new JatisMobileDLRReceiver();
		drReceiver.startServer();
	}

}
