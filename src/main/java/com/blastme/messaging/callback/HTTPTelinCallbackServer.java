package com.blastme.messaging.callback;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Enumeration;
import java.util.TimeZone;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.json.JSONObject;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.rabbitmq.client.Channel;

public class HTTPTelinCallbackServer {
	private static Logger logger;
	
	private static int port = 8898;
	private static String path = "telincb";
	
	private static String callbackQueueName = "TELINCALLBACK";
	
	private RabbitMQPooler rabbitMqPooler;
	private Channel publishingChannel;
	
	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

	public HTTPTelinCallbackServer() {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
        logger = LogManager.getLogger("HTTP_TELIN_CALLBACK");
        
		// Initiate logTool
		new LoggingPooler();
        
        rabbitMqPooler = new RabbitMQPooler();
        
        try {
        	publishingChannel = rabbitMqPooler.getChannel(rabbitMqPooler.getConnection());
			publishingChannel.queueDeclare(callbackQueueName, true, false, false, null);

			LoggingPooler.doLog(logger, "INFO", "HTTPCallbackServer", "HTTPCallbackServer", false, false, true, "", 
					"Module StatusReceiverHTTPServer is loaded. Connected to queue " + callbackQueueName, null);
		} catch (Exception e) {
			LoggingPooler.doLog(logger, "INFO", "HTTPCallbackServer", "HTTPCallbackServer", true, false, true, "", 
					"Failed loading Module HTTPCallbackServer. Error occured.", e);

			e.printStackTrace();
			
			System.exit(-1);
		}		
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
			
			LoggingPooler.doLog(logger, "INFO", "HTTPCallbackServer", "startServer", false, false, true, "", 
					"Service HTTPCallbackServer is running on port " + port, null);
		} catch (Exception e) {
			LoggingPooler.doLog(logger, "INFO", "HTTPCallbackServer", "startServer", true, false, true, "", 
					"Service HTTPCallbackServer is failed running on port " + port + ". Error occured", e);
			
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		HTTPTelinCallbackServer cbServer = new HTTPTelinCallbackServer();
		cbServer.startServer();
	}

	
	private class HandlerServlet extends HttpServlet{
		private static final long serialVersionUID = 1L;

		@Override
		protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException{
			System.out.println("QueryString: " + request.getQueryString());
			
			LoggingPooler.doLog(logger, "INFO", "HTTPCallbackServer", "doGet", false, false, true, "", 
					"GET incoming request: " + request.toString(), null);

			Enumeration<String> headerNames = request.getHeaderNames();
			while(headerNames.hasMoreElements()) {
			  String headerName = (String)headerNames.nextElement();
				LoggingPooler.doLog(logger, "INFO", "HTTPCallbackServer", "doGet", false, false, true, "", 
						"GET incoming header name: " + headerName + ", headerValue: " + request.getHeader(headerName), null);
			}
			
			// Get requestQueryString
			String requestQueryString = request.getQueryString();
			
			// Get the remoteIpAddress
			String remoteIpAddress = request.getRemoteAddr();

			LoggingPooler.doLog(logger, "INFO", "HTTPCallbackServer", "doGet", false, false, true, "", 
					"GET incoming Remote IP Address: " + remoteIpAddress + ", requestURI: " + request.getRequestURI() + 
					", requestURL: " + request.getRequestURL() + ", requestQueryString: " + requestQueryString, null);

			// Get callback path from RequestURI.
			String callBackPath = request.getRequestURI().replace("/" + path + "/", "");			
			LoggingPooler.doLog(logger, "INFO", "HTTPCallbackServer", "doGet", false, false, true, "", 
					"GET incoming callback path: " + callBackPath, null);

			String getResponse = processTheGetCallBack(requestQueryString, remoteIpAddress, callBackPath);
			
			response.setStatus(HttpStatus.OK_200);
			response.getWriter().println(getResponse);
		}
		
		@Override
		protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException{
			System.out.println("RemoteAddress: " + request.getRemoteAddr() + " - RemoteHost: " + request.getRemoteHost() + " - RemotePort: " + Integer.toString(request.getRemotePort()));
			System.out.println("RequestURI: " + request.getRequestURI());
			System.out.println("RequestURL: " + request.getRequestURL());
			System.out.println("RequestContextPath: " + request.getContextPath());
			System.out.println("RequestQueryString: " + request.getQueryString());
			
			// Get the remoteIpAddress
			String remoteIpAddress = request.getRemoteAddr();

			// Get POST body content
			String postContent = getBodyContentOfPost(request);			
			LoggingPooler.doLog(logger, "INFO", "HTTPCallbackServer", "doPost", false, false, true, "", 
					"POST incoming request: " + postContent, null);

			// Get callback path from RequestURI.
			String callBackPath = request.getRequestURI().replace("/" + path + "/", "");
			callBackPath = callBackPath.replace("/", "").trim();
			
			String postResponse = processThePostCallBack(postContent, remoteIpAddress, callBackPath);
			
			response.setStatus(HttpStatus.OK_200);
			response.getWriter().println(postResponse);
		}
		
		private String getBodyContentOfPost(HttpServletRequest request){
			String bodyContent = null;
			StringBuilder buffer = new StringBuilder();
			BufferedReader reader = null;
			
			try {
				reader = request.getReader();
				
				String line = "";
				
				while((line = reader.readLine()) != null){
					buffer.append(line);
				}
			} catch (IOException e) {
				LoggingPooler.doLog(logger, "INFO", "HTTPCallbackServer", "doPost", true, false, true, "", 
						"Error while getting POST content.", e);
				e.printStackTrace();
			} finally {
		        if (reader != null) {
		            try {
		                reader.close();
		            } catch (IOException ex) {
		                ex.printStackTrace();
		            }
		        }
		    }
			
			bodyContent = buffer.toString();
			
			return bodyContent;
		}
		
		private String processTheGetCallBack(String incomingGetParameters, String remoteIpAddress, String callBackPath){
			String getResponse = "OK";
			
			LoggingPooler.doLog(logger, "INFO", "StatusReceiverHTTPServer", "processTheGetCallBack", false, false, true, "", 
					"incomingGetParameters: " + incomingGetParameters + ", callBackPath: " + callBackPath + ", remoteIpAddress: " + remoteIpAddress, null);

			JSONObject jsonToQueue = new JSONObject();
			jsonToQueue.put("remoteIpAddress", remoteIpAddress);
			jsonToQueue.put("incomingMessage", incomingGetParameters);
			jsonToQueue.put("callbackPath", callBackPath);
			jsonToQueue.put("incomingDateTime", LocalDateTime.now().format(formatter));
			
			try {
				publishingChannel.basicPublish("", callbackQueueName, null, jsonToQueue.toString().getBytes());
			} catch (IOException e) {
				LoggingPooler.doLog(logger, "DEBUG", "StatusReceiverHTTPServer", "processTheGetCallBack", true, false, true, "", 
						"Failed to publish callback to queue. Error occured.", e);
				e.printStackTrace();
			}

			getResponse = "[" + LocalDateTime.now().format(formatter) + "] RECEIVED OK";
			
			return getResponse;
		}
		
		private String processThePostCallBack(String incomingPostPars, String remoteIpAddress, String callBackPath){
			String postResponse = "OK";
			
			try {
			LoggingPooler.doLog(logger, "INFO", "StatusReceiverHTTPServer", "processThePostCallBack", false, false, true, "", 
					"incomingPostPars: " + incomingPostPars + ", callBackPath: " + callBackPath + ", remoteIpAddress: " + remoteIpAddress, null);

			// Put into queue aja di sini, spy enteng.
			// Wrap the parameter into JSONObject
				JSONObject jsonToQueue = new JSONObject();
				jsonToQueue.put("remoteIpAddress", remoteIpAddress);
				jsonToQueue.put("incomingMessage", incomingPostPars);
				jsonToQueue.put("callbackPath", callBackPath);
				jsonToQueue.put("incomingDateTime", LocalDateTime.now().format(formatter));
				
				LoggingPooler.doLog(logger, "INFO", "StatusReceiverHTTPServer", "processThePostCallBack", false, false, true, "", 
						"Proccess the callback to queue with message: " + jsonToQueue, null);

				publishingChannel.basicPublish("", callbackQueueName, null, jsonToQueue.toString().getBytes());
			} catch (IOException e) {
				LoggingPooler.doLog(logger, "INFO", "StatusReceiverHTTPServer", "processThePostCallBack", true, false, true, "", 
						"Failed to proccess the callback to queue. Error occured.", e);
				
				e.printStackTrace();
			}
			
			// Get default success response
			// It should be taken from other function but for now it will be like this
			postResponse = "[" + LocalDateTime.now().format(formatter) + "] RECEIVED OK";
			logger.debug("Function processThePostCallBack - HTTP Body Response to vendor: " + postResponse);
			
			return postResponse;
		}
	}
}
