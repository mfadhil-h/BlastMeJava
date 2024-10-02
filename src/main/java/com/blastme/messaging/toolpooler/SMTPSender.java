package com.blastme.messaging.toolpooler;

import java.io.File;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import com.blastme.messaging.configuration.Configuration;

public class SMTPSender {
	private static Logger logger;

	public SMTPSender() {
		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("POOLER");

		// Initiate LoggingPooler
		new LoggingPooler();
	}

	public int sendingSMTP(String messageId, String username, String password, String host, int port, boolean isStartTlsEnabled, String subject,
			String body, String fromName, String fromAddress, String toAddress, String attachmentPath01, String attachmentPath02, 
			String attachmentPath03, String attachmentPath04, String attachmentPath05){
		int trxStatus = 0;
        LoggingPooler.doLog(logger, "INFO", "SMTPSender", "sendingSMTP", false, false, true, messageId, 
        		"username: " + username + ", password: " + password + ", host: " + host + ", port: " + port + ", subject: " + subject + ", body: " + body + 
        		", fromName: " + fromName + ", fromAddrss: " + fromAddress + ", toAddrss: " + toAddress + 
        		", attachmentPath01: " + attachmentPath01 + ", attachmentPath02: " + attachmentPath02 + 
        		", attachmentPath03: " + attachmentPath03 + ", attachmentPath04: " + attachmentPath04 + ", attachmentPath05: " + attachmentPath05, null);

	    try{		    
	    	Properties props = System.getProperties();
	    	props.put("mail.transport.protocol", "smtp");
	    	props.put("mail.smtp.port", port); 
	    	props.put("mail.smtp.starttls.enable", "true");
	    	props.put("mail.smtp.auth", "true");

	        // Create a Session object to represent a mail session with the specified properties. 
	    	Session session = Session.getDefaultInstance(props);

	        // Create a message with the specified information. 
	        MimeMessage msg = new MimeMessage(session);
	        msg.setFrom(new InternetAddress(fromAddress,fromName));
	        msg.setRecipient(Message.RecipientType.TO, new InternetAddress(toAddress));
	        msg.setSubject(subject);
	        //msg.setContent(body,"text/html");
	        
	        // Prepare the multipart to contain body and attachments
	        Multipart multipart = new MimeMultipart();
	        
	        // Set email body
	        MimeBodyPart messagePart = new MimeBodyPart();
	        messagePart.setContent(body, "text/html");
	        multipart.addBodyPart(messagePart);

	        // Attachment01
	        if(attachmentPath01 != null && attachmentPath01.trim().length() > 0){
	        	MimeBodyPart attachment01Part = new MimeBodyPart();
	        	
	        	FileDataSource fileDataSource01 = new FileDataSource(attachmentPath01){
	        		@Override
	        		public String getContentType(){
	        			return "application/octet-stream";
	        		}
	        	};
	        	
	        	attachment01Part.setDataHandler(new DataHandler(fileDataSource01));
	        	attachment01Part.setFileName(fileDataSource01.getName());
	        	
	        	multipart.addBodyPart(attachment01Part);
	        }
	        
	        // Attachment02
	        if(attachmentPath02 != null && attachmentPath02.trim().length() > 0){
	        	MimeBodyPart attachment02Part = new MimeBodyPart();
	        	
	        	FileDataSource fileDataSource02 = new FileDataSource(attachmentPath02){
	        		@Override
	        		public String getContentType(){
	        			return "application/octet-stream";
	        		}
	        	};
	        	
	        	attachment02Part.setDataHandler(new DataHandler(fileDataSource02));
	        	attachment02Part.setFileName(fileDataSource02.getName());
	        	
	        	multipart.addBodyPart(attachment02Part);
	        }
	        
	        // Attachment03
	        if(attachmentPath03 != null && attachmentPath03.trim().length() > 0){
	        	MimeBodyPart attachment03Part = new MimeBodyPart();
	        	
	        	FileDataSource fileDataSource03 = new FileDataSource(attachmentPath03){
	        		@Override
	        		public String getContentType(){
	        			return "application/octet-stream";
	        		}
	        	};
	        	
	        	attachment03Part.setDataHandler(new DataHandler(fileDataSource03));
	        	attachment03Part.setFileName(fileDataSource03.getName());
	        	
	        	multipart.addBodyPart(attachment03Part);
	        }
	        
	        // Attachment04
	        if(attachmentPath04 != null && attachmentPath04.trim().length() > 0){
	        	MimeBodyPart attachment04Part = new MimeBodyPart();
	        	
	        	FileDataSource fileDataSource04 = new FileDataSource(attachmentPath04){
	        		@Override
	        		public String getContentType(){
	        			return "application/octet-stream";
	        		}
	        	};
	        	
	        	attachment04Part.setDataHandler(new DataHandler(fileDataSource04));
	        	attachment04Part.setFileName(fileDataSource04.getName());
	        	
	        	multipart.addBodyPart(attachment04Part);
	        }
	        
	        // Attachment05
	        if(attachmentPath05 != null && attachmentPath05.trim().length() > 0){
	        	MimeBodyPart attachment05Part = new MimeBodyPart();
	        	
	        	FileDataSource fileDataSource05 = new FileDataSource(attachmentPath05){
	        		@Override
	        		public String getContentType(){
	        			return "application/octet-stream";
	        		}
	        	};
	        	
	        	attachment05Part.setDataHandler(new DataHandler(fileDataSource05));
	        	attachment05Part.setFileName(fileDataSource05.getName());
	        	
	        	multipart.addBodyPart(attachment05Part);
	        }
	        
	        
	        // Add multipart into msg
	        msg.setContent(multipart);
	        
	        // Create a transport.
	        Transport transport = session.getTransport();
	                    
	        // Send the message.
	        try
	        {
	            LoggingPooler.doLog(logger, "INFO", "SMTPSender", "sendingSMTP", false, false, true, "", 
	            		"Sending email to " + toAddress + " ... ", null);
	            
	            // Connect to Amazon SES using the SMTP username and password you specified above.
	            transport.connect(host, username, password);
	        	
	            // Send the email.
	            transport.sendMessage(msg, msg.getAllRecipients());
	            LoggingPooler.doLog(logger, "INFO", "SMTPSender", "sendingSMTP", false, false, true, "", 
	            		"Email sent to " + toAddress + " ... ", null);	
	            
	            trxStatus = 0;
	        }
	        catch (Exception ex) {
	            LoggingPooler.doLog(logger, "INFO", "SMTPSender", "sendingSMTP", true, false, true, "", 
	            		"Failed sending email to " + toAddress + ". Error occured.", ex);	
	            
	            trxStatus = -1;
	        }
	        finally
	        {
	            // Close and terminate the connection.
	            transport.close();
	        }
	    } catch (Exception e) {
            LoggingPooler.doLog(logger, "INFO", "SMTPSender", "sendingSMTP", true, false, true, "", 
            		"Failed sending email to " + toAddress + ". Error occured.", e);
            
            trxStatus = -1;
	    }
		
		return trxStatus;
	}
}
