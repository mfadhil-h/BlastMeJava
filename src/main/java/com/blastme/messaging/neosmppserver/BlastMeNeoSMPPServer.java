package com.blastme.messaging.neosmppserver;

import at.favre.lib.crypto.bcrypt.BCrypt;
import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.*;
import com.cloudhopper.smpp.*;
import com.cloudhopper.smpp.impl.DefaultSmppServer;
import com.cloudhopper.smpp.pdu.BaseBind;
import com.cloudhopper.smpp.pdu.BaseBindResp;
import com.cloudhopper.smpp.ssl.SslConfiguration;
import com.cloudhopper.smpp.type.SmppChannelException;
import com.cloudhopper.smpp.type.SmppProcessingException;
import com.rabbitmq.client.Connection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BlastMeNeoSMPPServer {
    private static Logger logger;

//    private final static int smppPort = 2775; // NON-TLS PORT
    private final static int smppPort = 2776; // TLS PORT
    private final static int smppMaxConnection = 500;
    private final static int smppRequestExpiryTimeout = 30000;
    private final static int smppWindowMonitorInterval = 15000;
    private final static int smppWindowSize = 10;

    private static RedisPooler redisPooler;

    static HashMap<String, SmppServerSession> mapSession = new HashMap<String, SmppServerSession>();

    public BlastMeNeoSMPPServer() {
        // Set timezone
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

        // Load Configuration
        new Configuration();
//		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
//		File file = new File(Configuration.getLogConfigPath());
//		context.setConfigLocation(file);

        // Setup logger
        logger = LogManager.getLogger("SMPP_SERVER");

        // Initiate LoggingPooler
        new LoggingPooler();

        try {
            // Initiate RedisPooler
            redisPooler = new RedisPooler();

            // Initiate RabbitMQPooler
            new RabbitMQPooler();

            // Initiate jsonPrefixProperty in TelecomPrefixPooler
            new TelecomPrefixPooler();

            // Initiate SenderIdSMSPooler
            new SenderIdSMSPooler();

            // Initiate RouteSMSPooler
            new RouteSMSPooler();
        } catch (Exception e) {
            e.printStackTrace();

            System.exit(-1);
        }
    }

    private void startSMPPServer() {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

        try {
            // create a server configuration
            SmppServerConfiguration configuration = new SmppServerConfiguration();
            configuration.setPort(smppPort);
            configuration.setMaxConnectionSize(smppMaxConnection);
            configuration.setNonBlockingSocketsEnabled(true);
            configuration.setDefaultRequestExpiryTimeout(smppRequestExpiryTimeout);
            configuration.setDefaultWindowMonitorInterval(smppWindowMonitorInterval);
            configuration.setDefaultWindowSize(smppWindowSize);
            configuration.setDefaultWindowWaitTimeout(configuration.getDefaultRequestExpiryTimeout());
            configuration.setDefaultSessionCountersEnabled(true);
            configuration.setJmxEnabled(true);

            // Enable smpp configuration for SSL configuration
            SslConfiguration sslConfig = getSslConfig();
            configuration.setSslConfiguration(sslConfig);
            configuration.setUseSsl(true);

            DefaultSmppServer smppServer = new DefaultSmppServer(configuration, new NeoSmppServerHandler(), executor);

            System.out.println("Starting NEO SMPP server...");
            smppServer.start();
//            System.out.println("NEO SMPP server is started");
            System.out.println("NEO SMPP TLS server is started");
            System.out.println("SSL/TLS Configuration - KeyStorePath: " + sslConfig.getKeyStorePath());
            System.out.println("smppServer Configuration: " + smppServer.getConfiguration().getSslConfiguration().toString());
        } catch (SmppChannelException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static SslConfiguration getSslConfig() {
        SslConfiguration sslConfig = new SslConfiguration();
        sslConfig.setKeyStorePath(Configuration.getPkcs12());
        sslConfig.setKeyStoreType("PKCS12");
        sslConfig.setKeyStorePassword(Configuration.getSslPass());
        sslConfig.setKeyManagerPassword(Configuration.getSslPass());
        sslConfig.setTrustStorePath(Configuration.getJks());
        sslConfig.setTrustStorePassword(Configuration.getSslPass());
        sslConfig.setValidateCerts(true);
        sslConfig.setValidatePeerCerts(true);
        return sslConfig;
    }

    public static void main(String[] args) {
        BlastMeNeoSMPPServer smppServer = new BlastMeNeoSMPPServer();
        smppServer.startSMPPServer();
    }


    public static class NeoSmppServerHandler implements SmppServerHandler {
        private UserAPISMPPSMSPooler userApiSMPPSMSPooler;
        private SMPPEnquiryLinkPooler smppEnquiryLinkPooler;
        private ClientBalancePooler clientBalancePooler;
        private SMSTransactionOperationPooler smsTransactionOperationPooler;
        private ClientPropertyPooler clientPropertyPooler;
        private RouteSMSPooler routeSMSPooler;
        private RabbitMQPooler rabbitPooler;
        private Connection rabbitConnection;

        private Tool tool;

        private final static ScheduledThreadPoolExecutor execClientDLR = new ScheduledThreadPoolExecutor(5);
//        private final static ScheduledThreadPoolExecutor execClientMOWAMessage = new ScheduledThreadPoolExecutor(1);

        public NeoSmppServerHandler() {
            try {
                // Initiate tool
                tool = new Tool();

                userApiSMPPSMSPooler = new UserAPISMPPSMSPooler();
                System.out.println("USERAPISMPPSMSPooler is initiated.");

                smppEnquiryLinkPooler = new SMPPEnquiryLinkPooler();
                System.out.println("SMPPEnquiryLinkPooler is initiated.");

                clientBalancePooler = new ClientBalancePooler();
                System.out.println("ClientBalancePooler is initiated.");

                smsTransactionOperationPooler = new SMSTransactionOperationPooler();
                System.out.println("SMSTransactionPooler is initiated.");

                clientPropertyPooler = new ClientPropertyPooler();
                System.out.println("ClientPropertyPooler is initiated.");

                rabbitPooler = new RabbitMQPooler();
                System.out.println("RabbitMQPooler is initiated.");

                routeSMSPooler = new RouteSMSPooler();
                System.out.println("RouteSMSPooler is initiated.");

                rabbitConnection = rabbitPooler.getConnection();
                System.out.println("RabbitMQConnection is initiated.");

                // Run executor DLR Client
                System.out.println("Executing CLIENTDLRSUBMITTER is disabled it will handled by smppneo without tls.");
                execClientDLR.schedule(new BlastMeNeoClientDLRSubmitter(), 10, TimeUnit.SECONDS);
//                execClientMOWAMessage.schedule(new BlastmeNeoSMPPWAIncomingSubmitter(), 10, TimeUnit.SECONDS);
            } catch (Exception e) {
                e.printStackTrace();

                LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPServer", "NeoSMPPServerHandler", true, false, true, "",
                        "Failed to initiate NeoSMPPServerHandler. Error occur.", e);
            }
        }

        @Override
        public void sessionBindRequested(Long sessionId, SmppSessionConfiguration sessionConfiguration,
                                         @SuppressWarnings("rawtypes") BaseBind bindRequest) throws SmppProcessingException {

            String systemId = sessionConfiguration.getSystemId();
            String password = sessionConfiguration.getPassword();
            String remoteIpAddress = sessionConfiguration.getHost();

            LoggingPooler.doLog(logger, "DEBUG", "BlastMeSMPPServer - DefaultSmppServerHandler", "sessionBindRequested", false, false, true, "",
                    "Incoming binding request - systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress, null);

            // Validate username and password
            if (UserAPISMPPSMSPooler.jsonNeoSysIdAccess.has(systemId.trim())) {
                LoggingPooler.doLog(logger, "DEBUG", "NeoSmppServerHandler", "sessionBindRequested", false, false, true, "",
                        "Incoming binding request - systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress + ", SYSTEMID IS VERIFIED.", null);

                // systemId valid, check password
                JSONObject jsonDetail = UserAPISMPPSMSPooler.jsonNeoSysIdAccess.getJSONObject(systemId.trim());

                String configPassword = jsonDetail.getString("password");
                String configIPAddress = jsonDetail.getString("ipAddress");

                if (configIPAddress.trim().contains("ALL") || configIPAddress.trim().contains(remoteIpAddress.trim())) {
                    // Remote IP Address is VALID.
                    LoggingPooler.doLog(logger, "DEBUG", "NeoSmppServerHandler", "sessionBindRequested", false, false, true, "",
                            "Incoming binding request - systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress + ", REMOTE IP ADDRESS IS VERIFIED.", null);

                    // Verify the password
                    BCrypt.Result result = BCrypt.verifyer().verify(password.getBytes(StandardCharsets.UTF_8), configPassword.getBytes());

                    if (result.verified) {
                        // PASSWORD VERIFIED
                        LoggingPooler.doLog(logger, "DEBUG", "NeoSmppServerHandler", "sessionBindRequested", false, false, true, "",
                                "Incoming binding request - systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress + ", PASSWORD IS VERIFIED.", null);

                        // LOGIN SUCCESS
                        smsTransactionOperationPooler.saveSMPPBindAttempt(String.valueOf(sessionId), LocalDateTime.now(), systemId, remoteIpAddress, "BINDING REQUESTED", "PROCESSING");
                    } else {
                        // NOT VERIFIED
                        LoggingPooler.doLog(logger, "DEBUG", "NeoSmppServerHandler", "sessionBindRequested", false, false, true, "",
                                "Incoming binding request - systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress + ", PASSWORD IS INVALID.", null);

                        // Save to SMPP bind attempt to log
                        smsTransactionOperationPooler.saveSMPPBindAttempt(String.valueOf(sessionId), LocalDateTime.now(), systemId, remoteIpAddress, "BINDING REQUESTED", "FAILED TO BIND. INVALID SYSID/PASSWORD/REMOTE IP ADDRESS.");

                        // Throw invalid password
                        throw new SmppProcessingException(SmppConstants.STATUS_INVPASWD, null);
                    }
                } else {
                    // Remote IP Address INVALID
                    LoggingPooler.doLog(logger, "DEBUG", "NeoSmppServerHandler", "sessionBindRequested", false, false, true, "",
                            "Incoming binding request - systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress + ", REMOTE IP ADDRESS IS INVALID.", null);

                    // Save to SMPP bind attempt to log
                    smsTransactionOperationPooler.saveSMPPBindAttempt(String.valueOf(sessionId), LocalDateTime.now(), systemId, remoteIpAddress, "BINDING REQUESTED", "FAILED TO BIND. INVALID SYSID/PASSWORD/REMOTE IP ADDRESS.");

                    // Throw invalid password
                    throw new SmppProcessingException(SmppConstants.STATUS_INVSYSID, null);
                }
            } else {
                // SystemID INVALID
                LoggingPooler.doLog(logger, "DEBUG", "NeoSmppServerHandler", "sessionBindRequested", false, false, true, "",
                        "Incoming binding request - systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress + ", SYSTEMID IS INVALID.", null);

                // Save to SMPP bind attempt to log
                smsTransactionOperationPooler.saveSMPPBindAttempt(String.valueOf(sessionId), LocalDateTime.now(), systemId, remoteIpAddress, "BINDING REQUESTED", "FAILED TO BIND. INVALID SYSID/PASSWORD/REMOTE IP ADDRESS.");

                // Throw invalid password
                throw new SmppProcessingException(SmppConstants.STATUS_INVSYSID, null);
            }
        }

        @Override
        public void sessionCreated(Long sessionId, SmppServerSession session, BaseBindResp preparedBindResponse)
                throws SmppProcessingException {

            LoggingPooler.doLog(logger, "DEBUG", "NeoSmppServerHandler", "sessionCreated", false, false, true, "",
                    "Session created: " + sessionId.toString(), null);

            String systemId = session.getConfiguration().getSystemId();
            String remoteIpAddress = session.getConfiguration().getHost();
            String clientId = userApiSMPPSMSPooler.getNeoClientId(systemId);
            LoggingPooler.doLog(logger, "INFO", "NeoSmppServerHandler", "sessionCreated", false, false, true, "",
                    "systemId: " + systemId + ", remote IP address: " + remoteIpAddress + ", clientID: " + clientId, null);

            // Assign sessionId
            String blastmeSessionId = systemId + "-" + tool.generateUniqueID();
            LoggingPooler.doLog(logger, "DEBUG", "NeoSmppServerHandler", "sessionCreated", false, false, true, "",
                    "blastmeSessionId: " + blastmeSessionId, null);

            // Put session into mapSession
            mapSession.put(blastmeSessionId, session);

            // Name the session with blastmessSessionId
            session.getConfiguration().setName(blastmeSessionId);

            // Save to smpp bind attempt log
            smsTransactionOperationPooler.saveSMPPBindAttempt(String.valueOf(sessionId), LocalDateTime.now(), systemId, remoteIpAddress, "BINDING SESSION CREATED", "SUCCESS - SESSION CREATED");

            LoggingPooler.doLog(logger, "DEBUG", "NeoSmppServerHandler", "sessionCreated", false, false, true, "",
                    "Session created fro systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress + ", clientId: " +
                            clientId + " -> blastmeSessionId: " + blastmeSessionId + ". Session NAME: " + session.getConfiguration().getName(), null);

            session.serverReady(new BlastMeNeoSMPPSessionHandler(session, blastmeSessionId, remoteIpAddress, clientId, tool, smppEnquiryLinkPooler, clientPropertyPooler,
                    clientBalancePooler, smsTransactionOperationPooler, routeSMSPooler, rabbitPooler, rabbitConnection, redisPooler, logger));
        }

        @Override
        public void sessionDestroyed(Long sessionId, SmppServerSession session) {
            LoggingPooler.doLog(logger, "INFO", "NeoSmppServerHandler", "sessionDestroyed", false, false, true, "",
                    "Session destroyed: " + session.toString(), null);

            // print out final stats
            if (session.hasCounters()) {
                logger.info(" final session rx-submitSM: {}", session.getCounters().getRxSubmitSM());
            }

            // Save to smpp bind attempt log
            smsTransactionOperationPooler.saveSMPPBindAttempt(String.valueOf(sessionId), LocalDateTime.now(), session.getConfiguration().getSystemId(),
                    session.getConfiguration().getHost(), "BINDING DESTROY", "SUCCESS - SESSION DESTROYED");

            // make sure it's really shutdown
            session.destroy();
        }
    }
}
