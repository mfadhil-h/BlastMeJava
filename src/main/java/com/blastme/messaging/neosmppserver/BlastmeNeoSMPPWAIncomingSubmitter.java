package com.blastme.messaging.neosmppserver;

import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.SMPPDLRPooler;
import com.blastme.messaging.toolpooler.UserAPISMPPSMSPooler;
import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.commons.util.windowing.WindowFuture;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppServerSession;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.pdu.DeliverSm;
import com.cloudhopper.smpp.pdu.DeliverSmResp;
import com.cloudhopper.smpp.pdu.PduRequest;
import com.cloudhopper.smpp.pdu.PduResponse;
import com.cloudhopper.smpp.tlv.Tlv;
import com.cloudhopper.smpp.type.Address;
import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map.Entry;

public class BlastmeNeoSMPPWAIncomingSubmitter implements Runnable {
    private Logger logger;

    private static final String MessageClientQueue = "SMPP_WA_CLIENT_MESSAGE_NEO";
    private static final int clientMessageTPS = 100;

    private RabbitMQPooler rabbitMqPooler;
    private Channel channel;

    public BlastmeNeoSMPPWAIncomingSubmitter() {
        try {
            // Setup logger
            logger = LogManager.getLogger("SMPP_SERVER");

            rabbitMqPooler = new RabbitMQPooler();

            Connection connection = rabbitMqPooler.getConnection();
            channel = rabbitMqPooler.getChannel(connection);

            channel.queueDeclare(MessageClientQueue, true, false, false, null);

            // Initiate SMPPDLRPooler
            new SMPPDLRPooler();

            LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPWAIncomingSubmitter", "BlastMeNeoSMPPWAIncomingSubmitter", false, false, true, "",
                    "Successfully initialize channel rabbitMq to queueName " + MessageClientQueue, null);
        } catch (IOException e) {
            LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPWAIncomingSubmitter", "BlastMeNeoSMPPWAIncomingSubmitter", true, false, true, "",
                    "Failed to initialize channel rabbitMq to queueName " + MessageClientQueue, e);
        }
    }

    private void sendClientMessageRequestPdu(SmppSession session, String messageId, String sourceAddress, String destAddress, String message, String encoding) {
        try {
            byte[] textBytes = CharsetUtil.encode(message, CharsetUtil.CHARSET_GSM);

            if (encoding.equals("UCS2")) {
                textBytes = CharsetUtil.encode(message, CharsetUtil.CHARSET_UCS_2);
            }

            DeliverSm deliver = new DeliverSm();

            deliver.setSourceAddress(new Address((byte) 0x03, (byte) 0x00, sourceAddress));
            deliver.setDestAddress(new Address((byte) 0x01, (byte) 0x01, destAddress));

            // Utk message > 255 pake message payload
            if (textBytes != null && textBytes.length > 255) {
                deliver.addOptionalParameter(new Tlv(SmppConstants.TAG_MESSAGE_PAYLOAD, textBytes, "whatsapp_payload"));
            } else {
                deliver.setShortMessage(textBytes);
            }

            @SuppressWarnings("rawtypes")
            WindowFuture<Integer, PduRequest, PduResponse> future = session.sendRequestPdu(deliver, 10000, false);
            if (!future.await()) {
                LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPWAIncomingSubmitter", "sendClientMessageRequestPdu", false, false, true, "",
                        "Failed to receive deliver_sm_resp within specified time", null);
            } else if (future.isSuccess()) {
                DeliverSmResp deliverSmResp = (DeliverSmResp) future.getResponse();
                LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPWAIncomingSubmitter", "sendClientMessageRequestPdu", false, false, true, "",
                        "deliver_sm_resp: commandStatus [" + deliverSmResp.getCommandStatus() + "=" + deliverSmResp.getResultMessage() + "]", null);
            } else {
                LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPWAIncomingSubmitter", "sendClientMessageRequestPdu", false, false, true, "",
                        "Failed to properly receive deliver_sm_resp: " + future.getCause(), null);
            }

        } catch (Exception e) {
            LoggingPooler.doLog(logger, "INFO", "BlastMeSMPPServer - ClientMessageSubmmiter", "sendClientMessageRequestPdu", true, false, true, "",
                    "Failed to send message to client. Error occured.", e);
        }
    }

    private void proccessClientWABAMessage(String queueMessage) {
        LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPWAIncomingSubmitter", "proccessClientWABA", false, false, false, "",
                "Processing queue WABA to SMPPClient with message: " + queueMessage, null);

        try {
            JSONObject jsonMessage = new JSONObject(queueMessage);

            String clientId = jsonMessage.getString("clientId");
            String waMessageId = jsonMessage.getString("message_id");
            String contactId = jsonMessage.getString("contact_id");
            String waId = "16502636146";

            JSONObject jsonDetail = UserAPISMPPSMSPooler.jsonClientIdToAccess.getJSONObject(clientId);
            LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPWAIncomingSubmitter", "proccessClientWABAMessage", false, false, false, waMessageId,
                    "jsonDetail: " + jsonDetail.toString(), null);

            if (jsonDetail == null || jsonDetail.length() <= 0) {
                // Do nothing here, gak tahu mau dikirim ke session yang mana
                LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPWAIncomingSubmitter", "proccessClientWABAMessage", false, false, false, waMessageId,
                        "SysID is not found for clientID: " + clientId + ". DO NOTHING!", null);
            } else {
                // Process
                String theSysId = jsonDetail.getString("sysId");
                LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPWAIncomingSubmitter", "proccessClientWABAMessage", false, false, false, waMessageId,
                        "theSysId: " + theSysId, null);

                // Get sysSessionId
                SmppServerSession theSession = null;
                int counter = 0;
                for (Entry<String, SmppServerSession> entry : BlastMeNeoSMPPServer.mapSession.entrySet()) {
                    if (entry.getKey().startsWith(theSysId)) {
                        theSession = entry.getValue();

                        counter = counter + 1;
                    }
                }
                LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPWAIncomingSubmitter", "proccessClientWABAMessage", false, false, false, waMessageId,
                        "counter: " + counter, null);

                LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPWAIncomingSubmitter", "proccessClientWABAMessage", false, false, false, waMessageId,
                        "SMPPSession: " + theSession.toString(), null);

                LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPWAIncomingSubmitter", "proccessClientWABAMessage", false, false, false, waMessageId,
                        "clientId: " + clientId + " -> SMPP sysId: " + theSysId + " -> found matching SMPPSession: " + theSession.getConfiguration().getName(), null);

                // Send to client using the session
                sendClientMessageRequestPdu(theSession, waMessageId, contactId, waId, queueMessage, "UTF-8");
                LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPWAIncomingSubmitter", "proccessClientWABAMessage", false, false, false, waMessageId,
                        "Sending MO Message with session: " + theSession.getConfiguration().getName() + ". MO Message: " + queueMessage, null);
            }
        } catch (Exception e) {
            LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPWAIncomingSubmitter", "proccessClientWABAMessage", true, false, true, "",
                    "Failed to process the DLR message.", e);
            SMPPDLRPooler.sendSMPPPreviouslyFailedDLR("", queueMessage);
        }
    }

    private void readClientWAMOMessageQueue() {
        try {
            LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoSMPPWAIncomingSubmitter", "readClientMOMessageQueue", false, false, true, "",
                    "Reading queue " + MessageClientQueue + " for MO message to client...", null);

            // Guava rateLimiter
            RateLimiter rateLimiter = RateLimiter.create(clientMessageTPS);

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    String message = new String(body, StandardCharsets.UTF_8);

                    try {
                        LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPWAIncomingSubmitter", "readClientMOMessageQueue", false, false, false, "",
                                "SMPP_WA_CLIENT_MESSAGE -> Receive message: " + message, null);

                        // Limit the speed
                        rateLimiter.acquire();

                        // Send to client via SMPPServer
                        proccessClientWABAMessage(message);
                    } finally {
                        LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPWAIncomingSubmitter", "readClientMOMessageQueue", false, false, false, "",
                                "Done processing message: " + message, null);

                        channel.basicAck(envelope.getDeliveryTag(), false);
                    }
                }
            };

            boolean autoAck = false; // If not finally exectued well, no ack to rabbitmq, message not gone
            channel.basicConsume(MessageClientQueue, autoAck, consumer);
        } catch (Exception e) {
            LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPWAIncomingSubmitter", "readClientMOMessageQueue", true, false, false, "",
                    "Failed to access queue " + MessageClientQueue, e);
        }
    }

    @Override
    public void run() {
        try {
            LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPWAIncomingSubmitter",
                    "RUN", false, false, true, "", "Starting WA SMPP MO Submitter ...", null);

            readClientWAMOMessageQueue();
        } catch (Exception e) {
            LoggingPooler.doLog(logger, "INFO", "BlastMeNeoSMPPWAIncomingSubmitter",
                    "RUN", true, false, true, "", "Error to run WA Message MO Submitter", e);
        }
    }
}
