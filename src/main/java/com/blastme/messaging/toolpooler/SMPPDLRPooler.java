package com.blastme.messaging.toolpooler;

import com.blastme.messaging.configuration.Configuration;
import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.pdu.DeliverSm;
import com.cloudhopper.smpp.type.Address;
import com.cloudhopper.smpp.util.DeliveryReceipt;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class SMPPDLRPooler {
    private static Logger logger;

    private RabbitMQPooler rabbitMqPooler;
    private com.rabbitmq.client.Connection connDLR;
    private com.rabbitmq.client.Connection connDLRDelay;
    private static Channel channelDLR;
    private static Channel channelDRLDelay;

    private static final String DLRQueueName = "SMPP_DLR";
    private static final String DLRDelayExchange = "DELAY_DLR";

    private BasicDataSource bds;
    private Connection connectionDB;
    private static PreparedStatement statementUpdateTrxData;
    private static PreparedStatement statementUpdateTrxVendor;
    private static PreparedStatement statementInsertTrxDLR;

    // connection, statement di initiate di setiap fungsi untuk concurrent thread safe! Jangan diinitiate di awal
    public SMPPDLRPooler() {
        // Load Configuration
        new Configuration();
//		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
//		File file = new File(Configuration.getLogConfigPath());
//		context.setConfigLocation(file.toURI());

        // Setup logger
        //logger = LogManager.getLogger("POOLER");
        logger = LogManager.getLogger("SMPP_SERVER");

        // Initiate LoggingPooler
        new LoggingPooler();

        try {
            bds = DataSource.getInstance().getBds();
            connectionDB = bds.getConnection();
            statementUpdateTrxData = connectionDB.prepareStatement("update transaction_sms set status_code=? where message_id=?");
            statementUpdateTrxVendor = connectionDB.prepareStatement("update transaction_sms_vendor set vendor_callback_date_time=?, "
                    + "vendor_callback=? where message_id=?");
            statementInsertTrxDLR = connectionDB.prepareStatement("INSERT INTO transaction_sms_dlr(message_id, client_id, "
                    + "dlr_date_time, dlr_body, dlr_status, dlr_push_to, dlr_client_push_response, dlr_log_note) VALUES "
                    + "(?, ?, ?, ?, ?, ?, ?, ?)");

            rabbitMqPooler = new RabbitMQPooler();
            connDLR = rabbitMqPooler.getConnection();
            connDLRDelay = rabbitMqPooler.getConnection();

            channelDLR = rabbitMqPooler.getChannel(connDLR);
            channelDRLDelay = rabbitMqPooler.getChannel(connDLRDelay);

            LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "SMPPDLRPooler", false, false, false, "",
                    "Module SMPPDLRPooler is initiated and ready to serve.", null);
        } catch (Exception e) {
            e.printStackTrace();

            LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "SMPPDLRPooler", true, false, false, "",
                    "Failed to initiate SMPPDLRPooler. Error occured.", e);
        }
    }

    private static void updateTrxData(String messageId, String statusCode, LocalDateTime callbackDateTime, String callbackBody) {

        try {
            statementUpdateTrxData.setString(1, statusCode);
            statementUpdateTrxData.setString(2, messageId);

            int impactedRowTrx = statementUpdateTrxData.executeUpdate();

            if (impactedRowTrx > 0) {
                LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "updateTrxData", false, false, false, messageId,
                        "Updating transaction data is SUCCESS! Impacted row: " + impactedRowTrx, null);
            } else {
                LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "updateTrxData", false, false, false, messageId,
                        "Updating transaction data is FAILED! Impacted row: " + impactedRowTrx, null);
            }

            // Update transaction_sms_vendor -
            statementUpdateTrxVendor.setObject(1, LocalDateTime.now());
            statementUpdateTrxVendor.setString(2, callbackBody);
            statementUpdateTrxVendor.setString(3, messageId);
            System.out.println("statementUpdateTrxVendor: " + statementUpdateTrxVendor.toString());

            int impactedRowVendor = statementUpdateTrxVendor.executeUpdate();

            if (impactedRowVendor > 0) {
                LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "updateTrxData", false, false, false, messageId,
                        "Updating transaction vendor data is SUCCESS! Impacted row: " + impactedRowVendor, null);
            } else {
                LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "updateTrxData", false, false, false, messageId,
                        "Updating transaction vendor data is FAILED! Impacted row: " + impactedRowVendor, null);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "updateTrxData", true, false, false, "",
                    "Failed to update transaction data. Error occured.", e);
        }
    }

    public static void sendSMPPDLR(String messageId, String msisdn, String clientSenderId, String message, String statusCode, LocalDateTime callbackDateTime, String callbackBody, String sysSessionId) {
        try {
            JSONObject jsonDLR = new JSONObject();
            jsonDLR.put("messageId", messageId);
            jsonDLR.put("msisdn", msisdn);
            jsonDLR.put("clientSenderId", clientSenderId);
            jsonDLR.put("message", message);
            jsonDLR.put("errorCode", statusCode);
            jsonDLR.put("sysSessionId", sysSessionId);
            LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "SMPPDLRPooler", false, false, false, messageId,
                    "Sending DLR to SMPPServer, jsonDLR: " + jsonDLR, null);

            // Update db
            updateTrxData(messageId, statusCode, callbackDateTime, callbackBody);
            LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "SMPPDLRPooler", false, false, false, messageId,
                    "jsonDLR: " + jsonDLR + " updated to database SUCCESSfully!", null);

            channelDLR.queueDeclare(DLRQueueName, true, false, false, null);
            channelDLR.basicPublish("", DLRQueueName, MessageProperties.PERSISTENT_TEXT_PLAIN, jsonDLR.toString().getBytes());
            LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "SMPPDLRPooler", false, false, false, messageId,
                    "jsonDLR: " + jsonDLR + " published to queue " + DLRQueueName + " SUCCESSfully!", null);
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "SMPPDLRPooler", true, false, false, messageId,
                    "Failed to send DLR. Error occured.", e);
        }
    }

    public void sendReceipt(String messageId, String msisdn, String theSMS, String clientId, String clientSenderId, String status, SmppSession theSession) {
        try {
            DeliveryReceipt dlr = new DeliveryReceipt();
            dlr.setMessageId(messageId);
            if (status.equals("000")) {
                dlr.setSubmitCount(1);
                dlr.setDeliveredCount(1);
            } else if (status.equals("001") || status.equals("002") || status.equals("003")) {
                dlr.setDeliveredCount(0);
                dlr.setSubmitCount(1);
            } else {
                dlr.setDeliveredCount(0);
            }

            dlr.setSubmitDate(new DateTime());
            dlr.setDoneDate(new DateTime());

            byte deliveryState = SmppConstants.STATE_DELIVERED;
            if (status.equals("000")) {
                deliveryState = SmppConstants.STATE_DELIVERED;
            } else if (status.equals("001") || status.equals("002") || status.equals("003")) {
                deliveryState = SmppConstants.STATE_ACCEPTED;
            } else {
                deliveryState = SmppConstants.STATE_UNDELIVERABLE;
            }
            dlr.setState(deliveryState);

            // change errorCode to int
            int iErrorCode = 0;
            if (status.equals("000")) {
                iErrorCode = 0;
            } else if (status.equals("001") || status.equals("002") || status.equals("003")) {
                iErrorCode = 1;
            } else {
                iErrorCode = Integer.parseInt(status);
            }

            dlr.setErrorCode(iErrorCode);
            dlr.setText(theSMS);

            String receipt0 = dlr.toShortMessage();
            LoggingPooler.doLog(logger, "DEBUG", "SMPPDLRPooler", "sendReceipt", false, false, false, messageId,
                    "messageId: " + messageId + ", the DLR: " + receipt0, null);

            // Source address is the msidn - dibalik dari sms masuk
            Address mtDestinationAddress = new Address((byte) 0x03, (byte) 0x00, clientSenderId);
            Address mtSourceAddress = new Address((byte) 0x01, (byte) 0x01, msisdn);

            DeliverSm deliver = new DeliverSm();
            deliver.setEsmClass(SmppConstants.ESM_CLASS_MT_SMSC_DELIVERY_RECEIPT);
            deliver.setSourceAddress(mtSourceAddress);
            deliver.setDestAddress(mtDestinationAddress);
            if (receipt0.trim().length() > 0) {
                deliver.setShortMessage(receipt0.getBytes());
                System.out.println("receipt0 length > 0. Content: " + receipt0);
                System.out.println("deliver: " + CharsetUtil.decode(deliver.getShortMessage(), CharsetUtil.CHARSET_GSM));
            }

            theSession.sendRequestPdu(deliver, 10000, false);
            LoggingPooler.doLog(logger, "DEBUG", "SMPPDLRPooler", "sendReceipt", false, false, false, messageId,
                    "Sending DLR via sessionId " + theSession + " is OK.", null);

            // Save to DB transaction_sms_dlr
            LocalDateTime now = LocalDateTime.now();
            updateDLRTable(messageId, clientId, now, receipt0, status, CharsetUtil.decode(deliver.getShortMessage(), CharsetUtil.CHARSET_GSM), "", "");
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "DEBUG", "SMPPDLRPooler", "sendReceipt", true, false, false, messageId,
                    "Sending DLR via sessionId " + theSession.toString() + " is FAILED. Error occured.", e);
        }
    }

    public void sendSMPPFakeDLR(String messageId, String msisdn, String clientSenderId, String message, String statusCode, LocalDateTime callbackDateTime, String callbackBody, String sysSessionId) {
        try {
            int delay = 2;

            Random r = new Random();

            delay = delay + r.nextInt(10);
            LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "SMPPDLRPooler", false, false, false, messageId,
                    "Set delay for DLR: " + delay + " seconds.", null);

            JSONObject jsonDLR = new JSONObject();
            jsonDLR.put("messageId", messageId);
            jsonDLR.put("msisdn", msisdn);
            jsonDLR.put("clientSenderId", clientSenderId);
            jsonDLR.put("message", message);
            jsonDLR.put("errorCode", statusCode);
            jsonDLR.put("sysSessionId", sysSessionId);
            LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "SMPPDLRPooler", false, false, false, messageId,
                    "Sending DLR to SMPPServer, jsonDLR: " + jsonDLR, null);

            // Update db
            updateTrxData(messageId, statusCode, callbackDateTime, callbackBody);
            LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "SMPPDLRPooler", false, false, false, messageId,
                    "jsonDLR: " + jsonDLR + " updated to database SUCCESSfully!", null);

            // Send to message with delay
            Map<String, Object> headers = new HashMap<String, Object>();
            headers.put("x-delay", delay * 1000);
            AMQP.BasicProperties.Builder props = new AMQP.BasicProperties.Builder().headers(headers);

            //RabbitMQPooler.channel.queueDeclare(DLRQueueName, true, false, false, null);
            //RabbitMQPooler.channel.basicPublish("", DLRQueueName, MessageProperties.PERSISTENT_TEXT_PLAIN, jsonDLR.toString().getBytes());

            Map<String, Object> args = new HashMap<String, Object>();
            args.put("x-delayed-type", "direct");
            channelDRLDelay.exchangeDeclare(DLRDelayExchange, "x-delayed-message", true, false, args);
            channelDRLDelay.queueBind(DLRQueueName, DLRDelayExchange, "");

            channelDRLDelay.basicPublish(DLRDelayExchange, "", props.build(), jsonDLR.toString().getBytes());
            LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "SMPPDLRPooler", false, false, false, messageId,
                    "jsonDLR: " + jsonDLR + " published to queue " + DLRQueueName + " with delay " + delay + " seconds - SUCCESSfully!", null);
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "SMPPDLRPooler", true, false, false, messageId,
                    "Failed to send DLR. Error occured.", e);
        }
    }

    public static void sendSMPPPreviouslyFailedDLR(String messageId, String requeueMessage) {
        try {
            int delay = 60;  // delay 1 menit

            // Send to message with delay
            Map<String, Object> headers = new HashMap<String, Object>();
            headers.put("x-delay", delay * 1000);
            AMQP.BasicProperties.Builder props = new AMQP.BasicProperties.Builder().headers(headers);

            //RabbitMQPooler.channel.queueDeclare(DLRQueueName, true, false, false, null);
            //RabbitMQPooler.channel.basicPublish("", DLRQueueName, MessageProperties.PERSISTENT_TEXT_PLAIN, jsonDLR.toString().getBytes());

            Map<String, Object> args = new HashMap<String, Object>();
            args.put("x-delayed-type", "direct");
            channelDRLDelay.exchangeDeclare(DLRDelayExchange, "x-delayed-message", true, false, args);
            channelDRLDelay.queueBind(DLRQueueName, DLRDelayExchange, "");

            channelDRLDelay.basicPublish(DLRDelayExchange, "", props.build(), requeueMessage.getBytes());
            LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "SMPPDLRPooler", false, false, false, messageId,
                    "requeue DLR message: " + requeueMessage + " published to queue " + DLRQueueName + " with delay " + delay + " seconds - SUCCESSfully!", null);
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "SMPPDLRPooler", true, false, false, messageId,
                    "Failed to send DLR. Error occured.", e);
        }
    }

    public static void updateDLRTable(String messageId, String clientId, LocalDateTime dlrDateTime, String dlrBody, String dlrStatus, String dlrPushTo, String dlrClientResponse, String dlrLogNote) {
        try {
            statementInsertTrxDLR.setString(1, messageId);
            statementInsertTrxDLR.setString(2, clientId);
            statementInsertTrxDLR.setObject(3, dlrDateTime);
            statementInsertTrxDLR.setString(4, dlrBody);
            statementInsertTrxDLR.setString(5, dlrStatus);
            statementInsertTrxDLR.setString(6, dlrPushTo);
            statementInsertTrxDLR.setString(7, dlrClientResponse);
            statementInsertTrxDLR.setString(8, dlrLogNote);

            int insertDLR = statementInsertTrxDLR.executeUpdate();

            if (insertDLR > 0) {
                LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "updateDLRTable", false, false, false, messageId,
                        "Inserting to DLR Table is success. Impacted row: " + insertDLR, null);
            } else {
                LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "updateDLRTable", false, false, false, messageId,
                        "Inserting to DLR Table is failed. Impacted row: " + insertDLR, null);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "SMPPDLRPooler", "updateDLRTable", true, false, false, messageId,
                    "Inserting to DLR Table is failed. Error occured.", e);
        }
    }
}
