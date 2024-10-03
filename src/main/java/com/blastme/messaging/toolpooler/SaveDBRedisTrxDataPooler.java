package com.blastme.messaging.toolpooler;

import com.blastme.messaging.configuration.Configuration;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class SaveDBRedisTrxDataPooler {
    private static Logger logger;

    // Redis
    private final RedisPooler redisPooler;
    private final RedisCommands<String, String> redisCommands;

    private Connection connection = null;

    // THIS MODULE IS CALLED by Last Function in the process (usually transceivers)
    public SaveDBRedisTrxDataPooler() {
        // Load Configuration
        new Configuration();
//		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
//		File file = new File(Configuration.getLogConfigPath());
//		context.setConfigLocation(file.toURI());

        // Setup logger
        logger = LogManager.getLogger("POOLER");

        // Initiate LoggingPooler
        new LoggingPooler();

        // Initiate redisPooler
        redisPooler = new RedisPooler();
        redisCommands = redisPooler.redisInitiateConnection();

        // Initiate connection to Postgresql
        try {
            BasicDataSource bds = DataSource.getInstance().getBds();
            connection = bds.getConnection();
            LoggingPooler.doLog(logger, "INFO", "SaveDBRedisTrxDataPooler", "SaveDBRedisTrxDataPooler", false, false, false, "",
                    "Database connection is load and initiated.", null);
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "SaveDBRedisTrxDataPooler", "SaveDBRedisTrxDataPooler", true, false, false, "",
                    "Failed to load connection to database server. Error occured.", e);
        }

        LoggingPooler.doLog(logger, "INFO", "SaveDBRedisTrxDataPooler", "SaveDBRedisTrxDataPooler", false, false, false, "",
                "Module SaveDBRedisTrxDataPooler is initiated and ready to serve.", null);
    }

    public void saveToTrxDBFromTrxRedisTransceiverStage(String messageId) {
        try {
            //SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHHmmssSSS");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMddHHmmssSSS");

            String redisKey = "trxdata-" + messageId.trim();
            String redisVal = redisPooler.redisGet(redisCommands, redisKey);

            LoggingPooler.doLog(logger, "DEBUG", "SaveDBRedisTrxDataPooler", "saveToTrxDBFromTrxRedisTransceiverStage", false, false, false, messageId,
                    "redisKey: " + redisKey + ", redisVal: " + redisVal, null);

            if (redisVal.trim().length() > 0) {
                JSONObject jsonRedis = new JSONObject(redisVal);

                // Field yang perlu di update di stage transceiver final ini:
                // - vendorId
                // - vendorHitDateTime
                // - vendorRequest
                // - vendorHitResponse
                // - vendorRespDateTime
                // - vendorMessageId
                // - deductionDateTime
                // - routerToTransceiverDateTime
                // - routerToDLRDateTime
                // - prevBalance
                // - deduction
                // - newBalance

                // Check if vendorId exists
                String vendorId = "";
                if (jsonRedis.has("vendorId")) {
                    vendorId = jsonRedis.getString("vendorId");
                    LoggingPooler.doLog(logger, "DEBUG", "SaveDBRedisTrxDataPooler", "saveToTrxDBFromTrxRedisTransceiverStage", false, false, false, messageId,
                            "vendorId exist: " + vendorId, null);
                }

                // Check if vendorHitDateTime exists
                LocalDateTime vendorHitDateTime = null;
                if (jsonRedis.has("vendorHitDateTime")) {
                    vendorHitDateTime = LocalDateTime.parse(jsonRedis.getString("vendorHitDateTime"), formatter);
                    LoggingPooler.doLog(logger, "DEBUG", "SaveDBRedisTrxDataPooler", "saveToTrxDBFromTrxRedisTransceiverStage", false, false, false, messageId,
                            "vendorHitDateTime exist: " + vendorHitDateTime.format(formatter), null);
                }

                // Check if vendorRequest exists
                String vendorRequest = "";
                if (jsonRedis.has("vendorRequest")) {
                    vendorRequest = jsonRedis.getString("vendorRequest");
                    LoggingPooler.doLog(logger, "DEBUG", "SaveDBRedisTrxDataPooler", "saveToTrxDBFromTrxRedisTransceiverStage", false, false, false, messageId,
                            "vendorRequest exist: " + vendorRequest, null);
                }

                // Check if vendorHitResponseDateTime exist
                LocalDateTime vendorRespDateTime = null;
                if (jsonRedis.has("vendorRespDateTime")) {
                    //vendorRespDateTime = sdf.parse(jsonRedis.getString("vendorRespDateTime"));
                    vendorRespDateTime = LocalDateTime.parse(jsonRedis.getString("vendorRespDateTime"), formatter);
                    LoggingPooler.doLog(logger, "DEBUG", "SaveDBRedisTrxDataPooler", "saveToTrxDBFromTrxRedisTransceiverStage", false, false, false, messageId,
                            "vendorRespDateTime exist: " + vendorRespDateTime.format(formatter), null);
                }

                // Check if vendorResponse exist
                String vendorResponse = "";
                if (jsonRedis.has("vendorResponse")) {
                    vendorResponse = jsonRedis.getString("vendorResponse");
                    LoggingPooler.doLog(logger, "DEBUG", "SaveDBRedisTrxDataPooler", "saveToTrxDBFromTrxRedisTransceiverStage", false, false, false, messageId,
                            "vendorResponse exist: " + vendorResponse, null);
                }

                // Check if vendorTrxId/vendorMessageId exist
                String vendorMessageId = "";
                if (jsonRedis.has("vendorMessageId")) {
                    vendorMessageId = jsonRedis.getString("vendorMessageId");
                    LoggingPooler.doLog(logger, "DEBUG", "SaveDBRedisTrxDataPooler", "saveToTrxDBFromTrxRedisTransceiverStage", false, false, false, messageId,
                            "vendorMessageId exist: " + vendorMessageId, null);
                }

                // Check if deductionDateTime exists
                LocalDateTime deductionDateTime = null;
                if (jsonRedis.has("deductionDateTime")) {
                    deductionDateTime = LocalDateTime.parse(jsonRedis.getString("deductionDateTime"), formatter);
                    LoggingPooler.doLog(logger, "DEBUG", "SaveDBRedisTrxDataPooler", "saveToTrxDBFromTrxRedisTransceiverStage", false, false, false, messageId,
                            "deductionDateTime exist: " + deductionDateTime.format(formatter), null);
                }

                // Check if routerToTransceiverDateTime exists
                LocalDateTime routerToTransceiverDateTime = null;
                if (jsonRedis.has("routerToTransceiverDateTime")) {
                    routerToTransceiverDateTime = LocalDateTime.parse(jsonRedis.getString("routerToTransceiverDateTime"), formatter);
                    LoggingPooler.doLog(logger, "DEBUG", "SaveDBRedisTrxDataPooler", "saveToTrxDBFromTrxRedisTransceiverStage", false, false, false, messageId,
                            "routerToTransceiverDateTime exist: " + routerToTransceiverDateTime.format(formatter), null);
                } else {
                    routerToTransceiverDateTime = LocalDateTime.now();
                }

                // Query: INSERT INTO public.transaction_sms_vendor(
                // message_id, vendor_id, vendor_hit_date_time, vendor_hit_request, vendor_hit_resp_date_time, vendor_hit_response,
                // vendor_message_id, vendor_callback_date_time, vendor_callback, router_to_transaceiver_date_time)
                // VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

                PreparedStatement statement = null;

                try {
                    statement = connection
                            .prepareStatement("INSERT INTO public.transaction_sms_vendor(message_id, vendor_id, vendor_hit_date_time, vendor_hit_request, " +
                                    "vendor_hit_resp_date_time, vendor_hit_response, vendor_message_id, router_to_transaceiver_date_time) " +
                                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");


                    statement.setString(1, messageId);
                    statement.setString(2, vendorId);
                    statement.setObject(3, vendorHitDateTime);
                    statement.setString(4, vendorRequest);
                    statement.setObject(5, vendorRespDateTime);
                    statement.setString(6, vendorResponse);
                    statement.setString(7, vendorMessageId);
                    statement.setObject(8, routerToTransceiverDateTime);

                    System.out.println("Update query: " + statement);

                    statement.executeUpdate();

                    LoggingPooler.doLog(logger, "INFO", "SaveDBRedisTrxDataPooler", "saveToTrxDBFromTrxRedisTransceiverStage", false, false, false, messageId,
                            "Successfully save data from redis trxData to Database.", null);
                } catch (Exception e) {
                    e.printStackTrace();
                    LoggingPooler.doLog(logger, "INFO", "SaveDBRedisTrxDataPooler", "saveToTrxDBFromTrxRedisTransceiverStage", true, false, false, messageId,
                            "Failed to save data from redis trxData to Database. Error occured.", e);
                } finally {
                    try {
                        if (statement != null)
                            statement.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                        LoggingPooler.doLog(logger, "DEBUG", "SaveDBRedisTrxDataPooler", "saveToTrxDBFromTrxRedisTransceiverStage", true, false, false, messageId,
                                "Failed to close query statement.", e);
                    }
                }
            } else {
                LoggingPooler.doLog(logger, "DEBUG", "SaveDBRedisTrxDataPooler", "saveToTrxDBFromTrxRedisTransceiverStage", false, false, false, messageId,
                        "FAILED to save transaction data from redis to database. DATA NOT FOUND IN REDIS.", null);
            }
        } catch (Exception e) {
            LoggingPooler.doLog(logger, "DEBUG", "SaveDBRedisTrxDataPooler", "saveToTrxDBFromTrxRedisTransceiverStage", true, false, false, messageId,
                    "Failed to save transaction data from redis to database. Error occured.", e);
        }
    }
}
