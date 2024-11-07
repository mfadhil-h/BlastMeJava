package com.blastme.messaging.toolpooler;

import com.blastme.messaging.configuration.Configuration;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.util.Iterator;

public class TransactionRedisPooler {
    private static Logger logger;

    private final RedisPooler redisPooler;
    private final RedisCommands<String, String> redisCommand;
    private static final int redisExpiry = 2 * 24 * 60 * 60; // 7 days from last operation

    public TransactionRedisPooler() {
        // Load Configuration
        new Configuration();
//		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
//		File file = new File(Configuration.getLogConfigPath());
//		context.setConfigLocation(file.toURI());

        // Setup logger
        logger = LogManager.getLogger("POOLER");

        // Initiate LoggingPooler
        new LoggingPooler();

        // Initiate RedisPooler
        redisPooler = new RedisPooler();
        redisCommand = redisPooler.redisInitiateConnection();

        LoggingPooler.doLog(logger, "INFO", "TransactionRedisPooler", "TransactionRedisPooler", false, false, true, "",
                "Module TransactionRedisPooler is initiated.", null);
    }

    public void updateTrxRedisData(String messageId, JSONObject jsonNewAppendedData) {
        String redisKey = "trxdata-" + messageId.trim();

        // Get existing redis trxdata
        String redisVal = redisPooler.redisGet(redisCommand, redisKey);

        JSONObject jsonRedis = new JSONObject();
        if (redisVal != null && redisVal.trim().length() > 0) {
            // Existing redisVal is set
            LoggingPooler.doLog(logger, "INFO", "TransactionRedisPooler", "updateTrxRedisData", false, false, true, "",
                    redisKey + " - already has data. Existing data: " + redisVal.trim(), null);

            jsonRedis = new JSONObject(redisVal);

            // Append jsonNewAppendedData into jsonRedis
            LoggingPooler.doLog(logger, "INFO", "TransactionRedisPooler", "updateTrxRedisData", false, false, true, "",
                    redisKey + " - appending new trx data: " + jsonNewAppendedData.toString(), null);
            Iterator<String> keys = jsonNewAppendedData.keys();

            while (keys.hasNext()) {
                String key = keys.next();

                jsonRedis.put(key, jsonNewAppendedData.get(key));
            }
            LoggingPooler.doLog(logger, "INFO", "TransactionRedisPooler", "updateTrxRedisData", false, false, true, "",
                    redisKey + " - new trx data become " + jsonRedis, null);
        } else {
            // Existing redisVal is NOT set
            LoggingPooler.doLog(logger, "INFO", "TransactionRedisPooler", "updateTrxRedisData", false, false, true, "",
                    redisKey + " - NEW TRX DATA.", null);
            jsonRedis = jsonNewAppendedData;
        }

        LoggingPooler.doLog(logger, "INFO", "TransactionRedisPooler", "updateTrxRedisData", false, false, true, "",
                redisKey + " - final jsonRedis: " + jsonRedis.toString(), null);

        // Update redis
        redisPooler.redisSetWithExpiry(redisCommand, redisKey, jsonRedis.toString(), redisExpiry);
    }

    public String getTrxRedisData(String messageId) {
        String trxData = "";

        trxData = redisPooler.redisGet(redisCommand, "trxdata-" + messageId.trim());

        return trxData;
    }
}
