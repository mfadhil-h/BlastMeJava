package com.blastme.messaging.toolpooler;

import com.blastme.messaging.configuration.Configuration;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class TransactionStatusPooler {
    private static Logger logger;

    Connection connection = null;
    ResultSet resultSet = null;

    public static JSONObject jsonTrxStatus;

    public TransactionStatusPooler() {
        // Load Configuration
        new Configuration();
//		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
//		File file = new File(Configuration.getLogConfigPath());
//		context.setConfigLocation(file.toURI());

        // Setup logger
        logger = LogManager.getLogger("POOLER");

        // Initiate LoggingPooler
        new LoggingPooler();

        // Initiate connection to Postgresql
        try {
            BasicDataSource bds = DataSource.getInstance().getBds();
            connection = bds.getConnection();
            LoggingPooler.doLog(logger, "INFO", "TransactionStatusPooler", "TransactionStatusPooler", false, false, false, "",
                    "Database connection is load and initiated.", null);
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "TransactionStatusPooler", "TransactionStatusPooler", true, false, false, "",
                    "Failed to load connection to database server. Error occured.", e);
        }

        loadJsonTrxStatus();

        LoggingPooler.doLog(logger, "INFO", "TransactionStatusPooler", "TransactionStatusPooler", false, false, false, "",
                "Module TransactionStatusPooler is initiated and ready to serve.", null);
    }

    private void loadJsonTrxStatus() {
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        jsonTrxStatus = new JSONObject();

        try {
            statement = connection
                    .prepareStatement("select status_code, description, is_deducting_bill from transaction_status");
            resultSet = statement.executeQuery();

            while (resultSet.next()) {
                JSONObject jsonStatus = new JSONObject();
                jsonStatus.put("statusCode", resultSet.getString("status_code"));
                jsonStatus.put("description", resultSet.getString("description"));
                jsonStatus.put("isDeducting", resultSet.getBoolean("is_deducting_bill"));

                jsonTrxStatus.put(resultSet.getString("status_code"), jsonStatus);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "TransactionStatusPooler", "loadJsonTrxStatus", true, false, false, "",
                    "Failed to intiate jsonSenderIdSMSProperty. Error occured.", e);
        } finally {
            try {
                if (resultSet != null)
                    resultSet.close();
                if (statement != null)
                    statement.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                LoggingPooler.doLog(logger, "DEBUG", "TransactionStatusPooler", "loadJsonTrxStatus", true, false, false, "",
                        "Failed to close query statement.", e);
            }
        }
    }

    public boolean isBillable(String statusCode) {
        if (jsonTrxStatus.has(statusCode.trim())) {
            JSONObject jsonDetail = jsonTrxStatus.getJSONObject(statusCode.trim());

            return jsonDetail.getBoolean("isDeducting");
        } else {
            return true; // Default will deducting.
        }
    }
}
