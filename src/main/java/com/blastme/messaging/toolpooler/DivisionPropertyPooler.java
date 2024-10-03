package com.blastme.messaging.toolpooler;

import com.blastme.messaging.configuration.Configuration;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class DivisionPropertyPooler {
    private static Logger logger;
    private static JSONObject jsonDivisionProperty;

    private Connection connection = null;
    private PreparedStatement statement = null;
    private ResultSet resultSet = null;

    public DivisionPropertyPooler() {
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
            LoggingPooler.doLog(logger, "INFO", "DivisionPropertyPooler", "DivisionPropertyPooler", false, false, false, "",
                    "Database connection is load and initiated.", null);
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "DivisionPropertyPooler", "DivisionPropertyPooler", true, false, false, "",
                    "Failed to load connection to database server. Error occured.", e);
        }

        // Initate JSONDivisionProperty
        jsonDivisionProperty = new JSONObject();
        initiateJSONDivisionProperty();

        LoggingPooler.doLog(logger, "INFO", "DivisionPropertyPooler", "DivisionPropertyPooler", false, false, false, "",
                "Module DivisionPropertyPooler is initiated and ready to serve. jsonDivisionProperty: " + jsonDivisionProperty.toString(), null);

    }

    public void initiateJSONDivisionProperty() {
        try {
            statement = connection
                    .prepareStatement("select division_id, client_id, division_country, division_name from division where is_active = true");
            resultSet = statement.executeQuery();

            while (resultSet.next()) {
                JSONObject jsonDetail = new JSONObject();

                jsonDetail.put("clientId", resultSet.getString("client_id"));
                jsonDetail.put("divisionCountry", resultSet.getString("division_country"));
                jsonDetail.put("divisionName", resultSet.getString("division_name"));

                jsonDivisionProperty.put(resultSet.getString("division_id"), jsonDetail);
            }

            LoggingPooler.doLog(logger, "DEBUG", "DivisionPropertyPooler", "initiateJSONDivisionProperty", false, false, false, "",
                    "jsonDivisionProperty: " + jsonDivisionProperty.toString(), null);
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "DivisionPropertyPooler", "initiateJSONDivisionProperty", true, false, false, "",
                    "Failed to intiate sonDivisionProperty. Error occured.", e);
        } finally {
            try {
                if (resultSet != null)
                    resultSet.close();
                if (statement != null)
                    statement.close();
            } catch (Exception e) {
                e.printStackTrace();
                LoggingPooler.doLog(logger, "DEBUG", "DivisionPropertyPooler", "initiateJSONDivisionProperty", true, false, false, "",
                        "Failed to close query statement.", e);
            }
        }
    }

    public String getClientId(String divisionId) {
        String clientId = "";

        try {
            if (jsonDivisionProperty.has(divisionId.trim())) {
                clientId = jsonDivisionProperty.getJSONObject(divisionId.trim()).getString("clientId");
            }
        } catch (Exception e) {
            LoggingPooler.doLog(logger, "DEBUG", "DivisionPropertyPooler", "getClientId", true, false, false, "",
                    "Failed to get the clientId.", e);
        }

        return clientId;
    }

}
