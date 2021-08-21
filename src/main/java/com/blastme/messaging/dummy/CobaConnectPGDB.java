package com.blastme.messaging.dummy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.dbcp2.BasicDataSource;

import com.blastme.messaging.toolpooler.DataSource;

public class CobaConnectPGDB {

	public static void main(String[] args) {
		Connection connection = null;
		PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            BasicDataSource bds = DataSource.getInstance().getBds();
            connection = bds.getConnection();
            statement = connection
                    .prepareStatement("select a_number, b_number, message, message_length, no_of_sms from autogenbnumberuploaderdetail");
            resultSet = statement.executeQuery();
            System.out
                    .println("a_number, b_number, message, message_length, no_of_sms");
            while (resultSet.next()) {
                System.out.println(resultSet.getString("a_number") + ", "
                        + resultSet.getString("b_number") + " "
                        + resultSet.getString("message") + ", "
                        + resultSet.getInt("message_length") + ", "
                        + resultSet.getInt("no_of_sms"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet != null)
                    resultSet.close();
                if (statement != null)
                    statement.close();
                if (connection != null)
                    connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
	}

}
