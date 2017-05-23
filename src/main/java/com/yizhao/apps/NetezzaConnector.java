package com.yizhao.apps;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author YI ZHAO
 */
public class NetezzaConnector {
    private static final String NETEZZA_DB_DRIVER = "org.netezza.Driver";
    private static final String DB_CONNECTION = "jdbc:netezza://nz-vip-nym1:5480/opinmind_dev";
    private static final String DB_USER = "opinmind_dev_admin";
    private static final String DB_PASSWORD = "29JWmn2e";

    public static void dataToCsv(String table, String csvFileOutputPath, String partition) throws SQLException {
        Connection dbConnection = null;
        Statement statement = null;

        String selectTableSQL = null;

        if (partition == null) {
            selectTableSQL = "create external table \'" + csvFileOutputPath + "\'" +
                    "\n" +
                    "using (delim '|' escapechar '\\' remoteSource 'JDBC')" +
                    "\n" +
                    "as select * from " + table;
        } else {
            selectTableSQL = "create external table \'" + csvFileOutputPath + "\'" +
                    "\n" +
                    "using (delim '|' escapechar '\\' remoteSource 'JDBC')" +
                    "\n" +
                    "as select * from " + table +
                    "\n" +
                    "WHERE MOD(" + table + ".EVENT_ID, 10)=" + partition +
                    "\n" +
                    "ORDER BY " + table + ".EVENT_ID";
        }

        try {
            dbConnection = getDBConnection();
            statement = dbConnection.createStatement();

            System.out.println("execute query: \n" + selectTableSQL);

            // execute select SQL stetement
            statement.execute(selectTableSQL);
        } catch (SQLException e) {
            System.out.println("Exception in dataToCsv:" + e.getMessage());

        } finally {
            if (statement != null) {
                statement.close();
            }

            if (dbConnection != null) {
                dbConnection.close();
            }
        }
    }

    public static Connection getDBConnection() {
        Connection dbConnection = null;

        try {
            Class.forName(NETEZZA_DB_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Exception in getDBConnection:" + e.getMessage());
        }

        try {
            dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER,
                    DB_PASSWORD);
            return dbConnection;
        } catch (SQLException e) {
            System.out.println("Exception in getDBConnection:" + e.getMessage());
        }

        return dbConnection;

    }


    public static void connectionTesting() {
        try {
            Class.forName(NETEZZA_DB_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        Connection connection = null;

        try {
            connection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
//            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/marketplace","om", "N3wQA3ra.");

        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;
        }

        if (connection != null) {
            System.out.println("You made it, take control your Netezza database now!");
        } else {
            System.out.println("Failed to make connection to Netezza database!");
        }
    }
}
