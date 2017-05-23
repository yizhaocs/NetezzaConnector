package com.yizhao.apps; /**
 * Created by yzhao on 5/22/17.
 */

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;


/**
 * @author YI ZHAO[yizhao.cs@gmail.com]
 *         <p>
 *         build it:
 *              mvn clean package
 *         <p>
 *         Run it:
 *              java -jar NetezzaConnector-jar-with-dependencies.jar eng759_backfill_apac /workplace/yzhao/eng759_backfill_apac.csv /workplace/yzhao/apac_fastrack.csv
 *              java -jar NetezzaConnector-jar-with-dependencies.jar eng759_backfill_apac /workplace/yzhao/eng759_backfill_apac.csv /workplace/yzhao/apac_fastrack.csv 0
 *              java -jar NetezzaConnector-jar-with-dependencies.jar eng759_backfill_apac /workplace/yzhao/eng759_backfill_apac.csv /workplace/yzhao/apac_fastrack.csv 1
 *              java -jar NetezzaConnector-jar-with-dependencies.jar eng759_backfill_apac /workplace/yzhao/eng759_backfill_apac.csv /workplace/yzhao/apac_fastrack.csv 2
 *              java -jar NetezzaConnector-jar-with-dependencies.jar eng759_backfill_apac /workplace/yzhao/eng759_backfill_apac.csv /workplace/yzhao/apac_fastrack.csv 9
 */
public class Main {
    private static final String NETEZZA_DB_DRIVER = "org.netezza.Driver";
    private static final String DB_CONNECTION = "jdbc:netezza://nz-vip-nym1:5480/opinmind_dev";
    private static final String DB_USER = "opinmind_dev_admin";
    private static final String DB_PASSWORD = "29JWmn2e";

    /**
     * @param argv
     */
    public static void main(String[] argv) {
        String table = null;
        String csvFileOutputPath = null;
        String fastrackFileOutputPath = null;
        String partition = null;
        try {
             table = argv[0];
             csvFileOutputPath = argv[1];
             fastrackFileOutputPath = argv[2];
             partition = argv[3];

            dataToCsv(table, csvFileOutputPath, partition);
        } catch (Exception e) {
            System.out.println("exception");
        }
        if(csvFileOutputPath != null){
           System.out.println("argument 2 is missing for csv file output path");
            return;
        }
        if(fastrackFileOutputPath != null){
            System.out.println("argument 3 is missing for fastrack file output path");
            return;
        }

        Map<String, FastrackFileDao> eventIdToData =  FastrackFileProcessor.execute(csvFileOutputPath);
        FastrackFileGenerator.execute(eventIdToData, fastrackFileOutputPath);
    }

    private static void dataToCsv(String table, String csvFileOutputPath, String partition) throws SQLException {
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
                    "ORDER BY " + table + ".EVENT_ID" + " LIMIT 1000";
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

    private static Connection getDBConnection() {
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


    private static void connectionTesting() {
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
