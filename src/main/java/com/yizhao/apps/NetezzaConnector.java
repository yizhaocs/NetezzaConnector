package com.yizhao.apps; /**
 * Created by yzhao on 5/22/17.
 */
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * @author YI ZHAO[yizhao.cs@gmail.com]
 *
 * build it:
 *      mvn clean package
 *
 * Run it:
 *      java -jar NetezzaConnector-jar-with-dependencies.jar eng759_backfill_apac /workplace/yzhao/eng759_backfill_apac.csv
 *      java -jar NetezzaConnector-jar-with-dependencies.jar eng759_backfill_apac /workplace/yzhao/eng759_backfill_apac.csv 0
 *      java -jar NetezzaConnector-jar-with-dependencies.jar eng759_backfill_apac /workplace/yzhao/eng759_backfill_apac.csv 1
 *      java -jar NetezzaConnector-jar-with-dependencies.jar eng759_backfill_apac /workplace/yzhao/eng759_backfill_apac.csv 2
 *      java -jar NetezzaConnector-jar-with-dependencies.jar eng759_backfill_apac /workplace/yzhao/eng759_backfill_apac.csv 9
 */
public class NetezzaConnector {
    private static final String NETEZZA_DB_DRIVER = "org.netezza.Driver";
    private static final String DB_CONNECTION = "jdbc:netezza://nz-vip-nym1:5480/opinmind_dev";
    private static final String DB_USER = "opinmind_dev_admin";
    private static final String DB_PASSWORD = "29JWmn2e";
    static boolean success = true;

    /**
     *
     * @param argv
     */
    public static void main(String[] argv) {
        try {
            selectRecordsFromDbUserTable(argv[0], argv[1], argv[2]);
        } catch (Exception e) {
            System.out.println("exception");
        }
    }

    private static void selectRecordsFromDbUserTable(String table, String outputFilePath, String partition) throws SQLException {

        Connection dbConnection = null;
        Statement statement = null;

        String selectTableSQL = null;

        if(partition == null){
             selectTableSQL = "create external table \'" + outputFilePath + "\' using (delim '|' escapechar '\\' remoteSource 'JDBC') as select * from " + table;
        }else{
             selectTableSQL = "create external table \'" + outputFilePath + "\' using (delim '|' escapechar '\\' remoteSource 'JDBC') as select * from " + table +
                    "\n" +
                    "WHERE MOD(" + table + ".EVENT_ID, 10)=" + partition +
                    "\n" +
                    "ORDER BY " + table + ".EVENT_ID";
        }



        try {
            dbConnection = getDBConnection();
            statement = dbConnection.createStatement();

            System.out.println(selectTableSQL);

            // execute select SQL stetement
            success = statement.execute(selectTableSQL);


        } catch (SQLException e) {
            success = false;
            System.out.println(e.getMessage());

        } finally {

            System.out.println("table dump success = " + success);
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
            success = false;
            System.out.println(e.getMessage());
        }

        try {
            dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER,
                    DB_PASSWORD);
            return dbConnection;
        } catch (SQLException e) {
            success = false;
            System.out.println(e.getMessage());
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
