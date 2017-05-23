package com.yizhao.apps;
import java.util.Map;


/**
 * @author YI ZHAO
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

            if(table == null){
                System.out.println("argument 1 is missing for table name");
                return;
            }

            if(csvFileOutputPath == null){
                System.out.println("argument 2 is missing for csv file output path");
                return;
            }
            if(fastrackFileOutputPath == null){
                System.out.println("argument 3 is missing for fastrack file output path");
                return;
            }

            NetezzaConnector.dataToCsv(table, csvFileOutputPath, partition);
        } catch (Exception e) {
            System.out.println("exception");
        }

        FastrackFileProcessor.execute(csvFileOutputPath, fastrackFileOutputPath);
    }

}
