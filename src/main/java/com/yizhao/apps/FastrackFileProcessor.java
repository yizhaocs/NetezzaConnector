package com.yizhao.apps;

import org.apache.commons.lang3.text.StrTokenizer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * Reading the ekv raw files to create a map that does even_id -> even_id,kvPair,cookie_id,dp_id,location_id,modification_ts
 *
 * @author YI ZHAO
 */
public class FastrackFileProcessor {
    private static final StrTokenizer st = StrTokenizer.getCSVInstance();
    static {
        st.setDelimiterChar('|');
    }


    /**
     * EKV row format:
     *      20161497900101|25594|BOS|208642445203|3346|45815|2017-03-15 23:12
     *      20161497900101|25595|FLL|208642445203|3346|45815|2017-03-15 23:12
     *
     * @param inFilePath
     */
    public static Map<String, FastrackFileDao> execute(String inFilePath) {
        Map<String, FastrackFileDao> eventIdToData = new HashMap<String, FastrackFileDao>();
        Scanner s = null;
        try {
            s = new Scanner(new File(inFilePath));
            while (s.hasNextLine()) {
                String line = s.nextLine();
                String[] str = st.reset(line).getTokenArray();
                if (str.length != 7) {
                    System.out.println("str.length != 7 at the line:" + line);
                    continue;
                }
                String event_id = str[0];
                String key_id = str[1];
                String value = str[2];
                String cookie_id = str[3];
                String dp_id = str[4];
                String location_id = str[5];
                String modification_ts = str[6];

                String kvPair = key_id + "=" + value;
                if (eventIdToData.containsKey(event_id)) {
                    FastrackFileDao tmpFastrackFileDao = eventIdToData.get(event_id);
                    tmpFastrackFileDao.setKvPair(tmpFastrackFileDao.getKvPair() + "&" + kvPair);
                } else {
                    FastrackFileDao mFastrackFileDao = new FastrackFileDao(event_id, kvPair, cookie_id, dp_id, location_id, modification_ts);
                    eventIdToData.put(event_id, mFastrackFileDao);
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("Caught FileNotFoundException: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("Caught IOException: " + e.getMessage());
        } finally {
            System.out.print("eventIdToData.size:" + eventIdToData.size());
            if (s != null) {
                s.close();
            }
        }
        return eventIdToData;
    }
}
