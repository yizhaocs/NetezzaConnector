package com.yizhao.apps;

import java.util.Map;

/**
 * Created by yzhao on 5/23/17.
 */
public class TestMain {
    public static void main(String[] args){
        Map<String, FastrackFileDao> eventIdToData =  FastrackFileProcessor.execute("/Users/yzhao/Desktop/test.txt");
        FastrackFileGenerator.execute(eventIdToData, "/Users/yzhao/Desktop/result.txt");
    }
}
