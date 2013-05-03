package com.servicenow.bigdata.metadata;

/**
 * Created with IntelliJ IDEA.
 * User: SERVICE-NOW\eason.hu
 * Date: 4/18/13
 * Time: 11:07 AM
 * To change this template use File | Settings | File Templates.
 */

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

import com.stumbleupon.async.Deferred;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.hbase.async.HBaseClient;

public class Test {
    public static void main(String[] args) throws Throwable {
        System.out.println("Hello World!");

        XMLConfigLoader xcl = new XMLConfigLoader();
        String hbaseURL = xcl.get("HBaseURL");

        Random generator = new Random();

        /* Asynchbase test code here */
        long startTimeAsync = System.currentTimeMillis();
        AsynchbaseAccess.init(hbaseURL);
        /*for(int i = 1; i <= 100; ++i) {
            for(int j = 1; j <= 100; ++j) {
                AsynchbaseAccess.add("h" + i + "-m" + j, new Integer(generator.nextInt(100)).toString());
            }
        }*/

        //System.out.println(AsynchbaseAccess.get("h1-m2"));
        /*ArrayList<String> keysAsync = (ArrayList<String>) AsynchbaseAccess.getKeysWithPrefix("h2-");
        for(String key : keysAsync) {
            System.out.println(key);
        }
        System.out.println("Total number of keys = " + keysAsync.size());
        long endTimeAsync   = System.currentTimeMillis();
        long totalTimeAsync = endTimeAsync - startTimeAsync;
        System.out.println("Asynchbase read total time = " + totalTimeAsync + "ms");*/

        //AsynchbaseAccess.removeKeysWithPrefix("3");

        AsynchbaseAccess.disconnect();

        /* HTable test code here */

        /*long startTimeHTable = System.currentTimeMillis();
        HbaseAccess.init(hbaseURL);
        ArrayList<String> keysHTable = (ArrayList<String>) HbaseAccess.getKeysWithPrefix("h1");
        System.out.println(keysHTable.size());
        long endTimeHTable   = System.currentTimeMillis();
        long totalTimeHTable = endTimeHTable - startTimeHTable;
        System.out.println("HTable read total time = " + totalTimeHTable + "ms");

        HbaseAccess.close();*/
    }
}
