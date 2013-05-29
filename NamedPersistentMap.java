package com.servicenow.bigdata.metadata;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
NamesPersistentMap is a persistent map of String -> Collection<String>
It does all the lookup in underlying distributed data store without caching anything
It is managed by MetaDataRepository so that such a map can be queried by its name
 */

public class NamedPersistentMap {

    private static final Logger logger = LoggerFactory.getLogger(NamedPersistentMap.class);
    private static String hbaseHost = "10.64.14.192";
    //private static String hbaseHost = "localhost";

    public static final String KEY_SEPARATOR = "##";   // used to construct keys in hbase
    public static final String TAG_SEPARATOR = "_";    // used to construct tag_keys in hbase
    private String mapIDName;
    private String mapIDTag;

    private static Class dataAccess = HbaseAccess.class;
    //private static Class dataAccess = AsynchbaseAccess.class;

    private NamedPersistentCache cache = new NamedPersistentCache();


    // initialize data layer
    static {
        try {
            if ("false".equalsIgnoreCase(System.getProperty("metaDataRepository.hbaseAsync"))) {
                dataAccess = HbaseAccess.class;
            }
            if (System.getProperty("metaDataRepository.hbaseHost") != null ) {
                hbaseHost = System.getProperty("metaDataRepository.hbaseHost");
            }
            dataAccess.getMethod("init",String.class).invoke(null,hbaseHost);
        } catch (Exception e) {
            logger.error("Cannot initialize Hbase data access",e);
        }
    }

    // create a NamedPersistentMap object referencing to corresponding hbase representation
    // if no corresponding hbase representation exists, create one in hbase
    public NamedPersistentMap(String mapIDName) throws IOException {
        this.mapIDName = mapIDName;
        try {
            mapIDTag = (String) dataAccess.getMethod("getMapTag", String.class).invoke(null, mapIDName);
        } catch (Exception e)  {
            logger.error("Cannot get map tag from HBase",e);
            throw new IOException(e);
        }
    }


    // constructor to enable choose which hbase client to use
    public NamedPersistentMap(String mapIDName, boolean enableAH) throws IOException {
        this.mapIDName = mapIDName;
        dataAccess = enableAH == true? AsynchbaseAccess.class : HbaseAccess.class;
        try {
            mapIDTag = (String) dataAccess.getMethod("getMapTag", String.class).invoke(null, mapIDName);
        } catch (Exception e) {
            logger.error("Cannot get map tag from HBase",e);
        }
    }


    // enable Asynchbase
    public void enableAsynchbase(boolean status) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if(status) {
            dataAccess = AsynchbaseAccess.class;
            dataAccess.getMethod("init",String.class).invoke(null, hbaseHost);
        }
        else {
            dataAccess = HbaseAccess.class;
            dataAccess.getMethod("init",String.class).invoke(null,hbaseHost);
        }
    }


    /**
     * helper function
     * return the key part in the Hbase representation of this key-value pair
     */
    private String getHKey(String key, String value) {
        return new StringBuffer(mapIDTag).append(TAG_SEPARATOR)
                .append(key).append(KEY_SEPARATOR)
                .append(value).toString();
    }


    // add value to the key's mapped collection
    public void addValue(String key, String value) throws IOException {
        String hkey = getHKey(key, value);

        if(cache.contains(hkey) == false) {
            try {
                dataAccess.getMethod("addValue",String.class, String.class).invoke(null, hkey, "1");
                cache.add(hkey);
            } catch (Exception e)  {}
        }
    }


    // get the value collection for this key
    // return an empty arraylist if no value
    public Collection<String> getValues(String key) throws IOException {
        String prefix = new StringBuilder(mapIDTag).append(TAG_SEPARATOR).append(key).append(KEY_SEPARATOR).toString();
        Collection<String> hkeys = null;
        try {
            hkeys = (Collection<String>) dataAccess.getMethod("getKeysWithPrefix",String.class).invoke(null,prefix);
        } catch (Exception e)  {e.printStackTrace();}
        Collection<String> result = new ArrayList<String>(hkeys.size());
        String[] val;
        for (String k:hkeys) {
            val = k.split(KEY_SEPARATOR);
            result.add(val.length>1 ? val[1] : ""); // value can be empty string
        }
        StringBuilder sb = new StringBuilder();
        for (String s:result) {
             sb.append(s).append(",");
        }
        logger.debug("NPM "+mapIDName+" "+key+" "+sb.toString());
        return result;
    }

}
