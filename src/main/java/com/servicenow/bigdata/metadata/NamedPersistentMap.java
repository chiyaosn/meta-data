package com.servicenow.bigdata.metadata;

import java.io.IOException;
<<<<<<< HEAD
import java.lang.reflect.InvocationTargetException;
=======
>>>>>>> 2d5414547256609f3cfe63f878baeadd9cc9fc11
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
<<<<<<< HEAD

=======
>>>>>>> 2d5414547256609f3cfe63f878baeadd9cc9fc11
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
NamesPersistentMap is a persistent map of String -> Collection<String>
It does all the lookup in underlying distributed data store without caching anything
It is managed by MetaDataRepository so that such a map can be queried by its name
 */

public class NamedPersistentMap {

    private static final Logger logger = LoggerFactory.getLogger(NamedPersistentMap.class);
<<<<<<< HEAD
    private static String hbaseHost = "10.64.14.192";
    //private static String hbaseHost = "localhost";

    public static final String KEY_SEPARATOR = "##";   // used to construct keys in hbase
    public static final String TAG_SEPARATOR = "_";    // used to construct tag_keys in hbase
    private String mapIDName;
    private String mapIDTag;

    private static Class dataAccess = HbaseAccess.class;
    //private static Class dataAccess = AsynchbaseAccess.class;
=======
    private static final String hbaseURL = "10.64.14.202";
    //private static final String hbaseURL = "localhost";

    public static final String KEY_SEPARATOR = "##";            // used to construct keys in hbase
    public static final String TAG_SEPARATOR = "_";             // used to construct tag_keys in hbase
    public static final String KEY_INSTANCE_SEPARATOR = "@@";   // used to construct key_instances in hbase
    private String mapIDName;
    private String mapIDTag;

    //private static Class dataAccess = HbaseAccess.class;
    private static Class dataAccess = AsynchbaseAccess.class;
>>>>>>> 2d5414547256609f3cfe63f878baeadd9cc9fc11

    private NamedPersistentCache cache = new NamedPersistentCache();


    // initialize data layer
    static {
        try {
<<<<<<< HEAD
            if ("false".equalsIgnoreCase(System.getProperty("metaDataRepository.hbaseAsync"))) {
                dataAccess = HbaseAccess.class;
            }
            if (System.getProperty("metaDataRepository.hbaseHost") != null ) {
                hbaseHost = System.getProperty("metaDataRepository.hbaseHost");
            }
            dataAccess.getMethod("init",String.class).invoke(null,hbaseHost);
        } catch (Exception e) {
            logger.error("Cannot initialize Hbase data access",e);
=======
            dataAccess.getMethod("init",String.class).invoke(null,hbaseURL);
        } catch (Exception e) {
             logger.error("Cannot initialize Hbase data access");
>>>>>>> 2d5414547256609f3cfe63f878baeadd9cc9fc11
        }
    }

    // create a NamedPersistentMap object referencing to corresponding hbase representation
    // if no corresponding hbase representation exists, create one in hbase
    public NamedPersistentMap(String mapIDName) throws IOException {
        this.mapIDName = mapIDName;
        try {
            mapIDTag = (String) dataAccess.getMethod("getMapTag", String.class).invoke(null, mapIDName);
<<<<<<< HEAD
        } catch (Exception e)  {
            logger.error("Cannot get map tag from HBase",e);
            throw new IOException(e);
        }
=======
        } catch (Exception e)  {e.printStackTrace();}
>>>>>>> 2d5414547256609f3cfe63f878baeadd9cc9fc11
    }


    // constructor to enable choose which hbase client to use
    public NamedPersistentMap(String mapIDName, boolean enableAH) throws IOException {
        this.mapIDName = mapIDName;
        dataAccess = enableAH == true? AsynchbaseAccess.class : HbaseAccess.class;
        try {
            mapIDTag = (String) dataAccess.getMethod("getMapTag", String.class).invoke(null, mapIDName);
<<<<<<< HEAD
        } catch (Exception e) {
            logger.error("Cannot get map tag from HBase",e);
        }
=======
        } catch (Exception e) {e.printStackTrace();}
>>>>>>> 2d5414547256609f3cfe63f878baeadd9cc9fc11
    }


    // enable Asynchbase
<<<<<<< HEAD
    public void enableAsynchbase(boolean status) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if(status) {
            dataAccess = AsynchbaseAccess.class;
            dataAccess.getMethod("init",String.class).invoke(null, hbaseHost);
        }
        else {
            dataAccess = HbaseAccess.class;
            dataAccess.getMethod("init",String.class).invoke(null,hbaseHost);
=======
    public void enableAsynchbase(boolean status) {
        if(status) {
            dataAccess = AsynchbaseAccess.class;
        }
        else {
            dataAccess = HbaseAccess.class;
>>>>>>> 2d5414547256609f3cfe63f878baeadd9cc9fc11
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
<<<<<<< HEAD
        logger.debug("NPM "+mapIDName+" "+key+" "+sb.toString());
=======
>>>>>>> 2d5414547256609f3cfe63f878baeadd9cc9fc11
        return result;
    }

}
