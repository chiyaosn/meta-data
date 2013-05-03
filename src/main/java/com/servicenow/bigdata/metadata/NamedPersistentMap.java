package com.servicenow.bigdata.metadata;

import java.io.IOException;
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
    //private static final Logger logger = LoggerFactory.getLogger(NamedPersistentMap.class);

    public static final String KEY_SEPARATOR = "##";   // used to construct keys in hbase
    public static final String TAG_SEPARATOR = "_";    // used to construct tag_keys in hbase
    private String mapIDName;
    private String mapIDTag;   // 1 char

    //private Class dataAccess = HbaseAccess.class;
    private Class dataAccess = AsynchbaseAccess.class;

    private NamedPersistentCache cache = new NamedPersistentCache();

    // create a NamedPersistentMap object referencing to corresponding hbase representation
    // if no corresponding hbase representation exists, create one in hbase
    public NamedPersistentMap(String mapIDName) throws IOException {
        this.mapIDName = mapIDName;
        try {
            mapIDTag = (String) dataAccess.getMethod("getMapTag", String.class).invoke(null, mapIDName);
        } catch (Exception e)  {e.printStackTrace();}
    }


    // constructor to enable choose which hbase client to use
    public NamedPersistentMap(String mapIDName, boolean enableAH) throws IOException {
        this.mapIDName = mapIDName;
        dataAccess = enableAH == true? AsynchbaseAccess.class : HbaseAccess.class;
        try {
            mapIDTag = (String) dataAccess.getMethod("getMapTag", String.class).invoke(null, mapIDName);
        } catch (Exception e) {e.printStackTrace();}
    }


    // enable Asynchbase
    public void enableAsynchbase(boolean status) {
        if(status) {
            dataAccess = AsynchbaseAccess.class;
            try {
                mapIDTag = (String) dataAccess.getMethod("getMapTag", String.class).invoke(null, mapIDName);
            } catch (Exception e) {e.printStackTrace();}
        }
        else {
            dataAccess = HbaseAccess.class;
            try {
                mapIDTag = (String) dataAccess.getMethod("getMapTag", String.class).invoke(null, mapIDName);
            } catch (Exception e) {e.printStackTrace();}
        }
    }


    // Get map key from hbase
    public String getMapKey(String key) {
        return new StringBuffer(mapIDTag).append(TAG_SEPARATOR)
                .append(key).toString();
    }


    // Get map key from hbase
    public String getMapKey(String key, String metric) {
        return new StringBuffer(mapIDTag).append(TAG_SEPARATOR)
                .append(key).append(KEY_SEPARATOR)
                .append(metric).toString();
    }


    // add value to the key's mapped collection
    public void addKey(String key) throws IOException {
        String hkey = getMapKey(key);

        if(cache.contains(hkey) == false) {
            try {
                dataAccess.getMethod("addValue", String.class, String.class).invoke(null, hkey, "1");
                cache.add(hkey);
            } catch (Exception e)  {}
        }
    }


    // add value to the key's mapped collection
    public void addKeyMetric(String key, String metric) throws IOException {
        String hkey = getMapKey(key, metric);

        if(cache.contains(hkey) == false) {
            try {
                dataAccess.getMethod("addValue",String.class, String.class).invoke(null, hkey, "1");
                cache.add(hkey);
            } catch (Exception e)  {}
        }
    }


    // remove a specific key
    public void removeKey(String key) throws IOException {
        String hkey = getMapKey(key);
        try {
            dataAccess.getMethod("removeKey", String.class).invoke(null, hkey);
        } catch (Exception e)  {}
    }


    // remove a specific key
    public void removeKey(String key, String metric) throws IOException {
        String hkey = getMapKey(key, metric);
        try {
            dataAccess.getMethod("removeKey", String.class).invoke(null, hkey);
        } catch (Exception e)  {}
    }


    // remove the key’s mapped collection
    public void removeKeys(String keyPrefix) throws IOException {
        String prefix = getMapKey(keyPrefix);
        try {
            dataAccess.getMethod("removeKeysWithPrefix", String.class).invoke(null, prefix);
        } catch (Exception e)  {}
    }


    // Check if key exists
    public final boolean hasKey(String key, String metric) throws IOException {
        String prefix = getMapKey(key, metric);

        Collection<String> hkeys = null;
        try {
            hkeys = (Collection<String>) dataAccess.getMethod("getKey", String.class).invoke(null, prefix);
        } catch (Exception e)  {e.printStackTrace();}

        if(hkeys == null || hkeys.isEmpty())
            return false;
        else
            return true;
    }


    // Check if key exists
    public final boolean hasKey(String key, String metric, boolean involveCache) throws IOException {
        String hkey = getMapKey(key, metric);

        if(involveCache && cache.contains(hkey)) {
            return true;
        }

        Collection<String> hkeys = null;
        try {
            hkeys = (Collection<String>) dataAccess.getMethod("getKey", String.class).invoke(null, hkey);
        } catch (Exception e)  {e.printStackTrace();}

        if(hkeys == null || hkeys.isEmpty())
            return false;
        else
            return true;
    }


    // get the metric collection for this key and return null if no value
    public Collection<String> getMetrics(String keyPrefix) throws IOException {
        String prefix = getMapKey(keyPrefix);

        Collection<String> hkeys = null;
        try {
            hkeys = (Collection<String>) dataAccess.getMethod("getKeysWithPrefix", String.class).invoke(null, prefix);
        } catch (Exception e)  {e.printStackTrace();}

        if(hkeys == null)
            return null;

        // hkey is of form <tag>_<key>##<metric>, where <tag> is a number
        Collection<String> result = new ArrayList<String>(hkeys.size());
        String[] temp;
        for (String k : hkeys) {
            temp = k.split(KEY_SEPARATOR);
            result.add(temp.length > 1 ? temp[1] : "");  // in case of invalid / empty input
        }
        return result;
    }


    // get all keys for this NamedPersistentMap
    // TODO: It requires to scan all rows to get all keys, performance-wise is not great.  Should figure out another approach
    public Collection<String> getAllKeys() throws IOException {
        Collection<String> hkeys = null;
        try {
            hkeys = (Collection<String>) dataAccess.getMethod("getKeysWithPrefix", String.class).invoke(null, mapIDTag + TAG_SEPARATOR);
        } catch (Exception e)  {e.printStackTrace();}

        // hkey is of form <tag><key>##<value>, where <tag> is 1 char long
        Collection<String> result = new HashSet<String>(hkeys.size());
        String[] temp;
        for (String k : hkeys) {
            temp = k.split(KEY_SEPARATOR)[0].split(TAG_SEPARATOR);
            result.add(temp.length > 1 ? temp[1] : "");  // in case of invalid / empty input
        }
        return result;
    }
}
