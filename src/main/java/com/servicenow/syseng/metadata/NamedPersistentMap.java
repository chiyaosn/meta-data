package com.servicenow.syseng.metadata;

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

//    private static final Logger logger = LoggerFactory.getLogger(NamedPersistentMap.class);

    private static final String KEY_SEPARATOR = "##"; // used to construct keys in hbase
    private String mapIDName;
    private String mapIDTag;   // 1 char
    //private Class dataAccess = HbaseAccess.class;
    private Class dataAccess = AsynchbaseAccess.class;


    // create a NamedPersistentMap object referencing to corresponding hbase representation
    // if no corresponding hbase representation exists, create one in hbase
    public NamedPersistentMap(String mapIDName) throws IOException {
        this.mapIDName = mapIDName;
        try {
            //tag = HbaseAccess.getMapTag(name);
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
        if(status == true) {
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

    // add value to the key's mapped collection
    public void addValue(String key, String value) throws IOException {
        // no separator to save space
        String hkey = new StringBuffer(mapIDTag)
                .append(key).append(KEY_SEPARATOR)
                .append(value).toString();
        try {
            dataAccess.getMethod("addValue",String.class, String.class).invoke(null, hkey, "");
        } catch (Exception e)  {}
    }

    // remove the key’s mapped collection
    public void removeKeys(String keyPrefix) throws IOException {
        String prefix = new StringBuilder(mapIDTag).append(keyPrefix).toString();
        //HbaseAccess.removeKeysWithPrefix(prefix);
        try {
            dataAccess.getMethod("removeKeysWithPrefix", String.class).invoke(null, prefix);
        } catch (Exception e)  {}
    }

    // get the value collection for this key
    // return an empty arraylist if no value
    public Collection<String> getMetrics(String keyPrefix) throws IOException {
        Collection<String> hkeys = null;
        String prefix = new StringBuilder(mapIDTag).append(keyPrefix).append(KEY_SEPARATOR).toString();
        try {
            hkeys = (Collection<String>) dataAccess.getMethod("getKeysWithPrefix", String.class).invoke(null, prefix);
        } catch (Exception e)  {e.printStackTrace();}

        if(hkeys == null)
            return null;

        // hkey is of form <tag><key>##<value>, where <tag> is 1 char long
        Collection<String> result = new ArrayList<String>(hkeys.size());
        String[] val;
        for (String k : hkeys) {
            val = k.split(KEY_SEPARATOR);
            result.add(val.length > 1 ? val[1] : ""); // value can be empty string
        }
        return result;
    }

    public final boolean hasKey(String key, String val) throws IOException {
        String prefix = new StringBuilder(mapIDTag).append(key).append(KEY_SEPARATOR).append(val).toString();
        Collection<String> hkeys = null;
        try {
            //hkeys = HbaseAccess.getKeysWithPrefix(prefix);
            hkeys = (Collection<String>) dataAccess.getMethod("getKeysWithPrefix", String.class).invoke(null, prefix);
        } catch (Exception e)  {e.printStackTrace();}

        if(hkeys == null || hkeys.isEmpty())
            return false;
        else
            return true;
    }

    // get all keys for this NamedPersistentMap
    public Collection<String> getAllKeys() throws IOException {
        Collection<String> hkeys=null;
        try {
            hkeys = (Collection<String>) dataAccess.getMethod("getKeysWithPrefix", String.class).invoke(null, mapIDTag);
        } catch (Exception e)  {e.printStackTrace();}

        // hkey is of form <tag><key>##<value>, where <tag> is 1 char long
        Collection<String> result = new HashSet<String>(hkeys.size());
        String s;
        for (String k : hkeys) {
            s = k.split(KEY_SEPARATOR)[0];
            result.add(s.substring(1));
        }
        return result;
    }
}
