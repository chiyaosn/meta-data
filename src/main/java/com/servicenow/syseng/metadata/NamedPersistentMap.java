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
    private String name;
    private String tag;   // 1 char

    // create a NamedPersistentMap object referencing to corresponding habse representation
    // if no corresponding hbase representation exists, create one in hbase
    public NamedPersistentMap(String name) throws IOException {
        this.name = name;
        tag = HbaseAccess.getMapTag(name);
    }

    // get all keys for this NamedPersistentMap
    public Collection<String> keys() throws IOException {
        Collection<String> hkeys = HbaseAccess.getKeysWithPrefix(tag);
        // hkey is of form <tag><key>##<value>, where <table> is 1 char long
        HashSet<String> result = new HashSet<String>(hkeys.size());
        String s;
        for (String k:hkeys) {
            s = k.split(KEY_SEPARATOR)[0];
            result.add(s.substring(1));
        }
        return result;
    }

    // add value to the key's mapped collection
    public void addValue(String key, String value) throws IOException {
        String hkey = new StringBuffer(tag)  // no separator to save space
                .append(key).append(KEY_SEPARATOR)
                .append(value).toString();
        HbaseAccess.add(hkey,"");
    }

    // remove the key’s mapped collection
    public void removeKey(String key) throws IOException {
        String prefix = new StringBuilder(tag).append(key).toString();
        HbaseAccess.removeKeysWithPrefix(prefix);
    }

    // get the value collection for this key
    // return an empty arraylist if no value
    public Collection<String> getValue(String key) throws IOException {
        String prefix = new StringBuilder(tag).append(key).append(KEY_SEPARATOR).toString();
        Collection<String> hkeys = HbaseAccess.getKeysWithPrefix(prefix);
        Collection<String> result = new ArrayList<String>(hkeys.size());
        String[] val;
        for (String k:hkeys) {
            val = k.split(KEY_SEPARATOR);
            result.add(val.length>1 ? val[1] : ""); // value can be empty string
        }
        return result;
    }


}
