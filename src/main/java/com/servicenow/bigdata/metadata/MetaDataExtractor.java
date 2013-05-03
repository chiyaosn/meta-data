package com.servicenow.bigdata.metadata;

/**
 * Created with IntelliJ IDEA.
 * User: SERVICE-NOW\chi.yao
 * Date: 4/23/13
 * Time: 10:48 AM
 * To change this template use File | Settings | File Templates.
 */

import java.util.HashSet;
import java.util.HashMap;

public abstract class MetaDataExtractor {

    public class KeyValuePair {
        String key;
        String value;
        KeyValuePair(String key,String value) {
            this.key = key;
            this.value = value;
        }
    }

    private final String name = this.getClass().getName();
    private String mapName = "map-for-"+name;
    private static HashMap<String, HashSet<String>> allCaches = new HashMap<String, HashSet<String>>();

    public MetaDataExtractor(String mapName) {
        this.mapName = mapName;
    }

    // extract key-value metadata from the canonical metrics record
    abstract KeyValuePair extract(GenericDataRecord genericDataRecord);

    // add the pair in cache if not exists
    // return whether it exists before caching
    public boolean exists(KeyValuePair pair) {
        // check cache
        HashSet<String> cache = allCaches.get(name);
        if (cache==null) {
            cache = new HashSet<String>();
            allCaches.put(name, cache);
        }
        String s = new StringBuilder(pair.key).append("##").append(pair.value).toString();
        if (cache.contains(s)) {
            return true;
        }
        else {
            cache.add(s);
            // todo: capacity management
            return false;
        }
    }

    public final String getMapName() {
        return mapName;
    }


}
