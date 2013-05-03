package com.servicenow.bigdata.metadata;

import java.util.Collection;
import java.util.HashSet;

/**
 * Created with IntelliJ IDEA.
 * User: SERVICE-NOW\eason.hu
 * Date: 5/2/13
 * Time: 1:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class NamedPersistentCache {
    private HashSet<String> cache = new HashSet<>();

    public NamedPersistentCache() {}

    public boolean contains(String key) {
        return cache.contains(key);
    }

    public boolean add(String key) {
        System.out.println("Added cache key: " + key);
        return cache.add(key);
    }

    public boolean remove(String key) {
        return cache.remove(key);
    }

    public HashSet<String> getAllKeys() {
        return cache;
    }
}
