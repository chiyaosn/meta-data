package com.servicenow.bigdata.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger logger = LoggerFactory.getLogger(NamedPersistentCache.class);
    private HashSet<String> cache = new HashSet<>();

    public NamedPersistentCache() {}

    public boolean contains(String key) {
        return cache.contains(key);
    }

    public boolean add(String key) {
        logger.debug("Added cache key: " + key);
        return cache.add(key);
    }

    public boolean remove(String key) {
        return cache.remove(key);
    }

    public HashSet<String> getAllKeys() {
        return cache;
    }
}
