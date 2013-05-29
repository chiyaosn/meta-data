package com.servicenow.bigdata.metadata;

<<<<<<< HEAD
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

=======
>>>>>>> 2d5414547256609f3cfe63f878baeadd9cc9fc11
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
<<<<<<< HEAD

    private static final Logger logger = LoggerFactory.getLogger(NamedPersistentCache.class);
=======
>>>>>>> 2d5414547256609f3cfe63f878baeadd9cc9fc11
    private HashSet<String> cache = new HashSet<>();

    public NamedPersistentCache() {}

    public boolean contains(String key) {
        return cache.contains(key);
    }

    public boolean add(String key) {
<<<<<<< HEAD
        logger.debug("Added cache key: " + key);
=======
        System.out.println("Added cache key: " + key);
>>>>>>> 2d5414547256609f3cfe63f878baeadd9cc9fc11
        return cache.add(key);
    }

    public boolean remove(String key) {
        return cache.remove(key);
    }

    public HashSet<String> getAllKeys() {
        return cache;
    }
}
