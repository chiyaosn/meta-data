
package com.servicenow.bigdata.metadata;
import java.util.Collection;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MetaDataRepository helps you find:
 * 1. all name spaces
 * 2. all instances for a name space
 * 3. all keys for a name space (including reading name)
 * 4. all values for a name space, instance, instance combination
 *
 */

public class MetaDataRepository {

    private static final String NAMESPACE = "nameSpace";
    private static final String NAMESPACE_KEYS = "nameSpaceKeys";
    private static final String NAMESPACE_INSTANCES = "nameSpaceInstances";

    public static class NamedPersistentMapPool {
        static ConcurrentHashMap<String,NamedPersistentMap> pool = new ConcurrentHashMap<String,NamedPersistentMap>();
        static NamedPersistentMap get(String name) throws IOException {
            if (!pool.containsKey(name)) {
                pool.put(name,new NamedPersistentMap(name));
            }
            return pool.get(name);
        }
    }

    public static final String genKeyInstanceTag(String key,String instanceId) {
        return new StringBuffer(key).append(NamedPersistentMap.KEY_INSTANCE_SEPARATOR).append(instanceId).toString();
    }


    /**
     *  add the key-value pair to namespace with the scope instanceId
     *  e.g. add("UsageStats","user","u1","instance01")
     *   or  add("UsageStats","reading.name","AppAccess","instance01")
     */
    public static final void add(String nameSpace, String key, String value, String instanceId) throws IOException {
        // add this name space
	    addNameSpace(nameSpace);
        // add this instance to the name space
        addNameSpaceInstance(nameSpace, instanceId);
        // add this key to the name space
        addNameSpaceKey(nameSpace,key);
        // add this value to the key in this name space in this instance scope
        addNameSpaceKeyValue(nameSpace,key,value,instanceId);
    }


    /**
     *  return the values for key in namespace and scope intanceId
     *  If instanceId is null, return global (all-instance) lookup result
     */
    public static final Collection<String> getValues(String nameSpace, String key, String instanceId) throws IOException {
	    return new NamedPersistentMap(nameSpace).getValues(genKeyInstanceTag(key,instanceId));
    }


    /**
     * return the keys in this name space
     */
    public static final Collection<String> getKeys(String nameSpace) throws IOException {
	return new NamedPersistentMap(NAMESPACE_KEYS).getValues(nameSpace);
    }

    /**
     * return the instances in this name space
     */
    public static final Collection<String> getInstances(String nameSpace) throws IOException {
	return new NamedPersistentMap(NAMESPACE_INSTANCES).getValues(nameSpace);
    }


    /**
     * get all the name spaces
     */
    public static final Collection<String> getNameSpaces() throws IOException {
        // go to persistent store directly
	return new NamedPersistentMap(NAMESPACE).getValues(NAMESPACE);
    }


    /**
     *  add the name space nameSpace if not exists yet
     */
    private static final void addNameSpace(String nameSpace) throws IOException {
        NamedPersistentMapPool.get(NAMESPACE).addValue(NAMESPACE, nameSpace);
	}



    /**
     * add the key to the name space
     */
    private static final void addNameSpaceKey(String nameSpace, String key) throws IOException {
        NamedPersistentMapPool.get(NAMESPACE_KEYS).addValue(nameSpace, key);
	}


    /**
     *  add the key value to the namespace in the scope instanceId
     */
    private static final void addNameSpaceKeyValue(String nameSpace, String key, String value, String instanceId) throws IOException {
	    NamedPersistentMapPool.get(nameSpace).addValue(genKeyInstanceTag(key,instanceId), value);
	}

    /**
     * add the instance to the name space
     */
    private static final void addNameSpaceInstance(String nameSpace,String instanceId) throws IOException {
	    NamedPersistentMapPool.get(NAMESPACE_INSTANCES).addValue(nameSpace, instanceId);
	}


    public static final void main(String[] args)  {

        try {
            add("UsageAnalytics", "User", "Chris", "i01");
            add("UsageAnalytics", "User", "Kevin", "i02");
            add("UsageAnalytics", "Table", "Syslog", "i01");
            add("UsageAnalytics", "Application", "ContentManagement", "i01");
            add("UsageAnalytics", "Application", "ConferenceRoomBooking", "i02");

            System.out.println("Namespaces: ");
            for (String ns: getNameSpaces()) {
                System.out.println("--"+ns);
                System.out.println("----Instances: ");
                for (String i: getInstances(ns)) {
                    System.out.println("------"+i);
                    for (String k: getKeys(ns)) {
                        for (String v: getValues(ns,k,i)) {
                            System.out.println("        "+k+"="+v);
                        }
                    }
                }
            }

            System.exit(0);
        } catch (Exception e) {e.printStackTrace();}
    }

}
