package com.servicenow.syseng.metadata;
import java.util.Collection;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaDataRepository {


    public static final String REPO_MAP_NAME = "repository";
    private static final Logger logger = LoggerFactory.getLogger(MetaDataRepository.class);

    // repositoryMap stores the map for all NamedPersistentMap including itself
    private static NamedPersistentMap repositoryMap;

    static { // init repository
        try {
            // make sure hbase has the schema
            HbaseAccess.init();
            repositoryMap = new NamedPersistentMap(REPO_MAP_NAME);
        } catch (IOException e) {
            logger.error("Cannot initialize MetaDataRepository");
        }
    }

    // get the NamedPersistentMap called name
    // if not exists yet, create one and return
    public static NamedPersistentMap getNamedPersistentMap(String name) throws IOException {
        if (repositoryMap.getValue(name).isEmpty()) { // add to repositoryMap if not exists
            createNamedPersistentMap(name);
        }
        return new NamedPersistentMap(name);
    }


    // get all NamedPersistentMap names
    public static Collection<String> getAllNamedPersistentMapNames() throws IOException {
	return repositoryMap.keys();
    }


    // create a new NamedPersistentMap called name
    private static final void createNamedPersistentMap(String name) throws IOException {
	    repositoryMap.addValue(name,name);
    }


    public static final void main(String[] args) {
        try {
            NamedPersistentMap hostMetricsMap = new NamedPersistentMap("host-metrics");
            hostMetricsMap.addValue("host1","xmlstats.transaction.count");
            hostMetricsMap.addValue("host1","xmlstats.transaction.avg");
            Collection<String> metrics = hostMetricsMap.getValue("host1");
            System.out.println("Host host1 has metrics:");
            for (String m:metrics) {
                System.out.print(m+" ");
            }
            System.out.println();

            NamedPersistentMap dataCenterHostsMap = new NamedPersistentMap("datacenter-hosts");
            dataCenterHostsMap.addValue("iad1","host1");
            dataCenterHostsMap.addValue("iad1","host2");
            Collection<String> hosts = dataCenterHostsMap.getValue("iad1");
            System.out.println("Data center iad1 has hosts:");
            for (String h:hosts) {
                System.out.print(h+" ");
            }
            System.out.println();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
