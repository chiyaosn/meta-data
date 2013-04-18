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
            System.out.println("test");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
