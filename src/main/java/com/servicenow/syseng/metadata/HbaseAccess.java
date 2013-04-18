package com.servicenow.syseng.metadata;

/**
 * Created with IntelliJ IDEA.
 * User: SERVICE-NOW\chi.yao
 * Date: 4/16/13
 * Time: 1:17 PM
 * To change this template use File | Settings | File Templates.
 */

import java.util.ArrayList;
import java.util.Collection;
import org.apache.hadoop.hbase.client.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseAccess {

    private static final Logger logger = LoggerFactory.getLogger(HbaseAccess.class);

    public static final String HBASE_URL = "";     // TODO: get config
    // all meta data live in one table
    private static final String METADATA_TABLE = "m";
    private static final String MAP_ID_TABLE = "map_id";
    private static final String DEFAULT_COLUMN_FAMILY = "cf"; // so far all tables use this column family only
    private static final byte[] DEFAULT_CF_BYTES = DEFAULT_COLUMN_FAMILY.getBytes();
    private static final String DEFAULT_COLUMN = "c"; // so far all tables use this column only
    private static final byte[] DEFAULT_COL_BYTES = DEFAULT_COLUMN.getBytes();
    private static final String MAP_SEQUENCE = "<map_sequence>";
    private static final byte[] MAP_SEQUENCE_BYTES = MAP_SEQUENCE.getBytes();

    private static final HTablePool htablePool = new HTablePool();


    // make sure hbase has the right schema (TODO: may do it from conf file)
    public static final void init() {

        try {
            // init table m with column family "cf"
            createTableIfNotExists(METADATA_TABLE,DEFAULT_COLUMN_FAMILY);
            // init table map_id with column family "cf"
            createTableIfNotExists(MAP_ID_TABLE,DEFAULT_COLUMN_FAMILY);
            // add record ("<map_sequence>","cf","c",1) to MAP_ID_TABLE
            add(MAP_ID_TABLE,MAP_SEQUENCE,ByteBuffer.allocate(8).putLong(1L).array());
            // add record ("m","cf","c",0) to MAP_ID_TABLE
            add(MAP_ID_TABLE,METADATA_TABLE,ByteBuffer.allocate(8).putLong(0L).array());

        } catch (Exception e) {
            logger.error("Cannot initialize Hbase database");
        }

    }

    // add this key-value pair to METADATA_TABLE
    public static final void add(String key, String val) throws IOException {
        add(METADATA_TABLE,key,val);
    }

    // from METADATA_TABLE
    public static final Collection<String> getKeysWithPrefix(String prefix) throws IOException {
        Scan scan = createRangeScanByPrefix(prefix);
        scan.addColumn(DEFAULT_CF_BYTES,DEFAULT_COL_BYTES).setMaxVersions(1);
        HTableInterface htable = htablePool.getTable(METADATA_TABLE);
        ResultScanner resultScanner = htable.getScanner(scan);
        ArrayList<String> al = new ArrayList<String>(20);
        for (Result r=resultScanner.next();r!=null;r=resultScanner.next()) {
            al.add(r.getRow().toString());
        }
        return al;
    }

    // from METADATA_TABLE
    public static final void removeKeysWithPrefix(String prefix) throws IOException {
        Scan scan = createRangeScanByPrefix(prefix);
        scan.addColumn(DEFAULT_CF_BYTES,DEFAULT_COL_BYTES).setMaxVersions(1);
        HTableInterface htable = htablePool.getTable(METADATA_TABLE);
        ResultScanner resultScanner = htable.getScanner(scan);
        // build a delete spec
        ArrayList<Delete> deletes = new ArrayList<Delete>(20);
        for (Result r=resultScanner.next();r!=null;r=resultScanner.next()) {
            deletes.add(new Delete(r.getRow()));
        }
        // TODO: not sure if this can handle large batch, but hbase only support single row delete
        htable.delete(deletes);
    }

    // create a scan with a start row and end row matching the prefix
    private static final Scan createRangeScanByPrefix(String prefix) throws IOException {
        // create end row for query
        byte[] beginRow = prefix.getBytes();
        byte[] endRow = beginRow.clone();
        endRow[endRow.length-1] = (byte) (endRow[endRow.length-1]+1);
        return new Scan(beginRow,endRow);
    }

    // add key-val pair to table
    private static final void add(String table, String key, String val) throws IOException {
        add(table,key,val.getBytes());
    }

    // add key-val pair to table
    private static final void add(String table, String key, byte[] val) throws IOException {
        HTableInterface htable = htablePool.getTable(table);
        htable.setAutoFlush(true); // TODO: to optimize
        htable.put(new Put(key.getBytes()).add(DEFAULT_CF_BYTES,DEFAULT_COL_BYTES,val));
    }

    private static final void createTableIfNotExists(String table, String columnFamily) {
        // TBD
    }

    // get val for key from table
    // return "" if no value for the key or no such table
    // TODO: is "" better than null?
    private static final String get(String table, String key) throws IOException {
        HTableInterface htable = htablePool.getTable(table);
        htable.setAutoFlush(true); // TODO: to optimize
        Result rs = htable.get(new Get(key.getBytes()));
        if (rs.isEmpty()) {
            return "";
        }
        else {
            byte[] v = rs.getValue(DEFAULT_CF_BYTES,DEFAULT_COL_BYTES);
            return (v==null ? "" : v.toString());
        }
    }

    // get table 1 char tag for map name
    // if name does not exist, create tag for it
    public static final String getMapTag(String name) throws IOException {
        String tag = get(MAP_ID_TABLE,name);
        if (tag == null) {
            HTableInterface htable = htablePool.getTable(MAP_ID_TABLE);
            long id = htable.incrementColumnValue(MAP_SEQUENCE_BYTES,DEFAULT_CF_BYTES,DEFAULT_COL_BYTES,1);
            // assuming id <= 26 so that we can use single char tags
            tag = String.valueOf('a'+id);
            // add this name-tag pair to map_id table
            Put put = new Put(name.getBytes()).add(DEFAULT_CF_BYTES,DEFAULT_COL_BYTES,tag.getBytes());
            htable.put(put);
        }
        return tag;
    }


}
