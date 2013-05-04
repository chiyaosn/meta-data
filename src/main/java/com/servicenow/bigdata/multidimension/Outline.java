package com.servicenow.bigdata.multidimension;

/**
 * Created with IntelliJ IDEA.
 * User: SERVICE-NOW\chi.yao
 * Date: 4/26/13
 * Time: 6:22 PM
 * To change this template use File | Settings | File Templates.
 */

import com.servicenow.bigdata.metadata.NamedPersistentMap;
import java.io.IOException;
import java.util.Collection;
import java.util.ArrayList;
import java.util.HashSet;


/**
 *   Outline describes a multi-dimensional cube
 *   It is implemented by a NamedPersistentMap, whose keys are the dimensions names and values of a key are the dimension's members
 */

public class Outline {
    private String name;
    private String mapIDName;
    private NamedPersistentMap namedPersistentMap;
    private String mainDimension=null;


    public Outline(String name) throws IOException {
        this.name = name;
        mapIDName = "outline."+name;
        this.namedPersistentMap = new NamedPersistentMap(mapIDName);
    }

    public final String getName() {
        return name;
    }

    /**
     *
     * @param name
     * @return this outline's dimension named name
     * @throws IOException
     */
    public final Dimension getDimension(String name) throws IOException {
        Collection<String> dimensionNames = namedPersistentMap.getAllKeys();
        return (dimensionNames.contains(name)? new Dimension(this,name) : null);
    }


    /**
     *
     * @return collection of all dimension names
     * @throws IOException
     */
    // TODO: This is a very heavy operation.
    public final Collection<String> getDimensionNames() throws IOException {
        Collection<String> allKeys = namedPersistentMap.getAllKeys();
        // remove <main-dimension> from the result set
        Collection<String> result = new ArrayList<String>();
        for (String s: allKeys) {
            if (!s.equals("<main-dimension>")) {
                result.add(s);
            }
        }
        return result;

    }

    /**
     * add a dimension to this outline
     * @param dimensionName
     * @return  itself
     */
    public final void addDimension(String dimensionName) throws IOException {
        if (getDimension(name)==null) {
            // <tag>_<dimensionName>
            namedPersistentMap.addKey(dimensionName); // dimensionName is the key (TODO: REVIEW)
        }
    }

    public final void addDimensionMember(String dimension, String member) throws IOException {
        // <tag>_<dimensionName>##<member>
        namedPersistentMap.addKeyMetric(dimension,member);
    }


    // support only one main dimension now
    public final void setMainDimension(String name) throws IOException {
        namedPersistentMap.addKeyMetric("<main-dimension>",name);
    }

    public final String getMainDimensionName() throws IOException {
        Collection<String> names = namedPersistentMap.getMetrics("<main-dimension>");
        return (names.isEmpty()? null : names.iterator().next()); // assuming one main dimension for now
    }


    /**
     *
     * @param dimensionName
     * @return all members of the given dimension
     * @throws IOException
     */
    public Collection<String> getDimensionMembers(String dimensionName) throws IOException {
        Collection<String> all = namedPersistentMap.getMetrics(dimensionName);
        // remove empty string from the result set
        Collection<String> result = new ArrayList<String>();
        for (String s:all) {
            if (!s.isEmpty()) {
                result.add(s);
            }
        }
        return result;
    }


    /**
     *
     * @param dimension
     * @param member
     * @return  whether dimension has member in this outline
     * @throws IOException
     */
    public final boolean hasDimensionMember(String dimension, String member) throws IOException {
        Collection<String> members = namedPersistentMap.getMetrics(dimension);
        for (String s:members) {
            if (s.equals(member)) {
                return true;
            }
        }
        return false;

    }


    public static final void main() {
        try {

            // show sample usage

            Outline ol = new Outline("usageStats");
            ol.addDimension("user");
            ol.addDimension("application");
            ol.addDimension("table");
            ol.addDimension("operation");
            ol.addDimension("instance");
            ol.setMainDimension("instance");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
