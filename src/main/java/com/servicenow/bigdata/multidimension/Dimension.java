package com.servicenow.bigdata.multidimension;

/**
 * Created with IntelliJ IDEA.
 * User: SERVICE-NOW\chi.yao
 * Date: 4/26/13
 * Time: 6:36 PM
 * To change this template use File | Settings | File Templates.
 */

import java.util.Collection;
import java.io.IOException;
import com.servicenow.syseng.metadata.NamedPersistentMap;

/**
 *  implemented by NamedPersistentMap
 *  Currently supports flat dimensions only, no hierarchy
 */
public class Dimension {

    public String name;
    private Outline outline;

    public Dimension(Outline outline, String name) throws IOException {
        this.name = name;
        this.outline = outline;
    }

    /**
     * This is a DB lookup call
     * @return  a collection of member names or empty collection if no members
     * @throws IOException
     */
    public final Collection<String> getMembers() throws IOException {
        return outline.getDimensionMembers(name);
    }

    public final void addMember(String memberName) throws IOException {
        outline.addDimensionMember(name, memberName);
    }

    /**
     * This is a DB lookup call
     * @param member
     * @return   whether member is a member of this dimension
     * @throws IOException
     */
    public final boolean isMemberOf(String member) throws IOException {
        return outline.hasDimensionMember(name, member);
    }

}
