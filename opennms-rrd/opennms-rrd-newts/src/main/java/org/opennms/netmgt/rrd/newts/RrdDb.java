package org.opennms.netmgt.rrd.newts;

import org.opennms.newts.api.Resource;

/**
 * Wrapper for holding the path to an RRD file.
 *
 * Used to convert the path to a resource id.
 *
 * @author jwhite
 */
public class RrdDb {
    private final String m_path;

    public RrdDb(String path) {
        m_path = path;
    }

    public String getPath() {
        return m_path;
    }

    public Resource getResource() {
        return NewtsUtils.getResourceFromPath(m_path);
    }
}
