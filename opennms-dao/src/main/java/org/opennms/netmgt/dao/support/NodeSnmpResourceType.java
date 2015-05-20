/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2007-2014 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2014 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.netmgt.dao.support;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.opennms.netmgt.dao.api.ResourceStorageDao;
import org.opennms.netmgt.model.OnmsAttribute;
import org.opennms.netmgt.model.OnmsNode;
import org.opennms.netmgt.model.OnmsResource;
import org.opennms.netmgt.model.OnmsResourceType;
import org.opennms.netmgt.model.ResourcePath;
import org.opennms.netmgt.model.ResourceTypeUtils;
import org.springframework.orm.ObjectRetrievalFailureException;

import com.google.common.collect.Lists;

public class NodeSnmpResourceType implements OnmsResourceType {

    private final ResourceStorageDao m_resourceStorageDao;

    /**
     * <p>Constructor for NodeSnmpResourceType.</p>
     *
     * @param resourceStorageDao a {@link org.opennms.netmgt.dao.api.ResourceStorageDao} object.
     */
    public NodeSnmpResourceType(ResourceStorageDao resourceStorageDao) {
        m_resourceStorageDao = resourceStorageDao;
    }

    /**
     * <p>getName</p>
     *
     * @return a {@link java.lang.String} object.
     */
    @Override
    public String getName() {
        return "nodeSnmp";
    }
    
    /**
     * <p>getLabel</p>
     *
     * @return a {@link java.lang.String} object.
     */
    @Override
    public String getLabel() {
        return "SNMP Node Data";
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean isResourceTypeOnNode(int nodeId) {
        return m_resourceStorageDao.exists(getResourcePath(nodeId));
    }

    public ResourcePath getResourcePath(int nodeId) {
        return new ResourcePath(
                ResourceTypeUtils.SNMP_DIRECTORY,
                Integer.toString(nodeId)
        );
    }

    public ResourcePath getResourcePath(String fs, String fid) {
        return new ResourcePath(
                ResourceTypeUtils.SNMP_DIRECTORY,
                ResourceTypeUtils.FOREIGN_SOURCE_DIRECTORY,
                fs,
                fid
        );
    }

    /** {@inheritDoc} */
    @Override
    public List<OnmsResource> getResourcesForNode(int nodeId) {
        return Lists.newArrayList(getResourceForNode(nodeId));
    }

    /** {@inheritDoc} */
    @Override
    public OnmsResource getChildByName(OnmsResource parent, String name) {
        // Node-level SNMP resources always have a blank name
        if (!"".equals(name)) {
            throw new ObjectRetrievalFailureException(OnmsResource.class, "Unsupported name '" + name + "' for node SNMP resource type.");
        }

        // Grab the node entity
        final OnmsNode node = ResourceTypeUtils.getNodeFromResource(parent);

        // Build the resource
        OnmsResource resource;
        if (ResourceTypeUtils.isStoreByForeignSource()) {
            resource = getResourceForNodeSource(node.getForeignSource(), node.getForeignId());
        } else {
            resource = getResourceForNode(node.getId());
        }
        resource.setParent(parent);
        return resource;
    }

    private OnmsResource getResourceForNode(int nodeId) {
        final Set<OnmsAttribute> attributes = m_resourceStorageDao.getAttributes(getResourcePath(nodeId));

        return new OnmsResource("", "Node-level Performance Data", this, attributes);
    }

    private OnmsResource getResourceForNodeSource(String fs, String fid) {
        final Set<OnmsAttribute> attributes = m_resourceStorageDao.getAttributes(getResourcePath(fs, fid));

        return new OnmsResource("", "Node-level Performance Data", this, attributes);
    }

    /**
     * {@inheritDoc}
     *
     * This resource type is never available for domains.
     * Only the interface resource type is available for domains.
     */
    @Override
    public boolean isResourceTypeOnDomain(String domain) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public List<OnmsResource> getResourcesForDomain(String domain) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override
    public String getLinkForResource(OnmsResource resource) {
        return null;
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean isResourceTypeOnNodeSource(String nodeSource, int nodeId) {
        String[] ident = ResourceTypeUtils.getFsAndFidFromNodeSource(nodeSource);
        return m_resourceStorageDao.exists(getResourcePath(ident[0], ident[1]));
    }
    
    /** {@inheritDoc} */
    @Override
    public List<OnmsResource> getResourcesForNodeSource(String nodeSource, int nodeId) {
        String[] ident = ResourceTypeUtils.getFsAndFidFromNodeSource(nodeSource);
        return Lists.newArrayList(getResourceForNodeSource(ident[0], ident[1]));
    }

}
