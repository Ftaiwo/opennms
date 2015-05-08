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

import org.opennms.netmgt.dao.util.ResourceResolver;
import org.opennms.netmgt.model.OnmsAttribute;
import org.opennms.netmgt.model.OnmsNode;
import org.opennms.netmgt.model.OnmsResource;
import org.opennms.netmgt.model.OnmsResourceType;
import org.opennms.netmgt.model.ResourceTypeUtils;
import org.springframework.orm.ObjectRetrievalFailureException;

import com.google.common.collect.Lists;

public class NodeSnmpResourceType implements OnmsResourceType {

    private final ResourceResolver m_resourceResolver;

    /**
     * <p>Constructor for NodeSnmpResourceType.</p>
     *
     * @param resourceDao a {@link org.opennms.netmgt.dao.api.ResourceDao} object.
     */
    public NodeSnmpResourceType(ResourceResolver resourceResolver) {
        m_resourceResolver = resourceResolver;
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
     * @return a {@link java.lang.String}        
        
        File nodeSnmpDir = new File(m_resourceDao.getRrdDirectory(), ResourceTypeUtils.SNMP_DIRECTORY + File.separator
                       + ResourceTypeUtils.getRelativeNodeSourceDirectory(nodeSource).toString());
        if (!nodeSnmpDir.isDirectory()) { // A node without performance metrics should not have a directory 
            return false;
        }
         object.
     */
    @Override
    public String getLabel() {
        return "SNMP Node Data";
    }

    /** {@inheritDoc} */
    @Override
    public boolean isResourceTypeOnNode(int nodeId) {
        return m_resourceResolver.exists(ResourceTypeUtils.SNMP_DIRECTORY, Integer.toString(nodeId));
    }

    /** {@inheritDoc} */
    @Override
    public List<OnmsResource> getResourcesForNode(int nodeId) {
        return Lists.newArrayList(getResourceForNode(Integer.toString(nodeId)));
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
            resource = getResourceForNodeSource(String.format("%s:%s",
                    node.getForeignSource(), node.getForeignId()));
        } else {
            resource = getResourceForNode(Integer.toString(node.getId()));
        }
        resource.setParent(parent);
        return resource;
    }

    private OnmsResource getResourceForNode(String nodeId) {
        final Set<OnmsAttribute> attributes = m_resourceResolver.getAttributes(ResourceTypeUtils.SNMP_DIRECTORY, nodeId);

        return new OnmsResource("", "Node-level Performance Data", this, attributes);
    }

    private OnmsResource getResourceForNodeSource(String nodeSource) {
        final Set<OnmsAttribute> attributes = m_resourceResolver.getAttributes(ResourceTypeUtils.SNMP_DIRECTORY, ResourceTypeUtils.getRelativeNodeSourcePathComponents(nodeSource));

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
        // FIXME: return nodeSnmpDir.listFiles(RrdFileConstants.RRD_FILENAME_FILTER).length > 0; 
        return m_resourceResolver.exists(ResourceTypeUtils.SNMP_DIRECTORY, ResourceTypeUtils.getRelativeNodeSourcePathComponents(nodeSource));
    }
    
    /** {@inheritDoc} */
    @Override
    public List<OnmsResource> getResourcesForNodeSource(String nodeSource, int nodeId) {
        return Lists.newArrayList(getResourceForNodeSource(nodeSource));
    }

}
