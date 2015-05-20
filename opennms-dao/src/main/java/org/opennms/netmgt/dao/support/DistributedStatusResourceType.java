/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2006-2014 The OpenNMS Group, Inc.
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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.opennms.core.utils.InetAddressUtils;
import org.opennms.core.utils.LazySet;
import org.opennms.netmgt.dao.api.LocationMonitorDao;
import org.opennms.netmgt.dao.api.ResourceStorageDao;
import org.opennms.netmgt.model.LocationMonitorIpInterface;
import org.opennms.netmgt.model.OnmsAttribute;
import org.opennms.netmgt.model.OnmsIpInterface;
import org.opennms.netmgt.model.OnmsNode;
import org.opennms.netmgt.model.OnmsResource;
import org.opennms.netmgt.model.OnmsResourceType;
import org.opennms.netmgt.model.ResourcePath;
import org.opennms.netmgt.model.ResourceTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.orm.ObjectRetrievalFailureException;

import com.google.common.collect.Lists;

public class DistributedStatusResourceType implements OnmsResourceType {
    
    private static final Logger LOG = LoggerFactory.getLogger(DistributedStatusResourceType.class);
    
    /** Constant <code>DISTRIBUTED_DIRECTORY="distributed"</code> */
    public static final String DISTRIBUTED_DIRECTORY = "distributed";
    
    private final ResourceStorageDao m_resourceStorageDao;
    private final LocationMonitorDao m_locationMonitorDao;
    
    /**
     * <p>Constructor for DistributedStatusResourceType.</p>
     *
     * @param resourceStorageDao a {@link org.opennms.netmgt.dao.api.ResourceStorageDao} object.
     * @param locationMonitorDao a {@link org.opennms.netmgt.dao.api.LocationMonitorDao} object.
     */
    public DistributedStatusResourceType(ResourceStorageDao resourceStorageDao, LocationMonitorDao locationMonitorDao) {
        m_resourceStorageDao = resourceStorageDao;
        m_locationMonitorDao = locationMonitorDao;
    }

    /**
     * <p>getLabel</p>
     *
     * @return a {@link java.lang.String} object.
     */
    @Override
    public String getLabel() {
        return "Distributed Status";
    }

    /**
     * <p>getName</p>
     *
     * @return a {@link java.lang.String} object.
     */
    @Override
    public String getName() {
        return "distributedStatus";
    }

    /** {@inheritDoc} */
    @Override
    public List<OnmsResource> getResourcesForDomain(String domain) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override
    public List<OnmsResource> getResourcesForNode(int nodeId) {
        LinkedList<OnmsResource> resources =
            new LinkedList<OnmsResource>();
        
        Collection<LocationMonitorIpInterface> statuses = m_locationMonitorDao.findStatusChangesForNodeForUniqueMonitorAndInterface(nodeId);

        for (LocationMonitorIpInterface status : statuses) {
            String definitionName = status.getLocationMonitor().getDefinitionName();
            int id = status.getLocationMonitor().getId();
            final OnmsIpInterface ipInterface = status.getIpInterface();
			String ipAddr = InetAddressUtils.str(ipInterface.getIpAddress());

			if (m_resourceStorageDao.exists(getRelativeInterfacePath(id, ipAddr))) {
			    resources.add(createResource(definitionName, id, ipAddr));
			}
        }

        return OnmsResource.sortIntoResourceList(resources);
    }

    /** {@inheritDoc} */
    @Override
    public OnmsResource getChildByName(OnmsResource parent, String name) {
        // Grab the node entity
        final OnmsNode node = ResourceTypeUtils.getNodeFromResource(parent);

        // Load all of the resources and search. This is not the most efficient approach,
        // but resources of this type should be sparse.
        for (OnmsResource resource : getResourcesForNode(node.getId())) {
            if (resource.getName().equals(name)) {
                resource.setParent(parent);
                return resource;
            }
        }

        throw new ObjectRetrievalFailureException(OnmsResource.class, "No child with name '" + name + "' found on '" + parent + "'");
    }

    /**
     * <p>getResourcesForLocationMonitor</p>
     *
     * @param locationMonitorId a int.
     * @return a {@link java.util.List} object.
     */
    public List<OnmsResource> getResourcesForLocationMonitor(int locationMonitorId) {
        final ArrayList<OnmsResource> resources = Lists.newArrayList();
        final ResourcePath locationMonitorPath = ResourcePath.get(Integer.toString(locationMonitorId));

        /*
         * Verify that the node directory exists so we can throw a good
         * error message if not.
         */
        if (!m_resourceStorageDao.exists(locationMonitorPath)) {
            throw new ObjectRetrievalFailureException("The '" + getName() + "' resource type does not exist on this location monitor: " + locationMonitorId, null);
        }

        for (ResourcePath intfPath : m_resourceStorageDao.children(locationMonitorPath)) {
            String intfName = intfPath.getName();
            String defName = getDefinitionNameFromLocationMonitorDirectory(intfName);
            int id = getLocationMonitorIdFromLocationMonitorDirectory(intfName);
            resources.add(createResource(defName, id, intfName));
        }

        return resources;
    }

    private OnmsResource createResource(String definitionName,
            int locationMonitorId, String intf) {
        String monitor = definitionName + "-" + locationMonitorId;
        
        String label = intf + " from " + monitor;
        String resource = locationMonitorId + File.separator + intf;

        Set<OnmsAttribute> set =
            new LazySet<OnmsAttribute>(new AttributeLoader(definitionName, locationMonitorId,
                                            intf));
        return new OnmsResource(resource, label, this, set);
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean isResourceTypeOnNodeSource(String nodeSource, int nodeId) {
        // is this right?
        return false;
    }
    
    /** {@inheritDoc} */
    @Override
    public List<OnmsResource> getResourcesForNodeSource(String nodeSource, int nodeId) {
        // TODO: is this right?
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isResourceTypeOnDomain(String domain) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isResourceTypeOnNode(int nodeId) {
        return getResourcesForNode(nodeId).size() > 0;
    }

    private String getDefinitionNameFromLocationMonitorDirectory(String dir) {
        int index = dir.indexOf('-');
        if (index == -1) {
            throw new IllegalArgumentException("Location monitor directory \""
                                               + dir + "\" isn't a valid "
                                               + "location monitor directory");
        }
        return dir.substring(0, index);
    }

    private int getLocationMonitorIdFromLocationMonitorDirectory(String dir) {
        int index = dir.indexOf('-');
        if (index == -1) {
            throw new IllegalArgumentException("Location monitor directory \""
                                               + dir + "\" isn't a valid "
                                               + "location monitor directory");
        }
        return Integer.parseInt(dir.substring(index + 1));
    }

    public ResourcePath getRelativeInterfacePath(int id, String ipAddr) {
        return new ResourcePath(
                ResourceTypeUtils.RESPONSE_DIRECTORY,
                DISTRIBUTED_DIRECTORY,
                Integer.toString(id),
                ipAddr);
    }

    public class AttributeLoader implements LazySet.Loader<OnmsAttribute> {
        private String m_definitionName;
        private int m_locationMonitorId;
        private String m_intf;

        public AttributeLoader(String definitionName, int locationMonitorId, String intf) {
            m_definitionName = definitionName;
            m_locationMonitorId = locationMonitorId;
            m_intf = intf;
        }

        @Override
        public Set<OnmsAttribute> load() {
            LOG.debug("lazy-loading attributes for distributed status resource {}-{}/{}", m_definitionName, m_locationMonitorId, m_intf);

            return m_resourceStorageDao.getAttributes(getRelativeInterfacePath(m_locationMonitorId, m_intf));
        }
    }

    /** {@inheritDoc} */
    @Override
    public String getLinkForResource(OnmsResource resource) {
        return null;
    }
}
