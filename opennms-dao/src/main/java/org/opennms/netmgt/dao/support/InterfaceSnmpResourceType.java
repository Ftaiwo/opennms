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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.opennms.core.utils.AlphaNumeric;
import org.opennms.core.utils.InetAddressUtils;
import org.opennms.core.utils.LazySet;
import org.opennms.core.utils.SIUtils;
import org.opennms.netmgt.dao.api.NodeDao;
import org.opennms.netmgt.dao.api.ResourceStorageDao;
import org.opennms.netmgt.model.ExternalValueAttribute;
import org.opennms.netmgt.model.OnmsAttribute;
import org.opennms.netmgt.model.OnmsIpInterface;
import org.opennms.netmgt.model.OnmsNode;
import org.opennms.netmgt.model.OnmsResource;
import org.opennms.netmgt.model.OnmsResourceType;
import org.opennms.netmgt.model.OnmsSnmpInterface;
import org.opennms.netmgt.model.ResourcePath;
import org.opennms.netmgt.model.ResourceTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.orm.ObjectRetrievalFailureException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * FIXME Note: We should remove any graphs from the list that have external
 * values.  See bug #1703.
 */
public class InterfaceSnmpResourceType implements OnmsResourceType {

    private static final Logger LOG = LoggerFactory.getLogger(InterfaceSnmpResourceType.class);

    private final ResourceStorageDao m_resourceStorageDao;
    private final NodeDao m_nodeDao;

    /**
     * <p>Constructor for InterfaceSnmpResourceType.</p>
     *
     * @param resourceStorageDao a {@link org.opennms.netmgt.dao.api.ResourceStorageDao} object.
     * @param nodeDao a {@link org.opennms.netmgt.dao.api.NodeDao} object.
     */
    public InterfaceSnmpResourceType(ResourceStorageDao resourceStorageDao, NodeDao nodeDao) {
        m_resourceStorageDao = resourceStorageDao;
        m_nodeDao = nodeDao;
    }

    /**
     * <p>getName</p>
     *
     * @return a {@link java.lang.String} object.
     */
    @Override
    public String getName() {
        return "interfaceSnmp";
    }

    /**
     * <p>getLabel</p>
     *
     * @return a {@link java.lang.String} object.
     */
    @Override
    public String getLabel() {
        return "SNMP Interface Data";
    }

    /** {@inheritDoc} */
    @Override
    public boolean isResourceTypeOnNode(int nodeId) {
        return isResourceTypeOnParentResource(Integer.toString(nodeId));
    }

    private boolean isResourceTypeOnParentResource(String... pathEls) {
        ResourcePath path = getParentResourcePath(false, pathEls);
        return m_resourceStorageDao.children(path).size() > 0;
    }

    private ResourcePath getParentResourcePath(boolean verify, String... pathEls) {
        ResourcePath path = new ResourcePath(new ResourcePath(ResourceTypeUtils.SNMP_DIRECTORY), pathEls);

        if (verify && !m_resourceStorageDao.exists(path)) {
            throw new ObjectRetrievalFailureException(ResourcePath.class, "No parent resource directory exists for " + path);
        }

        return path;
    }

    /** {@inheritDoc} */
    @Override
    public List<OnmsResource> getResourcesForNode(int nodeId) {
        OnmsNode node = m_nodeDao.get(nodeId);
        if (node == null) {
            throw new ObjectRetrievalFailureException(OnmsNode.class, Integer.toString(nodeId), "Could not find node with node ID " + nodeId, null);
        }

        ResourcePath parent = getParentResourcePath(true, Integer.toString(nodeId));
        return OnmsResource.sortIntoResourceList(populateResourceList(parent, node, false));
    }

    /** {@inheritDoc} */
    @Override
    public OnmsResource getChildByName(final OnmsResource parent, final String name) {
        if (parent.getResourceType() instanceof DomainResourceType) {
            // Load all of the resources and search when dealing with domains.
            // This is not efficient, but resources of this type should be sparse.
            for (final OnmsResource resource : getResourcesForDomain(parent.getName())) {
                if (resource.getName().equals(name)) {
                    return resource;
                }
            }
            throw new ObjectRetrievalFailureException(OnmsResource.class, "No child with name '" + name + "' found on '" + parent + "'");
        }

        // Grab the node entity
        final OnmsNode node = ResourceTypeUtils.getNodeFromResource(parent);

        // Determine the parent folder
        ResourcePath parentPath = null;
        Boolean isForeign = ResourceTypeUtils.isStoreByForeignSource();

        if (isForeign) {
            parentPath = getParentResourcePath(true, ResourceTypeUtils.FOREIGN_SOURCE_DIRECTORY, node.getForeignSource(), node.getForeignId());
        } else {
            parentPath = getParentResourcePath(true, Integer.toString(node.getId()));
        }

        // Verify that the requested resource exists
        final ResourcePath resourcePath = new ResourcePath(parentPath, name);
        if (!m_resourceStorageDao.exists(resourcePath)) {
            throw new ObjectRetrievalFailureException(OnmsResource.class, "No resource with name '" + name + "' found.");
        }

        // Leverage the existing function for retrieving the resource list
        final List<OnmsResource> resources = populateResourceList(parentPath, Sets.newHashSet(name), node, isForeign);
        if (resources.size() != 1) {
            throw new ObjectRetrievalFailureException(OnmsResource.class, "No resource with name '" + name + "' found.");
        }

        final OnmsResource resource = resources.get(0);
        resource.setParent(parent);
        return resource;
    }

    private List<OnmsResource> populateResourceList(ResourcePath parent, OnmsNode node, Boolean isForeign) {
        final Set<String> intfNames = m_resourceStorageDao.children(parent).stream()
                .map(rp -> rp.getName())
                .collect(Collectors.toSet());
        return populateResourceList(parent, intfNames, node, isForeign);
    }

    private List<OnmsResource> populateResourceList(ResourcePath parent, Set<String> intfNames, OnmsNode node, Boolean isForeign) {
            
        ArrayList<OnmsResource> resources = new ArrayList<OnmsResource>();

        Set<OnmsSnmpInterface> snmpInterfaces = node.getSnmpInterfaces();
        Map<String, OnmsSnmpInterface> intfMap = new HashMap<String, OnmsSnmpInterface>();

        for (OnmsSnmpInterface snmpInterface : snmpInterfaces) {
            /*
             * When Cisco Express Forwarding (CEF) or some ATM encapsulations
             * (AAL5) are used on Cisco routers, an additional entry might be 
             * in the ifTable for these sub-interfaces, but there is no
             * performance data available for collection.  This check excludes
             * ifTable entries where ifDescr contains "-cef".  See bug #803.
             */
            if (snmpInterface.getIfDescr() != null) {
                if (Pattern.matches(".*-cef.*", snmpInterface.getIfDescr())) {
                    continue;
                }
            }

            String replacedIfName = AlphaNumeric.parseAndReplace(snmpInterface.getIfName(), '_');
            String replacedIfDescr = AlphaNumeric.parseAndReplace(snmpInterface.getIfDescr(), '_');
            
            String[] keys = new String[] {
                    replacedIfName + "-",
                    replacedIfDescr + "-",
                    replacedIfName + "-" + snmpInterface.getPhysAddr(),
                    replacedIfDescr + "-" + snmpInterface.getPhysAddr()
            };
            
            for (String key : keys) {
                if (!intfMap.containsKey(key)) {
                    intfMap.put(key, snmpInterface);
                }
            }
        }

        for (String intfName : intfNames) {
            String desc = intfName;
            String mac = "";

            // Strip off the MAC address from the end, if there is one
            int dashIndex = intfName.lastIndexOf('-');

            if (dashIndex >= 0) {
                desc = intfName.substring(0, dashIndex);
                mac = intfName.substring(dashIndex + 1, intfName.length());
            }

            String key = desc + "-" + mac; 
            OnmsSnmpInterface snmpInterface = intfMap.get(key);
            
            String label;
            Long ifSpeed = null;
            String ifSpeedFriendly = null;
            if (snmpInterface == null) {
                label = intfName + " (*)";
            } else {
                StringBuffer descr = new StringBuffer();
                StringBuffer parenString = new StringBuffer();

                if (snmpInterface.getIfAlias() != null) {
                    parenString.append(snmpInterface.getIfAlias());
                }
                // Append all of the IP addresses on this ifindex
                for (OnmsIpInterface ipif : snmpInterface.getIpInterfaces()) {
                    String ipaddr = InetAddressUtils.str(ipif.getIpAddress());
                    if (!"0.0.0.0".equals(ipaddr)) {
                        if (parenString.length() > 0) {
                            parenString.append(", ");
                        }
                        parenString.append(ipaddr);
                    }
                }
                if ((snmpInterface.getIfSpeed() != null) && (snmpInterface.getIfSpeed() != 0)) {
                    ifSpeed = snmpInterface.getIfSpeed();
                    ifSpeedFriendly = SIUtils.getHumanReadableIfSpeed(ifSpeed);
                    if (parenString.length() > 0) {
                        parenString.append(", ");
                    }
                    parenString.append(ifSpeedFriendly);
                }

                if (snmpInterface.getIfName() != null) {
                    descr.append(snmpInterface.getIfName());
                } else if (snmpInterface.getIfDescr() != null) {
                    descr.append(snmpInterface.getIfDescr());
                } else {
                    /*
                     * Should never reach this point, since ifLabel is based on
                     * the values of ifName and ifDescr but better safe than sorry.
                     */
                    descr.append(intfName);
                }

                /* Add the extended information in parenthesis after the ifLabel,
                 * if such information was found.
                 */
                if (parenString.length() > 0) {
                    descr.append(" (");
                    descr.append(parenString);
                    descr.append(")");
                }

                label = descr.toString();
            }

            OnmsResource resource = getResourceByParentPathAndInterface(parent, intfName, label, ifSpeed, ifSpeedFriendly);
            if (snmpInterface != null) {
                Set<OnmsIpInterface> ipInterfaces = snmpInterface.getIpInterfaces();
                if (ipInterfaces.size() > 0) {
                    int id = ipInterfaces.iterator().next().getId();
                    resource.setLink("element/interface.jsp?ipinterfaceid=" + id);
                } else {
                    int ifIndex = snmpInterface.getIfIndex();
                    if(ifIndex > -1) {
                        resource.setLink("element/snmpinterface.jsp?node=" + node.getNodeId() + "&ifindex=" + ifIndex);
                    }
                }

                resource.setEntity(snmpInterface);
            } else {
                LOG.debug("populateResourceList: snmpInterface is null");
            }
            LOG.debug("populateResourceList: adding resource toString {}", resource.toString());
            resources.add(resource);
        }
        
        return resources; 
    }

    private OnmsResource getResourceByParentPathAndInterface(ResourcePath path, String intf, String label, Long ifSpeed, String ifSpeedFriendly) throws DataAccessException {
        Set<OnmsAttribute> set = new LazySet<OnmsAttribute>(new AttributeLoader(path, ifSpeed, ifSpeedFriendly));
        return new OnmsResource(intf, label, this, set);
    }

    public class AttributeLoader implements LazySet.Loader<OnmsAttribute> {
        private ResourcePath m_path;
        private Long m_ifSpeed;
        private String m_ifSpeedFriendly;

        public AttributeLoader(ResourcePath path, Long ifSpeed, String ifSpeedFriendly) {
            m_path = path;
            m_ifSpeed = ifSpeed;
            m_ifSpeedFriendly = ifSpeedFriendly;
        }

        @Override
        public Set<OnmsAttribute> load() {
            Set<OnmsAttribute> attributes = m_resourceStorageDao.getAttributes(m_path);
            if (m_ifSpeed != null) {
                attributes.add(new ExternalValueAttribute("ifSpeed", m_ifSpeed.toString()));
            }
            if (m_ifSpeedFriendly != null) {
                attributes.add(new ExternalValueAttribute("ifSpeedFriendly", m_ifSpeedFriendly));
            }
            return attributes;
        }
        
    }

    /**
     * {@inheritDoc}
     *
     * This resource type is never available for domains.
     * Only the interface resource type is available for domains.
     */
    @Override
    public boolean isResourceTypeOnDomain(String domain) {
        return getQueryableInterfacesForDomain(domain).size() > 0;
    }
    
    /** {@inheritDoc} */
    @Override
    public List<OnmsResource> getResourcesForDomain(String domain) {
        ArrayList<OnmsResource> resources =
            new ArrayList<OnmsResource>();

        List<String> ifaces = getQueryableInterfacesForDomain(domain);
        for (String iface : ifaces) {
            OnmsResource resource = getResourceByDomainAndInterface(domain, iface); 
            try {
                resource.setLink("element/nodeList.htm?listInterfaces=true&snmpParm=ifAlias&snmpParmMatchType=contains&snmpParmValue=" + URLEncoder.encode(iface, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("URLEncoder.encode complained about UTF-8. " + e, e);
            }
            resources.add(resource);
        }

        return OnmsResource.sortIntoResourceList(resources);
    }

    private List<String> getQueryableInterfacesForDomain(String domain) {
        Preconditions.checkNotNull(domain, "domain argument");
        return m_resourceStorageDao.children(ResourcePath.get(ResourceTypeUtils.SNMP_DIRECTORY, domain)).stream()
            .map(rp -> rp.getName())
            .collect(Collectors.toList());
    }

    private OnmsResource getResourceByDomainAndInterface(String domain, String intf) {
        Set<OnmsAttribute> set = new LazySet<OnmsAttribute>(new AttributeLoader(ResourcePath.get(domain, intf), null, null));
        return new OnmsResource(intf, intf, this, set);
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
        return isResourceTypeOnParentResource(ResourceTypeUtils.FOREIGN_SOURCE_DIRECTORY, ident[0], ident[1]);
    }

    /** {@inheritDoc} */
    @Override
    public List<OnmsResource> getResourcesForNodeSource(String nodeSource, int nodeId) {
        String[] ident = ResourceTypeUtils.getFsAndFidFromNodeSource(nodeSource);
        OnmsNode node = m_nodeDao.findByForeignId(ident[0], ident[1]);
        if (node == null) {
            throw new ObjectRetrievalFailureException(OnmsNode.class, nodeSource, "Could not find node with nodeSource " + nodeSource, null);
        }
        ResourcePath parent = getParentResourcePath(true, ResourceTypeUtils.FOREIGN_SOURCE_DIRECTORY, ident[0], ident[1]);
        return OnmsResource.sortIntoResourceList(populateResourceList(parent, node, true));
    }

}
