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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.opennms.core.utils.LazySet;
import org.opennms.core.utils.PropertiesUtils;
import org.opennms.core.utils.PropertiesUtils.SymbolTable;
import org.opennms.netmgt.collection.api.StorageStrategy;
import org.opennms.netmgt.dao.api.ResourceStorageDao;
import org.opennms.netmgt.model.ExternalValueAttribute;
import org.opennms.netmgt.model.OnmsAttribute;
import org.opennms.netmgt.model.OnmsNode;
import org.opennms.netmgt.model.OnmsResource;
import org.opennms.netmgt.model.OnmsResourceType;
import org.opennms.netmgt.model.ResourcePath;
import org.opennms.netmgt.model.ResourceTypeUtils;
import org.opennms.netmgt.model.StringPropertyAttribute;
import org.springframework.orm.ObjectRetrievalFailureException;

/**
 * <p>GenericIndexResourceType class.</p>
 */
public class GenericIndexResourceType implements OnmsResourceType {
    private static final Pattern SUB_INDEX_PATTERN = Pattern.compile("^subIndex\\((.*)\\)$");
    private static final Pattern SUB_INDEX_ARGUMENTS_PATTERN = Pattern.compile("^(-?\\d+|n)(?:,\\s*(\\d+|n))?$");
    private static final Pattern HEX_PATTERN = Pattern.compile("^hex\\((.*)\\)$");
    private static final Pattern STRING_PATTERN = Pattern.compile("^string\\((.*)\\)$");

    private final String m_name;
    private final String m_label;
    private final String m_resourceLabelExpression;
    private final ResourceStorageDao m_resourceStorageDao;
    private final StorageStrategy m_storageStrategy;

    /**
     * <p>Constructor for GenericIndexResourceType.</p>
     *
     * @param resourceStorageDao a {@link org.opennms.netmgt.dao.api.ResourceStorageDao} object.
     * @param name a {@link java.lang.String} object.
     * @param label a {@link java.lang.String} object.
     * @param resourceLabelExpression a {@link java.lang.String} object.
     * @param storageStrategy a {@link org.opennms.netmgt.collection.api.StorageStrategy} object.
     */
    public GenericIndexResourceType(ResourceStorageDao resourceStorageDao, String name, String label, String resourceLabelExpression, StorageStrategy storageStrategy) {
        m_resourceStorageDao = resourceStorageDao;
        m_name = name;
        m_label = label;
        m_resourceLabelExpression = resourceLabelExpression;
        m_storageStrategy = storageStrategy;
    }
    
    /**
     * <p>getName</p>
     *
     * @return a {@link java.lang.String} object.
     */
    @Override
    public String getName() {
        return m_name;
    }
    
    /**
     * <p>getLabel</p>
     *
     * @return a {@link java.lang.String} object.
     */
    @Override
    public String getLabel() {
        return m_label;
    }
    
    /**
     * <p>getStorageStrategy</p>
     *
     * @return a {@link org.opennms.netmgt.collection.api.StorageStrategy} object.
     */
    public StorageStrategy getStorageStrategy() {
        return m_storageStrategy;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isResourceTypeOnNode(int nodeId) {
        return m_resourceStorageDao.exists(getResourceTypePath(nodeId, false));
    }

    private ResourcePath getResourceTypePath(int nodeId, boolean verify) {
        final ResourcePath nodePath = new ResourcePath(ResourceTypeUtils.SNMP_DIRECTORY, Integer.toString(nodeId));
        if (verify && !m_resourceStorageDao.exists(nodePath)) {
            throw new ObjectRetrievalFailureException(ResourcePath.class, "No node directory exists for node " + nodeId);
        }

        final ResourcePath indexPath = new ResourcePath(nodePath, getName());
        if (verify && !m_resourceStorageDao.exists(indexPath)) {
            throw new ObjectRetrievalFailureException(ResourcePath.class, "No node directory exists for generic index " + getName());
        }

        return indexPath;
    }

    private ResourcePath getResourceTypePath(String nodeSource, boolean verify) {
        final String[] ident = ResourceTypeUtils.getFsAndFidFromNodeSource(nodeSource);
        final ResourcePath nodeSourcePath = new ResourcePath(ResourceTypeUtils.SNMP_DIRECTORY, ResourceTypeUtils.FOREIGN_SOURCE_DIRECTORY, ident[0], ident[1]);
        if (verify && !m_resourceStorageDao.exists(nodeSourcePath)) {
            throw new ObjectRetrievalFailureException(ResourcePath.class, "No node directory exists for nodeSource " + nodeSource);
        }

        final ResourcePath indexPath = new ResourcePath(nodeSourcePath, getName());
        if (verify && !m_resourceStorageDao.exists(indexPath)) {
            throw new ObjectRetrievalFailureException(ResourcePath.class, "No node directory exists for generic index " + getName());
        }

        return indexPath;
    }

    /** {@inheritDoc} */
    @Override
    public List<OnmsResource> getResourcesForNode(int nodeId) {
        ArrayList<OnmsResource> resources = new ArrayList<OnmsResource>();

        List<String> indexes = getQueryableIndexesForNode(nodeId);
        for (String index : indexes) {
            resources.add(getResourceByNodeAndIndex(nodeId, index));
        }
        return OnmsResource.sortIntoResourceList(resources);
    }

    /** {@inheritDoc} */
    @Override
    public OnmsResource getChildByName(OnmsResource parent, String index) {
        // Grab the node entity
        final OnmsNode node = ResourceTypeUtils.getNodeFromResource(parent);

        OnmsResource resource;
        if (ResourceTypeUtils.isStoreByForeignSource()) {
            final String nodeSource = String.format("%s:%s", node.getForeignSource(), node.getForeignId());
            resource = getResourceByNodeSourceAndIndex(nodeSource, index);
        } else {
            resource = getResourceByNodeAndIndex(node.getId(), index);
        }
        resource.setParent(parent);
        return resource;
    }

    /**
     * <p>getQueryableIndexesForNode</p>
     *
     * @param nodeId a int.
     * @return a {@link java.util.List} object.
     */
    public List<String> getQueryableIndexesForNode(int nodeId) {
        ResourcePath path = getResourceTypePath(nodeId, true);
        return m_resourceStorageDao.children(path).stream()
               .map(rp -> rp.getName())
               .collect(Collectors.toList());
    }

    /**
     * <p>getQueryableIndexesForNodeSource</p>
     *
     * @param nodeSource a String.
     * @return a {@link java.util.List} object.
     */
    public List<String> getQueryableIndexesForNodeSource(String nodeSource) {
        ResourcePath path = getResourceTypePath(nodeSource, true);
        return m_resourceStorageDao.children(path).stream()
                .map(rp -> rp.getName())
                .collect(Collectors.toList());
    }

    /**
     * <p>getResourceByNodeAndIndex</p>
     *
     * @param nodeId a int.
     * @param index a {@link java.lang.String} object.
     * @return a {@link org.opennms.netmgt.model.OnmsResource} object.
     */
    public OnmsResource getResourceByNodeAndIndex(int nodeId, final String index) {
        final Set<OnmsAttribute> set = new LazySet<OnmsAttribute>(new AttributeLoader(nodeId, index));
        return getResourceByIndex(set, index);
    }
    
    /**
     * <p>getResourceByNodeSourceAndIndex</p>
     *
     * @param nodeSource a {@link java.lang.String} object.
     * @param index a {@link java.lang.String} object.
     * @return a {@link org.opennms.netmgt.model.OnmsResource} object.
     */
    public OnmsResource getResourceByNodeSourceAndIndex(String nodeSource, final String index) {
        final Set<OnmsAttribute> set = new LazySet<OnmsAttribute>(new NodeSourceAttributeLoader(nodeSource, index));
        return getResourceByIndex(set, index);
    }
    
    /**
     * <p>getResourceByIndex</p>
     *
     * @param set a Set<OnmsAttribute> object.
     * @param index a {@link java.lang.String} object.
     * @return a {@link org.opennms.netmgt.model.OnmsResource} object.
     */
    public OnmsResource getResourceByIndex(final Set<OnmsAttribute> set, final String index) {
    
        String label;
        if (m_resourceLabelExpression == null) {
            label = index;
        } else {
            SymbolTable symbolTable = new SymbolTable() {
                private int lastN;
                private boolean lastNSet = false;
                
                @Override
                public String getSymbolValue(String symbol) {
                    if (symbol.equals("index")) {
                        return index;
                    }
 
                    Matcher subIndexMatcher = SUB_INDEX_PATTERN.matcher(symbol);
                    if (subIndexMatcher.matches()) {
                        Matcher subIndexArgumentsMatcher = SUB_INDEX_ARGUMENTS_PATTERN.matcher(subIndexMatcher.group(1));
                        if (!subIndexArgumentsMatcher.matches()) {
                            // Invalid arguments
                            return null;
                        }
                        
                        List<String> indexElements = tokenizeIndex(index);
                        
                        int start;
                        int offset;
                        if ("n".equals(subIndexArgumentsMatcher.group(1)) && lastNSet) {
                            start = lastN;
                            lastNSet = false;
                        } else if ("n".equals(subIndexArgumentsMatcher.group(1))) {
                            // Invalid use of "n" when lastN is not set
                            return null;
                        } else {
                            offset = Integer.parseInt(subIndexArgumentsMatcher.group(1));
                            if (offset < 0) {
                                start = indexElements.size() + offset;
                            } else {
                                start = offset;
                            }
                        }

                        int end;
                        if ("n".equals(subIndexArgumentsMatcher.group(2))) {
                            end = start + Integer.parseInt(indexElements.get(start)) + 1;
                            start++;
                            lastN = end;
                            lastNSet = true;
                        } else {
                            if (subIndexArgumentsMatcher.group(2) == null) {
                                end = indexElements.size();
                            } else {                            
                                end = start + Integer.parseInt(subIndexArgumentsMatcher.group(2));
                            }
                        }
                        
                        if (start < 0 || start >= indexElements.size()) {
                            // Bogus index start
                            return null;
                        }
                        
                        if (end < 0 || end > indexElements.size()) {
                            // Bogus index end
                            return null;
                        }

                        StringBuffer indexSubString = new StringBuffer();
                        for (int i = start; i < end; i++) {
                            if (indexSubString.length() != 0) {
                                indexSubString.append(".");
                            }
                            
                            indexSubString.append(indexElements.get(i));
                        }
                        
                        return indexSubString.toString();
                    }
                    
                    Matcher hexMatcher = HEX_PATTERN.matcher(symbol);
                    if (hexMatcher.matches()) {
                        String subSymbol = getSymbolValue(hexMatcher.group(1));
                        List<String> indexElements = tokenizeIndex(subSymbol);
                        
                        StringBuffer hexString = new StringBuffer();
                        for (String indexElement : indexElements) {
                            if (hexString.length() > 0) {
                                hexString.append(":");
                            }
                            try {
                                hexString.append(String.format("%02X", Integer.parseInt(indexElement)));
                            } catch (NumberFormatException e) {
                                return null;
                            }
                        }
                        
                        return hexString.toString();
                    }
                    
                    Matcher stringMatcher = STRING_PATTERN.matcher(symbol);
                    if (stringMatcher.matches()) {
                        String subSymbol = getSymbolValue(stringMatcher.group(1));
                        List<String> indexElements = tokenizeIndex(subSymbol);
                        
                        StringBuffer stringString = new StringBuffer();
                        for (String indexElement : indexElements) {
                            stringString.append(String.format("%c", Integer.parseInt(indexElement)));
                        }
                        
                        return stringString.toString();
                    }
                    
                    for (OnmsAttribute attr : set) {
                        if (symbol.equals(attr.getName())) {
                            if (StringPropertyAttribute.class.isAssignableFrom(attr.getClass())) {
                                StringPropertyAttribute stringAttr = (StringPropertyAttribute) attr;
                                return stringAttr.getValue();
                            }
                            if (ExternalValueAttribute.class.isAssignableFrom(attr.getClass())) {
                                ExternalValueAttribute extAttr = (ExternalValueAttribute) attr;
                                return extAttr.getValue();
                            }
                        }
                    }
                    
                    return null;
                }

                private List<String> tokenizeIndex(final String index) {
                    List<String> indexElements = new ArrayList<String>();
                    StringTokenizer t = new StringTokenizer(index, ".");
                    while (t.hasMoreTokens()) {
                        indexElements.add(t.nextToken());
                    }
                    return indexElements;
                }
            };
            
            label = PropertiesUtils.substitute(m_resourceLabelExpression, symbolTable);
        }

        return new OnmsResource(index, label, this, set);
    }

    public class AttributeLoader implements LazySet.Loader<OnmsAttribute> {
    
        private int m_nodeId;
        private String m_index;

        public AttributeLoader(int nodeId, String index) {
            m_nodeId = nodeId;
            m_index = index;
        }

        @Override
        public Set<OnmsAttribute> load() {
            return m_resourceStorageDao.getAttributes(getPathForResource(m_nodeId, m_index));
        }
    }

    public class NodeSourceAttributeLoader implements LazySet.Loader<OnmsAttribute> {

        private String m_nodeSource;
        private String m_index;

        public NodeSourceAttributeLoader(String nodeSource, String index) {
            m_nodeSource = nodeSource;
            m_index = index;
        }

        @Override
        public Set<OnmsAttribute> load() {
            return m_resourceStorageDao.getAttributes(getPathForNodeSourceResource(m_nodeSource, m_index));
        }
    }

    /**
     * <p>getPathForResource</p>
     *
     * @param nodeId a int.
     * @param index a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     */
    public ResourcePath getPathForResource(int nodeId, String index) {
        return new ResourcePath(
                ResourceTypeUtils.SNMP_DIRECTORY,
                Integer.toString(nodeId),
                getName(),
                index);
    }

    /**
     * <p>getPathForNodeSourceResource</p>
     *
     * @param nodeSource a {@link java.lang.String} object.
     * @param index a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     */
    public ResourcePath getPathForNodeSourceResource(String nodeSource, String index) {
        final String[] ident = ResourceTypeUtils.getFsAndFidFromNodeSource(nodeSource);
        return new ResourcePath(
                ResourceTypeUtils.SNMP_DIRECTORY,
                ResourceTypeUtils.FOREIGN_SOURCE_DIRECTORY,
                ident[0],
                ident[1],
                getName(),
                index);
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
        return m_resourceStorageDao.exists(getResourceTypePath(nodeSource, false));
    }

    /** {@inheritDoc} */
    @Override
    public List<OnmsResource> getResourcesForNodeSource(String nodeSource, int nodeId) {
        ArrayList<OnmsResource> resources = new ArrayList<OnmsResource>();

        List<String> indexes = getQueryableIndexesForNodeSource(nodeSource);


        for (String index : indexes) {
            resources.add(getResourceByNodeSourceAndIndex(nodeSource, index));
        }
        return OnmsResource.sortIntoResourceList(resources);
    }
    
}
