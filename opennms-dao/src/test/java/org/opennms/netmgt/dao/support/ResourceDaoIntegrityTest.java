package org.opennms.netmgt.dao.support;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.opennms.core.test.ConfigurationTestUtils;
import org.opennms.core.test.OpenNMSJUnit4ClassRunner;
import org.opennms.core.utils.InetAddressUtils;
import org.opennms.netmgt.config.CollectdConfigFactory;
import org.opennms.netmgt.config.api.DataCollectionConfigDao;
import org.opennms.netmgt.config.datacollection.Parameter;
import org.opennms.netmgt.config.datacollection.PersistenceSelectorStrategy;
import org.opennms.netmgt.config.datacollection.ResourceType;
import org.opennms.netmgt.config.datacollection.StorageStrategy;
import org.opennms.netmgt.dao.mock.MockIpInterfaceDao;
import org.opennms.netmgt.dao.mock.MockLocationMonitorDao;
import org.opennms.netmgt.dao.mock.MockNodeDao;
import org.opennms.netmgt.filter.FilterDaoFactory;
import org.opennms.netmgt.filter.api.FilterDao;
import org.opennms.netmgt.model.OnmsAttribute;
import org.opennms.netmgt.model.OnmsIpInterface;
import org.opennms.netmgt.model.OnmsLocationMonitor;
import org.opennms.netmgt.model.OnmsLocationSpecificStatus;
import org.opennms.netmgt.model.OnmsMonitoredService;
import org.opennms.netmgt.model.OnmsNode;
import org.opennms.netmgt.model.OnmsResource;
import org.opennms.netmgt.model.OnmsResourceType;
import org.opennms.netmgt.model.ResourceVisitor;
import org.opennms.netmgt.rrd.RrdUtils;
import org.opennms.netmgt.rrd.rrdtool.JniRrdStrategy;
import org.opennms.test.JUnitConfigurationEnvironment;
import org.opennms.test.mock.EasyMockUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ContextConfiguration;

/**
 * This test verifies the integrity of the resource tree returned
 * by the resource dao.
 *
 * The test relies on the following files:
 *
 *   resource-tree-files.txt
 *      Structure of the rrd folder on disk. Generated with:
 *       cd /opt/opennms/share/rrd/ && find * -type f
 *   
 *   resource-tree-ips.txt
 *      IP addresses that are found in the resource tree. Generated with:
 *       psql -U opennms -t -A -F"," -c "select ipaddr from ipinterface"
 *  
 *   resource-tree-results.txt
 *      Ordered list of resource ids and their attributes.
 *
 * @author jwhite
 */
@RunWith(OpenNMSJUnit4ClassRunner.class)
@ContextConfiguration(locations={
        "classpath:/META-INF/opennms/applicationContext-soa.xml",
        "classpath:/META-INF/opennms/applicationContext-mockDao.xml"
})
@JUnitConfigurationEnvironment
public class ResourceDaoIntegrityTest {

    private EasyMockUtils m_easyMockUtils;
    private FilterDao m_filterDao;
    private CollectdConfigFactory m_collectdConfig;
    private DataCollectionConfigDao m_dataCollectionConfigDao;
    private DefaultResourceDao m_resourceDao;
    private FilesystemResourceStorageDao m_resourceStorageDao;

    @Autowired
    private MockNodeDao m_nodeDao;

    @Autowired
    private MockLocationMonitorDao m_locationMonitorDao;

    @Autowired
    private MockIpInterfaceDao m_ipInterfaceDao;

    @Rule
    public TemporaryFolder m_tempFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        RrdUtils.setStrategy(new JniRrdStrategy());

        m_easyMockUtils = new EasyMockUtils();
        m_dataCollectionConfigDao = m_easyMockUtils.createMock(DataCollectionConfigDao.class);
        m_filterDao = m_easyMockUtils.createMock(FilterDao.class);

        FilterDaoFactory.setInstance(m_filterDao);

        expect(m_filterDao.getActiveIPAddressList("IPADDR IPLIKE *.*.*.*")).andReturn(new ArrayList<InetAddress>(0)).anyTimes();

        m_easyMockUtils.replayAll();
        InputStream stream = ConfigurationTestUtils.getInputStreamForResource(this, "/collectdconfiguration-testdata.xml");
        m_collectdConfig = new CollectdConfigFactory(stream, "localhost", false);
        m_easyMockUtils.verifyAll();

        m_resourceStorageDao = new FilesystemResourceStorageDao();
        m_resourceStorageDao.setRrdDirectory(m_tempFolder.getRoot());

        m_resourceDao = new DefaultResourceDao();
        m_resourceDao.setNodeDao(m_nodeDao);
        m_resourceDao.setLocationMonitorDao(m_locationMonitorDao);
        m_resourceDao.setCollectdConfig(m_collectdConfig);
        m_resourceDao.setResourceStorageDao(m_resourceStorageDao);
        m_resourceDao.setDataCollectionConfigDao(m_dataCollectionConfigDao);
        m_resourceDao.setIpInterfaceDao(m_ipInterfaceDao);
    }

    @Test
    public void walkResourceTree() throws IOException {
        // Setup the file tree and the necessary objects in the DAOs
        createResourceTree();
        createNodes();
        createLocationMonitorObjects();
        Map<String, ResourceType> types = createResourceTypes();

        expect(m_dataCollectionConfigDao.getLastUpdate()).andReturn(new Date(System.currentTimeMillis())).anyTimes();
        expect(m_dataCollectionConfigDao.getConfiguredResourceTypes()).andReturn(types).anyTimes();

        m_easyMockUtils.replayAll();
        m_resourceDao.afterPropertiesSet();

        // Walk the tree and collect the results
        ResourceCollector visitor = new ResourceCollector();
        ResourceTreeWalker walker = new ResourceTreeWalker();
        walker.setResourceDao(m_resourceDao);
        walker.setVisitor(visitor);
        walker.walk();

        // We must have at least one resource for every known type
        for (OnmsResourceType type : m_resourceDao.getResourceTypes()) {
            assertTrue("No resources of type: " + type.getLabel(), visitor.resourceTypes.contains(type));
        }

        // We must be able to retrieve the same resource by id
        for (Entry<String, OnmsResource> entry : visitor.resourcesById.entrySet()) {
            OnmsResource resourceRetrievedById = m_resourceDao.getResourceById(entry.getKey());
            assertNotNull(String.format("Failed to retrieve resource with id '%s'.", entry.getKey()), resourceRetrievedById);
            assertEquals(String.format("Result mismatch for resource with id '%s'. Retrieved id is '%s'.", entry.getKey(), resourceRetrievedById.getId()),
                    entry.getValue().getName(), resourceRetrievedById.getName());
        }

        // Build a line that represent the resource for every unique id
        // and compare it to the known results
        int k = 0;
        String[] expectedResults = loadExpectedResults();
        for (Entry<String, OnmsResource> entry : visitor.resourcesById.entrySet()) {
            // Convert the attributes to strings and order them lexicographically
            Set<String> attributeNames = new TreeSet<String>();
            for (OnmsAttribute attribute : entry.getValue().getAttributes()) {
                attributeNames.add(attribute.toString());
            }

            // Compare
            String actualResult = entry.getKey() + ": " + attributeNames;
            assertEquals(String.format("Result mismatch at index %d.", k),
                    expectedResults[k], actualResult);
            k++;
        }

        // We should have as many unique resource ids as we have results
        assertEquals(expectedResults.length, visitor.resourcesById.size());

        m_easyMockUtils.verifyAll();
    }

    private static class ResourceCollector implements ResourceVisitor {
        private Map<String, OnmsResource> resourcesById = new TreeMap<String, OnmsResource>();

        private Set<OnmsResourceType> resourceTypes = new HashSet<OnmsResourceType>();

        @Override
        public void visit(OnmsResource resource) {
            resource.getResourceType();
            resourcesById.put(resource.getId(), resource);
            resourceTypes.add(resource.getResourceType());
        }
    }

    private String[] loadExpectedResults() throws IOException {
        String fileAsString = IOUtils.toString(new ClassPathResource("resource-tree-results.txt").getInputStream());
        return fileAsString.split("\\r?\\n");
    }

    private void createResourceTree() throws IOException {
        String fileAsString = IOUtils.toString(new ClassPathResource("resource-tree-files.txt").getInputStream());
        String[] resourceTreeFiles = fileAsString.split("\\r?\\n");

        // This should match the number of lines in the file
        assertEquals(31831, resourceTreeFiles.length);

        for (String resourceTreeFile : resourceTreeFiles) {
            // Create the file and its parent directories in the temporary folder
            File entry = new File(m_tempFolder.getRoot(), resourceTreeFile);
            entry.getParentFile().mkdirs();
            assertTrue("Failed to create " + entry, entry.createNewFile());
        }
    }

    /**
     * Creates a set of nodes and assigns a single IP address from
     * the resource-tree-ips.txt file to each node.
     */
    private void createNodes() throws IOException {
        final int NUM_NODES = 250;
        
        String fileAsString = IOUtils.toString(new ClassPathResource("resource-tree-ips.txt").getInputStream());
        String[] resourceTreeIps = fileAsString.split("\\r?\\n");

        // Make sure every IP address is represented at lease once
        assertTrue(resourceTreeIps.length < NUM_NODES);

        for (int i = 0; i < NUM_NODES; i++) {
            OnmsNode n = new OnmsNode();
            n.setId(i);
            n.setLabel("node" + i);
            n.setForeignSource("NODES");
            n.setForeignId(Integer.toString(i));
            OnmsIpInterface ip = new OnmsIpInterface();
            ip.setId(10 + i);
            ip.setIpAddress(InetAddressUtils.addr(resourceTreeIps[i % resourceTreeIps.length]));
            ip.setNode(n);
            n.addIpInterface(ip);

            m_nodeDao.save(n);
        }
    }

    /**
     * Setup location monitor objects so that we can test the
     * DistributedStatusResourceType on this particular entry:
     *    response/distributed/1/172.20.1.40/http.jrb
     */
    private void createLocationMonitorObjects() {
        OnmsLocationMonitor locationMonitor = new OnmsLocationMonitor();
        locationMonitor.setId(1);
        OnmsIpInterface intf = new OnmsIpInterface();
        intf.setId(1);
        intf.setIpAddress(InetAddressUtils.addr("172.20.1.40"));
        OnmsMonitoredService svc = new OnmsMonitoredService();
        svc.setIpInterface(intf);

        OnmsLocationSpecificStatus status = new OnmsLocationSpecificStatus();
        status.setLocationMonitor(locationMonitor);
        status.setMonitoredService(svc);
        m_locationMonitorDao.saveStatusChange(status);
    }

    /**
     * Define a resource type so that test the GenericIndexResourceType
     */
    private Map<String, ResourceType> createResourceTypes() {
        Map<String, ResourceType> types = new HashMap<String, ResourceType>();

        ResourceType hrStorageIndex = new ResourceType();
        hrStorageIndex.setName("hrStorageIndex");
        hrStorageIndex.setLabel("Storage (SNMP MIB-2 Host Resources)");
        hrStorageIndex.setResourceLabel("${hrStorageDescr}");
        hrStorageIndex.setPersistenceSelectorStrategy(new PersistenceSelectorStrategy("org.opennms.netmgt.collectd.PersistAllSelectorStrategy"));
        StorageStrategy storageStrategy = new StorageStrategy("org.opennms.netmgt.dao.support.SiblingColumnStorageStrategy");
        storageStrategy.addParameter(new Parameter("sibling-column-name", "hrStorageDescr"));
        storageStrategy.addParameter(new Parameter("replace-first", "s/^-$/_root_fs/"));
        storageStrategy.addParameter(new Parameter("replace-all", "s/^-//"));
        storageStrategy.addParameter(new Parameter("replace-all", "s/\\s//"));
        storageStrategy.addParameter(new Parameter("replace-all", "s/:\\\\.*//"));
        hrStorageIndex.setStorageStrategy(storageStrategy);
        types.put(hrStorageIndex.getName(), hrStorageIndex);

        return types;
    }
}
