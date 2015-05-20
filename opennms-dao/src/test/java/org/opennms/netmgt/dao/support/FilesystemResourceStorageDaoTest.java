package org.opennms.netmgt.dao.support;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.opennms.netmgt.model.ResourcePath;
import org.opennms.netmgt.rrd.RrdUtils;

public class FilesystemResourceStorageDaoTest {

    private FilesystemResourceStorageDao m_fsResourceStorageDao = new FilesystemResourceStorageDao();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() {
        m_fsResourceStorageDao.setRrdDirectory(tempFolder.getRoot());
    }

    @Test
    public void exists() throws IOException {
        // Path is missing when the folder is missing
        assertFalse(m_fsResourceStorageDao.exists(ResourcePath.get("should", "not", "exist")));

        // Path is missing when the folder is empty
        File folder = tempFolder.newFolder("a");
        assertFalse(m_fsResourceStorageDao.exists(ResourcePath.get("a")));

        // Path is missing when it only contains an empty sub-folder
        File subFolder = tempFolder.newFolder("a", "b");
        assertFalse(m_fsResourceStorageDao.exists(ResourcePath.get("a")));

        // Path exists when the sub-folder contains an RRD file
        File rrd = new File(subFolder, "ds" + RrdUtils.getExtension());
        rrd.createNewFile();
        assertTrue(m_fsResourceStorageDao.exists(ResourcePath.get("a")));
        assertTrue(rrd.delete());

        // Path exists when the folder contains an RRD file
        rrd = new File(folder, "ds" + RrdUtils.getExtension());
        rrd.createNewFile();
        assertTrue(m_fsResourceStorageDao.exists(ResourcePath.get("a")));
    }

    @Test
    public void children() throws IOException {
        // Children are empty when the folder is missing
        assertEquals(0, m_fsResourceStorageDao.children(ResourcePath.get("should", "not", "exist")).size());

        // Children are empty when the folder is emtpy
        File folder = tempFolder.newFolder("a");
        assertEquals(0, m_fsResourceStorageDao.children(ResourcePath.get("a")).size());

        // Children are empty when the folder only contains an RRD file
        File rrd = new File(folder, "ds" + RrdUtils.getExtension());
        rrd.createNewFile();
        assertEquals(0, m_fsResourceStorageDao.children(ResourcePath.get("a")).size());
        assertTrue(rrd.delete());

        // Children are empty when the folder only contains an empty sub-folder
        File subFolder = tempFolder.newFolder("a", "b");
        assertEquals(0, m_fsResourceStorageDao.children(ResourcePath.get("a")).size());

        // Child exists when the sub-folder contains an RRD file
        rrd = new File(subFolder, "ds" + RrdUtils.getExtension());
        rrd.createNewFile();
        Set<ResourcePath> children = m_fsResourceStorageDao.children(ResourcePath.get("a"));
        assertEquals(1, children.size());
        assertEquals(ResourcePath.get("a", "b"), children.iterator().next());
        assertTrue(rrd.delete());
    }
}
