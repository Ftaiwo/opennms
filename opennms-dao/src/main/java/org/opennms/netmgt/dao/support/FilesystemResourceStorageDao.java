package org.opennms.netmgt.dao.support;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.opennms.netmgt.dao.api.ResourceStorageDao;
import org.opennms.netmgt.model.OnmsAttribute;
import org.opennms.netmgt.model.ResourcePath;
import org.opennms.netmgt.model.ResourceTypeUtils;
import org.opennms.netmgt.rrd.RrdUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class FilesystemResourceStorageDao implements ResourceStorageDao {

    private static final Logger LOG = LoggerFactory.getLogger(FilesystemResourceStorageDao.class);

    private File m_rrdDirectory;

    @Override
    public boolean exists(ResourcePath path) {
        return exists(toFile(path).toPath());
    }

    @Override
    public Set<ResourcePath> children(ResourcePath path) {
        final Set<ResourcePath> children = Sets.newTreeSet();

        File root = toFile(path);
        String[] entries = root.list();
        if (entries == null) {
            return children;
        }

        for (String entry : entries) {
            File child = new File(root, entry);
            if (!child.isDirectory()) {
                continue;
            }

            if (exists(child.toPath())) {
                children.add(new ResourcePath(path, entry));
            }
        }

        return children;
    }

    @Override
    public Set<OnmsAttribute> getAttributes(ResourcePath path) {
        return ResourceTypeUtils.getAttributesAtRelativePath(m_rrdDirectory, toRelativePath(path));
    }

    @Override
    public Map<String, String> getMetaData(ResourcePath path) {
        return RrdUtils.readMetaDataFile(getRrdDirectory().getAbsolutePath(), toRelativePath(path));
    }

    private boolean exists(Path root) {
        try {
            if (Files.walk(root).anyMatch(isRrdFile)) {
                return true;
            }
        } catch (IOException e) {
            LOG.error("Failed to walk {}. Marking path as non-existent.", root, e);
        }

        return false;
    }

    private File toFile(ResourcePath path) {
        return Paths.get(m_rrdDirectory.getAbsolutePath(), path.elements()).toFile();
    }

    private String toRelativePath(ResourcePath path) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (final String el : path) {
            if (!first) {
                sb.append(File.separator);
            } else {
                first = false;
            }
            sb.append(el);
        }
        return sb.toString();
    }

    public void setRrdDirectory(File rrdDirectory) {
        m_rrdDirectory = rrdDirectory;
    }

    public File getRrdDirectory() {
        return m_rrdDirectory;
    }

    @Override
    public boolean delete(ResourcePath path) {
        return deleteDir(toFile(path));
    }

    private static Predicate<Path> isRrdFile = new Predicate<Path>() {
        @Override
        public boolean test(Path path) {
            final File file = path.toFile();
            return file.isFile() && file.getName().endsWith(RrdUtils.getExtension());
        }
    };

    /**
     * Deletes all files and sub-directories under the specified directory
     * If a deletion fails, the method stops attempting to delete and returns
     * false.
     * 
     * @return true if all deletions were successful, false otherwise.
     */
    private static boolean deleteDir(File file) {
        // If this file is a directory, delete all of its children
        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                if (!deleteDir(child)) {
                    return false;
                }
            }
        }

        // Delete the file/directory itself
        boolean successful = file.delete();
        if (!successful) {
            LOG.warn("Failed to delete file: {}", file.getAbsolutePath());
        }

        return successful;
    }
}
