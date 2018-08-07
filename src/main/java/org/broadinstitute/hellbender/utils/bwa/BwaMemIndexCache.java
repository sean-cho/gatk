package org.broadinstitute.hellbender.utils.bwa;

import htsjdk.samtools.BamFileIoUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manage a global collection of {@link BwaMemIndex} instances.
 */
public class BwaMemIndexCache {

    private static final Logger logger = LogManager.getLogger(BwaMemIndexCache.class);

    private static final BwaMemIndexCache global = new BwaMemIndexCache();

    private final Map<String, BwaMemIndex> instances = new HashMap<>();

    /**
     * Returns a {@link BwaMemIndex} instance that corresponds to  given index image file.
     * @param indexImageFile the target image file.
     * @return never {@code null}.
     */
    public static synchronized BwaMemIndex getGlobalInstance(final String indexImageFile ) {
        return global.getInstance(indexImageFile);
    }

    public BwaMemIndex getInstance(final String indexImageFile) {
        return instances.computeIfAbsent(indexImageFile, fileName -> new BwaMemIndex(fileName));
    }

    /**
     * Closes all instances in the VM.
     */
    public static synchronized void closeGlobalInstances() {
        global.closeInstances();
    }

    public void closeInstances() {
        instances.values().forEach(BwaMemIndex::close);
        instances.clear();
    }

    public void closeInstanceAndDeleteFiles() {
        instances.values().forEach(BwaMemIndex::close);
        instances.keySet().forEach(file -> {
            try {
                if (BucketUtils.fileExists(file)) {
                    BucketUtils.deleteFile(file);
                }
            } catch (final IOException e) {
                logger.warn("Could not delete index file: " + file);
            }
        });
    }

    /**
     * Closes all instances in all the VMs involved in the spark context provided.
     * @param ctx 
     */
    public static void closeAllDistributedGlobalInstances(final JavaSparkContext ctx ) {
        int nJobs = ctx.defaultParallelism();
        final List<Integer> jobList = new ArrayList<>(nJobs);
        for ( int idx = 0; idx != nJobs; ++idx ) jobList.add(idx);
        ctx.parallelize(jobList, nJobs).foreach(idx -> closeGlobalInstances());
    }
}
