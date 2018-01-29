/**
 * FILE: QuadtreePartitioning.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.QuadtreePartitioning.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package edu.gmu.stc.vector.parition;

import com.vividsolutions.jts.geom.Envelope;

import org.datasyslab.geospark.spatialPartitioning.quadtree.QuadRectangle;
import org.datasyslab.geospark.spatialPartitioning.quadtree.StandardQuadTree;

import java.io.Serializable;
import java.util.List;

import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta;

public class QuadtreePartitioning implements Serializable {

    /**
     * The Quad-Tree.
     */
    private final StandardQuadTree<Integer> partitionTree;

    /**
     * Instantiates a new Quad-Tree partitioning.
     *
     * @param samples the sample list
     * @param boundary   the boundary
     * @param partitions the partitions
     */
    public QuadtreePartitioning(List<ShapeFileMeta> samples, Envelope boundary, int partitions) throws Exception {
        this(samples, boundary, partitions, -1);
    }

    public QuadtreePartitioning(List<ShapeFileMeta> samples, Envelope boundary, final int partitions, int minTreeLevel)
            throws Exception {
        // Make sure the tree doesn't get too deep in case of data skew
        int maxLevel = partitions;
        int maxItemsPerNode = samples.size() / partitions;
        partitionTree = new StandardQuadTree(new QuadRectangle(boundary), 0,
            maxItemsPerNode, maxLevel);
        if (minTreeLevel > 0) {
            partitionTree.forceGrowUp(minTreeLevel);
        }

        for (final ShapeFileMeta sample : samples) {
            partitionTree.insert(new QuadRectangle(sample.getEnvelopeInternal()), 1);
        }

        partitionTree.assignPartitionIds();
    }

    public StandardQuadTree getPartitionTree()
    {
        return this.partitionTree;
    }
}
