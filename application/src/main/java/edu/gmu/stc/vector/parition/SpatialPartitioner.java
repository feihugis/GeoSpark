/**
 * FILE: SpatialPartitioner.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package edu.gmu.stc.vector.parition;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import org.apache.spark.Partitioner;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.joinJudgement.DedupParams;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta;
import scala.Tuple2;

abstract public class SpatialPartitioner extends Partitioner implements Serializable{

	protected final GridType gridType;
	protected final List<Envelope> grids;

	protected SpatialPartitioner(GridType gridType, List<Envelope> grids)
	{
		this.gridType = gridType;
		this.grids = Objects.requireNonNull(grids, "grids");
	}

	/**
	 * Given a geometry, returns a list of partitions it overlaps.
	 *
	 * For points, returns exactly one partition as long as grid type is non-overlapping.
	 * For other geometry types or for overlapping grid types, may return multiple partitions.
	 */
	abstract public Iterator<Tuple2<Integer, ShapeFileMeta>>
	placeObject(ShapeFileMeta spatialObject) throws Exception;

	@Nullable
	abstract public DedupParams getDedupParams();

	public GridType getGridType() {
		return gridType;
	}

	public List<Envelope> getGrids() {
		return grids;
	}

	@Override
	public int getPartition(Object key) {
		return (int)key;
	}
}
