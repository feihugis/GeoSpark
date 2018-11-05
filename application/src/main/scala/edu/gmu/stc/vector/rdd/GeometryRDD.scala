package edu.gmu.stc.vector.rdd

import com.vividsolutions.jts.geom.{Geometry, GeometryFactory, Polygon}
import com.vividsolutions.jts.index.SpatialIndex
import edu.gmu.stc.config.ConfigParameter
import edu.gmu.stc.vector.operation.{FileConverter, OperationUtil}
import edu.gmu.stc.vector.parition.PartitionUtil
import edu.gmu.stc.vector.rdd.index.IndexOperator
import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta
import edu.gmu.stc.vector.shapefile.reader.GeometryReaderUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner
import org.wololo.geojson.{Feature, FeatureCollection}
import org.wololo.jts2geojson.GeoJSONWriter

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * Created by Fei Hu on 1/26/18.
  */
class GeometryRDD extends Logging{
  private var geometryRDD: RDD[Geometry] = _
  private var indexedGeometryRDD: RDD[SpatialIndex] = _
  private var partitioner: SpatialPartitioner = _

  def initialize(shapeFileMetaRDD: ShapeFileMetaRDD, hasAttribute: Boolean = false): Unit = {
    this.geometryRDD = shapeFileMetaRDD.getShapeFileMetaRDD.mapPartitions(itor => {
      val shapeFileMetaList = itor.toList
      if (hasAttribute) {
        GeometryReaderUtil.readGeometriesWithAttributes(shapeFileMetaList.asJava).asScala.toIterator
      } else {
        GeometryReaderUtil.readGeometries(shapeFileMetaList.asJava).asScala.toIterator
      }
    })

    this.partitioner = shapeFileMetaRDD.getPartitioner
  }

  def initializePartitioner(sc: SparkContext, gridType: GridType, partitionNum: Int): Unit = {
    if (this.geometryRDD == null) {
      throw new IllegalAccessError("Please generate the geometryRDD first")
    }

    val fraction = sc.hadoopConfiguration.get(ConfigParameter.RDD_SAMPLING_FRACTION).toDouble
    val envelopes = this.geometryRDD
      .sample(true, fraction)
      .map(geometry => geometry.getEnvelopeInternal)
      .collect().toList
    this.partitioner = PartitionUtil.spatialPartitioning(gridType, partitionNum, envelopes.asJava)
  }

  def partition(partition: SpatialPartitioner): Unit = {
    this.partitioner = partition
    this.geometryRDD = this.geometryRDD.flatMap(geometry => partition.placeObject(geometry).asScala)
      .partitionBy(partition).map(_._2)
  }

  def intersect(shapeFileMetaRDD1: ShapeFileMetaRDD, shapeFileMetaRDD2: ShapeFileMetaRDD, partitionNum: Int): Unit = {
    var joinRDD: RDD[(ShapeFileMeta, ShapeFileMeta)] = shapeFileMetaRDD1.spatialJoin(shapeFileMetaRDD2, partitionNum)
      .sortBy({case (shapeFileMeta1, shapeFileMeta2) => shapeFileMeta1.getShp_offset})
      .repartition(partitionNum)

    joinRDD = joinRDD.cache()

    //logInfo("************** Number of elements in JoinedRDD: %d".format(joinRDD.count()))

    val t1 = System.currentTimeMillis()

    this.geometryRDD = joinRDD.mapPartitions(IndexOperator.spatialIntersect)
    //this.geometryRDD = this.geometryRDD.repartition(5)

    val t2 = System.currentTimeMillis()

    //logInfo("******** Intersection takes: %d".format((t2 - t1)/1000))
  }

  def getGeometryRDD: RDD[Geometry] = this.geometryRDD

  def setGeometryRDD(rdd: RDD[Geometry]): Unit = {
    this.geometryRDD = rdd
  }

  def cache(): Unit = {
    this.geometryRDD = this.geometryRDD.cache()
  }

  def uncache(blocking: Boolean = true): Unit = {
    this.geometryRDD.unpersist(blocking)
  }

  def indexPartition(indexType: IndexType) = {
    val indexBuilder = new IndexOperator(indexType.toString)
    this.indexedGeometryRDD = this.geometryRDD.mapPartitions(indexBuilder.buildIndex)
  }

  def intersect(other: GeometryRDD): GeometryRDD = {
    val geometryRDD = new GeometryRDD
    geometryRDD.geometryRDD = this.indexedGeometryRDD.zipPartitions(other.geometryRDD)(IndexOperator.geoSpatialIntersection)
    geometryRDD.geometryRDD = geometryRDD.geometryRDD.filter(geometry => !geometry.isEmpty)
    geometryRDD
  }

  def intersectV2(other: GeometryRDD, partitionNum: Int): GeometryRDD = {
    val geometryRDD = new GeometryRDD
    val pairedRDD= this.indexedGeometryRDD.zipPartitions(other.geometryRDD)(IndexOperator.geoSpatialJoin)

    geometryRDD.geometryRDD = pairedRDD
      .map({case (g1, g2) => {
        (g1.hashCode() + "_" + g2.hashCode(), (g1, g2))
      }})
      .reduceByKey((v1, v2) => v1)
        .map(tuple => tuple._2)
      .repartition(partitionNum)
      .map({case(g1, g2) => {
        g1.intersection(g2)
      }})
      .filter(g => !g.isEmpty)
      .map(g => (g.hashCode(), g))
      .reduceByKey({
        case (g1, g2) => g1
      }).map(_._2)

    geometryRDD
  }

  def saveAsGeoJSON(outputLocation: String): Unit = {
    this.geometryRDD.mapPartitions(iterator => {
      val geoJSONWriter = new GeoJSONWriter
      val featureList = iterator.map(geometry => {
        if (geometry.getUserData != null) {
          val userData = Map("UserData" -> geometry.getUserData)
          new Feature(geoJSONWriter.write(geometry), userData.asJava)
        } else {
          new Feature(geoJSONWriter.write(geometry), null)
        }
      }).toList

      val featureCollection = new FeatureCollection(featureList.toArray[Feature])
      List[String](featureCollection.toString).toIterator
    }).saveAsTextFile(outputLocation)
  }

  def saveAsShapefile(filepath: String, crs: String): Unit = {
    val t = System.currentTimeMillis()
    val polygons = this.geometryRDD.collect().toList.asJava
    println("************** collect overlap polygon size: " +polygons.size)
    println("************** collect overlap polygon time: " + (System.currentTimeMillis() - t)/1000000)

    GeometryReaderUtil.saveAsShapefile(filepath, polygons, crs)
  }

  def save2GeoJson2Shapfile(shpFolder: String, crs: String): Unit = {
    val t = System.currentTimeMillis()
    val polygons = this.geometryRDD.collect().toList.asJava
    println("************** collect overlap polygon size: " +polygons.size)
    println("************** collect overlap polygon time: " + (System.currentTimeMillis() - t)/1000000)

    val jsonpath: scala.reflect.io.Path = scala.reflect.io.Path (shpFolder + "/" + "json/")
    val jsonFolder = jsonpath.createDirectory(failIfExists=false)
    val jsonFilePath = jsonFolder.path + "/" + jsonFolder.name + ".geojson"
    GeometryReaderUtil.saveAsGeoJSON(jsonFilePath, polygons, crs)
    GeometryReaderUtil.geojson2shp(jsonFolder.path, shpFolder, crs)

    //delete tmp files
    jsonFolder.deleteRecursively()
    val conf = new Configuration()
    val localFs = FileSystem.getLocal(conf)
    val jsonPath = new Path(shpFolder + "/" + "json/")
    if (localFs.exists(jsonPath)) {
      localFs.delete(jsonPath, true)
    }
  }

  def save2HfdsGeoJson2Shapfile(shpFolder: String, crs: String): Unit = {

    val hdfsLocation = "/user/root/tmpGeoJson/"
    val localGeoJsonFolder = "/tmp/GeoJson/"
    this.saveAsGeoJSON(hdfsLocation)
    OperationUtil.hdfsToLocal(hdfsLocation, localGeoJsonFolder)
    GeometryReaderUtil.geojson2shp(localGeoJsonFolder, shpFolder, crs)

    //delete tmp files
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val localFs = FileSystem.getLocal(conf)
    val hdfsPath = new Path(hdfsLocation)
    val localJsonPath = new Path(localGeoJsonFolder)
    if (fs.exists(hdfsPath)) {
      fs.delete(hdfsPath, true)
    }
    if (localFs.exists(localJsonPath)) {
      localFs.delete(localJsonPath, true)
    }
  }

  def filterByGeometryType(geometryType: Class[_]): GeometryRDD = {
    val rdd = this.geometryRDD.filter(geometry => {
      geometry.getClass == geometryType
    })

    val geometryRDD = new GeometryRDD
    geometryRDD.setGeometryRDD(rdd)
    geometryRDD
  }

  def filterByGeometryType(geometryType: Class[_], inputGeometryRDD: GeometryRDD): GeometryRDD = {
    val rdd = inputGeometryRDD.getGeometryRDD.filter(geometry => {
      geometry.getClass == geometryType
    })

    val geometryRDD = new GeometryRDD
    geometryRDD.setGeometryRDD(rdd)
    geometryRDD
  }

  def getPartitioner: SpatialPartitioner = this.partitioner
}
