package edu.gmu.stc.vector.sparkshell

import edu.gmu.stc.config.ConfigParameter
import edu.gmu.stc.vector.operation.OperationUtil
import edu.gmu.stc.vector.operation.OperationUtil.updateHadoopConfig
import edu.gmu.stc.vector.rdd.{GeometryRDD, ShapeFileMetaRDD}
import edu.gmu.stc.vector.serde.VectorKryoRegistrator
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{GridType, IndexType}

/**
  * Created by Fei Hu.
  */
object STC_OverlapTest_v4 extends Logging{
  def main(args: Array[String]): Unit = {

    if (args.length != 6) {
      logError("You input "+ args.length + "arguments: " + args.mkString(" ") + ", but it requires 5 arguments: " +
        "\n \t 1)configFilePath: this file path for the configuration file path" +
        "\n \t 2) numPartition: the number of partitions" +
        "\n \t 3) gridType: the type of the partition, e.g. EQUALGRID, HILBERT, RTREE, VORONOI, QUADTREE, KDBTREE" +
        "\n \t 4) indexType: the index type for each partition, e.g. QUADTREE, RTREE" +
        "\n \t 5) output file path: the file path for geojson output" +
        "\n \t 6) crs: coordinate reference system")

      return
    }

    val t = System.currentTimeMillis()

    val sparkConf = new SparkConf()
      .setAppName("%s_%s_%s_%s".format("STC_OverlapTest_v2", args(1), args(2), args(3)))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[VectorKryoRegistrator].getName)

    if (System.getProperty("os.name").equals("Mac OS X")) {
      sparkConf.setMaster("local[6]")
    }

    val sc = new SparkContext(sparkConf)

    val configFilePath = args(0)   //"/Users/feihu/Documents/GitHub/GeoSpark/config/conf.xml"
    val hConf = new Configuration()
    updateHadoopConfig(hConf, configFilePath)
    sc.hadoopConfiguration.addResource(hConf)

    val parquetIndexDirs = hConf.get(ConfigParameter.PARQUET_INDEX_DIRS).split(",").map(s => s.trim)

    val partitionNum = args(1).toInt  //24
    val minX = -180
    val minY = -180
    val maxX = 180
    val maxY = 180

    val gridType = GridType.getGridType(args(2)) //EQUALGRID, HILBERT, RTREE, VORONOI, QUADTREE, KDBTREE
    val indexType = IndexType.getIndexType(args(3))  //RTREE, QUADTREE

    val shapeFileMetaRDD1 = new ShapeFileMetaRDD(sc, hConf)
    val table1 = parquetIndexDirs(0)
    shapeFileMetaRDD1.initializeShapeFileMetaRDDFromParquetAndPartitioner(
      sc, table1, gridType, partitionNum, minX, minY, maxX, maxY)

    val geometryRDD1 = new GeometryRDD
    geometryRDD1.initialize(shapeFileMetaRDD1, hasAttribute = true)
    geometryRDD1.partition(shapeFileMetaRDD1.getPartitioner)
    geometryRDD1.indexPartition(indexType)
    geometryRDD1.cache()

    val shapeFileMetaRDD2 = new ShapeFileMetaRDD(sc, hConf)
    val table2 = parquetIndexDirs(1)
    shapeFileMetaRDD2.initializeShapeFileMetaRDDFromParquetWithoutPartition(
      sc, table2, partitionNum, minX, minY, maxX, maxY)

    val geometryRDD2 = new GeometryRDD
    geometryRDD2.initialize(shapeFileMetaRDD2, hasAttribute = true)
    geometryRDD2.partition(shapeFileMetaRDD1.getPartitioner)
    geometryRDD2.cache()


    logInfo(geometryRDD1.getGeometryRDD.getNumPartitions
      + "**********************"
      + geometryRDD2.getGeometryRDD.getNumPartitions)

    val startTime = System.currentTimeMillis()
    val geometryRDD = geometryRDD1.intersectV2(geometryRDD2, partitionNum)
    geometryRDD.cache()
    val endTime = System.currentTimeMillis()
    println("******** Intersection time: " + (endTime - startTime)/1000000)

    val filePath = args(4)
    val crs = args(5)
    if (filePath.endsWith("shp")) {
      geometryRDD.saveAsShapefile(filePath, crs)
    } else {
      geometryRDD.saveAsGeoJSON(filePath)
    }

    println("******** Number of intersected polygons: %d".format(geometryRDD.getGeometryRDD.count()))

    println("************** Total time: " + (System.currentTimeMillis() - t)/1000000)
  }

}
