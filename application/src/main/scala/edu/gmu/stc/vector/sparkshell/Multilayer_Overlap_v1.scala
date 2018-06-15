package edu.gmu.stc.vector.sparkshell

import edu.gmu.stc.config.ConfigParameter
import edu.gmu.stc.vector.operation.OperationUtil
import edu.gmu.stc.vector.rdd.{GeometryRDD, ShapeFileMetaRDD}
import edu.gmu.stc.vector.serde.VectorKryoRegistrator
import edu.gmu.stc.vector.sparkshell.STC_OverlapTest_v3.{logError, logInfo}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.datasyslab.geospark.enums.{GridType, IndexType}
import edu.gmu.stc.vector.operation.OperationUtil._
import org.apache.commons.io.FilenameUtils

/**
  * Created by Fei Hu.
  */
object Multilayer_Overlap_v1 extends Logging{
  def main(args: Array[String]): Unit = {

    if (args.length != 8) {
      logError("You input "+ args.length + "arguments: " + args.mkString(" ") + ", but it requires 5 arguments: " +
        "\n \t 1)configFilePath: this file path for the configuration file path" +
        "\n \t 2) numPartition: the number of partitions" +
        "\n \t 3) gridType: the type of the partition, e.g. EQUALGRID, HILBERT, RTREE, VORONOI, QUADTREE, KDBTREE" +
        "\n \t 4) indexType: the index type for each partition, e.g. QUADTREE, RTREE" +
        "\n \t 5) output file path: the file path for geojson output" +
        "\n \t 6) crs: coordinate reference system" +
        "\n \t 7) base_layer: the name for the base_layer" +
        "\n \t 8) overlay_layer: the file directory or file name for the input layer")

      return
    }

    val t = System.currentTimeMillis()

    // Initialize the Spark job context
    val sparkConf = new SparkConf()
      .setAppName("%s_%s_%s_%s".format("STC_MultiLayer_Overlay_v1", args(1), args(2), args(3)))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[VectorKryoRegistrator].getName)

    if (System.getProperty("os.name").equals("Mac OS X")) {
      sparkConf.setMaster("local[6]")
    }

    val sc = new SparkContext(sparkConf)

    // Get the input parameters
    val configFilePath = args(0)   //"/Users/feihu/Documents/GitHub/GeoSpark/config/conf.xml"
    val partitionNum = args(1).toInt  //24
    val gridType = GridType.getGridType(args(2)) //EQUALGRID, HILBERT, RTREE, VORONOI, QUADTREE, KDBTREE
    val indexType = IndexType.getIndexType(args(3))  //RTREE, QUADTREE
    val outputFilePath = args(4)
    val crs = args(5)
    val baselayerName = args(6)
    val overlayDir = args(7)


    val hConf = new Configuration()
    hConf.addResource(new Path(configFilePath))
    updateHadoopConfig(hConf, configFilePath)

    sc.hadoopConfiguration.addResource(hConf)

    val overlayerNames = List(FileSystem.get(sc.hadoopConfiguration)
      .listFiles(new Path(overlayDir), true))
      .filter(file => file.toString.endsWith(".shp"))
      .map(file => FilenameUtils.getBaseName(file.toString))


    val tableNames = hConf.get(ConfigParameter.SHAPEFILE_INDEX_TABLES).split(",").map(s => s.toLowerCase().trim)

    val minX = -180
    val minY = -180
    val maxX = 180
    val maxY = 180


    val shapeFileMetaRDD1 = new ShapeFileMetaRDD(sc, hConf)
    //val table1 = tableNames(0)
    shapeFileMetaRDD1.initializeShapeFileMetaRDDAndPartitioner(sc, baselayerName, gridType, partitionNum, minX, minY, maxX, maxY)
    val geometryRDD1 = new GeometryRDD
    geometryRDD1.initialize(shapeFileMetaRDD1, hasAttribute = true)
    geometryRDD1.partition(shapeFileMetaRDD1.getPartitioner)
    geometryRDD1.indexPartition(indexType)
    geometryRDD1.cache()

    println("*************Counting GeometryRDD1 Time: " + OperationUtil.show_timing(geometryRDD1.getGeometryRDD.count()))

    val partitionNum1 = geometryRDD1.getGeometryRDD.mapPartitionsWithIndex({
      case (index, itor) => {
        List((index, itor.size)).toIterator
      }
    }).collect()

    println("********geometryRDD1*************\n")
    partitionNum1.foreach(println)
    println("********geometryRDD1*************\n")
    println("******geometryRDD1****************" + geometryRDD1.getGeometryRDD.count())


    overlayerNames.map(overlayer => {
      val shapeFileMetaRDD2 = new ShapeFileMetaRDD(sc, hConf)
      shapeFileMetaRDD2.initializeShapeFileMetaRDDWithoutPartition(sc, overlayer,
        partitionNum, minX, minY, maxX, maxY)

      val geometryRDD2 = new GeometryRDD
      geometryRDD2.initialize(shapeFileMetaRDD2, hasAttribute = true)
      geometryRDD2.partition(shapeFileMetaRDD1.getPartitioner)
      geometryRDD2.cache()

      val geometryRDD = geometryRDD1.intersectV2(geometryRDD2, partitionNum)
      geometryRDD.cache()

      if (outputFilePath.endsWith("shp")) {
        geometryRDD.saveAsShapefile(outputFilePath + baselayerName + "_" + overlayer, crs)
      } else {
        geometryRDD.saveAsGeoJSON(outputFilePath + baselayerName + "_" + overlayer)
      }
    })

  }

}
