package edu.gmu.stc.vector.sparkshell

import edu.gmu.stc.config.ConfigParameter
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
object STC_OverlapTest_v5 extends Logging{
  def main(args: Array[String]): Unit = {

    if (args.length != 7) {
      logError("You input "+ args.length + " arguments: " + args.mkString(" ") + ", but it requires 7 arguments: " +
        "\n \t 1) configFilePath: File path for the configuration file path" +
        "\n \t 2) metaPartitionNum: Number of MetaRDD partitions" +
        "\n \t 3) gridType: Type of the partition, e.g. EQUALGRID, HILBERT, RTREE, VORONOI, QUADTREE, KDBTREE" +
        "\n \t 4) indexType: Index type for each partition, e.g. QUADTREE, RTREE" +
        "\n \t 5) output file path: File path for geojson output" +
        "\n \t 6) crs: coordinate reference system" +
        "\n \t 7) geometryPartitionNum: Number of GeomtryRDD paritions")

      return
    }

    val t = System.currentTimeMillis()

    val sparkConf = new SparkConf()
      .setAppName("%s_%s_%s_%s".format("STC_OverlapTest_v4", args(1), args(2), args(3)))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[VectorKryoRegistrator].getName)

    if (System.getProperty("os.name").equals("Mac OS X")) {
      sparkConf.setMaster("local[6]")
    }

    val sc = new SparkContext(sparkConf)

    val configFilePath = args(0)
    val hConf = new Configuration()
    updateHadoopConfig(hConf, configFilePath)
    sc.hadoopConfiguration.addResource(hConf)

    val parquetIndexDirs = hConf.get(ConfigParameter.PARQUET_INDEX_DIRS).split(",").map(s => s.trim)

    val metaPartitionNum = args(1).toInt
    val geometryPartitionNum = args(6).toInt

    val minX = -180
    val minY = -180
    val maxX = 180
    val maxY = 180

    val gridType = GridType.getGridType(args(2)) //EQUALGRID, HILBERT, RTREE, VORONOI, QUADTREE, KDBTREE
    val indexType = IndexType.getIndexType(args(3))  //RTREE, QUADTREE
    val table1 = parquetIndexDirs(0)
    val shapeFileMetaRDD1 = new ShapeFileMetaRDD(sc, hConf)
    val geometryRDD1 = shapeFileMetaRDD1.getGeometryRDDFromParquet(sc, table1, minX, maxX, minY, maxY)
    geometryRDD1.initializePartitioner(sc, gridType, metaPartitionNum)
    geometryRDD1.partition(geometryRDD1.getPartitioner)
    geometryRDD1.indexPartition(indexType)


    val shapeFileMetaRDD2 = new ShapeFileMetaRDD(sc, hConf)
    val table2 = parquetIndexDirs(1)
    val geometryRDD2 = shapeFileMetaRDD2.getGeometryRDDFromParquet(sc, table2, minX, maxX, minY, maxY)
    geometryRDD2.partition(geometryRDD1.getPartitioner)
    geometryRDD2.cache()


    logInfo(geometryRDD1.getGeometryRDD.getNumPartitions
      + "**********************"
      + geometryRDD2.getGeometryRDD.getNumPartitions)

    val startTime = System.currentTimeMillis()
    val geometryRDD = geometryRDD1.intersectV2(geometryRDD2, geometryPartitionNum)
    //val geometryRDD = geometryRDD1.intersect(geometryRDD2)
    val endTime = System.currentTimeMillis()
    println("******** Intersection time: " + (endTime - startTime)/1000000)

    println("******** Number of intersected polygons: %d".format(geometryRDD.getGeometryRDD.count()))

    println("************** Total time: " + (System.currentTimeMillis() - t)/1000000)
  }

}
