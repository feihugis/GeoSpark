package edu.gmu.stc.vector.sparkshell

import edu.gmu.stc.config.ConfigParameter
import edu.gmu.stc.hibernate.HibernateUtil
import edu.gmu.stc.vector.examples.ShapeFileMetaTest.logInfo
import edu.gmu.stc.vector.rdd.{GeometryRDD, ShapeFileMetaRDD}
import edu.gmu.stc.vector.serde.VectorKryoRegistrator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.IndexType
import org.apache.hadoop.conf.Configuration


/**
  * Created by Fei Hu on 1/29/18.
  */
object STC_BuildIndexTest extends Logging{

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      logError("Please input two arguments: " +
        "\n \t 1) appName: the name of the application " +
        "\n \t 2) configFilePath: this file path for the configuration file path")
      return
    }

    val sparkConf = new SparkConf().setAppName(args(0))//.setMaster("local[6]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[VectorKryoRegistrator].getName)

    if (System.getProperty("os.name").equals("Mac OS X")) {
      sparkConf.setMaster("local[6]")
    }

    val sc = new SparkContext(sparkConf)

    val configFilePath = args(1)   //"/Users/feihu/Documents/GitHub/GeoSpark/config/conf.xml"
    val hConf = new Configuration()
    hConf.addResource(new Path(configFilePath))
    sc.hadoopConfiguration.addResource(hConf)

    val shapeFileMetaRDD = new ShapeFileMetaRDD(sc, hConf)
    shapeFileMetaRDD.initializeShapeFileMetaRDD(sc, hConf)
    shapeFileMetaRDD.cache
    //shapeFileMetaRDD.saveShapeFileMetaToDB()
    //shapeFileMetaRDD.saveShapeFileMetaToParquet(sc)
    shapeFileMetaRDD.saveShapeFileMetaToParquetWithRealData(sc)
  }
}
