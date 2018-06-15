package edu.gmu.stc.vector.operation

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.util.Random

/**
  * Created by Fei Hu on 1/23/18.
  */
object OperationUtil {

  val random = Random

  val WEBHDFS_GET = "http://%s:50075/webhdfs/v1%s?op=OPEN&namenoderpcaddress=%s:8020&offset=0"

  val hostIPMap: HashMap[String, String] = HashMap("svr-A7-C-U12" -> "10.192.21.139",
    "svr-A7-C-U13" -> "10.192.21.140",
    "svr-A7-C-U14" -> "10.192.21.141",
    "svr-A7-C-U15" -> "10.192.21.142",
    "svr-A7-C-U16" -> "10.192.21.143",
    "svr-A7-C-U17" -> "10.192.21.144",
    "svr-A7-C-U18" -> "10.192.21.145",
    "svr-A7-C-U19" -> "10.192.21.146",
    "svr-A7-C-U20" -> "10.192.21.147",
    "svr-A7-C-U21" -> "10.192.21.148",
    "svr-A7-C-U6" -> "10.192.21.133",
    "localhost" -> "127.0.0.1")

  val HDFS_MASTER_NODE = "10.192.21.148"

  /**
    * monitor the runtime for the task/function
    *
    * @param proc
    * @tparam T
    * @return
    */
  def show_timing[T](proc: => T): Long = {
    val start=System.nanoTime()
    val res = proc // call the code
    val end = System.nanoTime()
    val runtime = (end-start)/1000000000  //seconds
    runtime
  }

  def getUniqID(id1: Long, id2: Long): Long = {
    (id1 << 32) + id2
  }

  def hdfsToLocal(hdfsPath: String, localPath: String) = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val localFs = FileSystem.getLocal(conf)

    val src = new Path(hdfsPath)
    val dest = new Path(localPath)

    if (localFs.exists(dest)) {
      localFs.delete(dest, true)
    }

    fs.copyToLocalFile(src, dest)
  }

  def getFileNum(dirPath: String, pathFilter: String = "part-"): Int = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val fileStatuses = listFileStatus(dirPath, pathFilter, fs)

    fileStatuses.length
  }

  def listFileStatus(dirPath: String, pathFilter: String = "part-", fs: FileSystem): Array[FileStatus] = {
    val fileStatuses = fs.listStatus(new Path(dirPath), new PathFilter {
      override def accept(path: Path): Boolean = path.toString.contains(pathFilter)
    })

    fileStatuses
  }

  def getWebHDFSUrl(path: String, pathFilter: String = "part-"): String = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val fileStatusArray = listFileStatus(path, pathFilter, fs)

    val hdfsUrls = fileStatusArray.indices.map(i => {
      val path = fileStatusArray(i).getPath
      val locations = fs.getFileBlockLocations(path, 0, fileStatusArray(i).getLen)
      val hosts = locations(random.nextInt(locations.size)).getHosts
      val randomHost = hosts(random.nextInt(hosts.size))
      val hostIP = hostIPMap.getOrElse(randomHost, "127.0.0.1")

      WEBHDFS_GET.format(hostIP, path.toUri.getRawPath, HDFS_MASTER_NODE)
    })

    hdfsUrls.mkString(",")
  }

  def publishWMS(shpFolder: String, cfgPath: String): String = {
    val url = MapUtil.publishWMS(shpFolder, cfgPath)
    url
  }

  def updateHadoopConfig(hConf: Configuration, newConfPath: String): Unit = {
    val fs = FileSystem.get(hConf)
    val inputStream = fs.open(new Path(newConfPath))
    hConf.addResource(inputStream)
  }
  
  def main(args: Array[String]): Unit = {
    //println(OperationUtil.publishWMS("/home/yun/test","/home/yun/GeoSpark/config/conf_dc.xml"))
    println(OperationUtil.publishWMS("D:\\python workspace\\data\\Soil_Type_by_Slope","D:\\geosparkWorkspace\\GeoSpark\\config\\conf.xml"))

  }
}
