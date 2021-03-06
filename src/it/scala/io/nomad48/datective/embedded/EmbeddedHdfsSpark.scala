package io.nomad48.datective.embedded

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.{DistributedFileSystem, MiniDFSCluster}
import org.apache.spark.sql.SparkSession

import java.io.File

/**
  * Create a temporary local HDFS cluster
  */
trait EmbeddedHdfsSpark {
  @transient protected var hdfsCluster: MiniDFSCluster = _
  def startHdfs(tempFolder: File = null, port: Int = 9000): Unit = {
    if (tempFolder != null) {
      System.setProperty(
        MiniDFSCluster.PROP_TEST_BUILD_DATA,
        tempFolder.getCanonicalPath
      )
    }
    val baseDir = new File(
      org.apache.hadoop.test.PathUtils.getTestDir(getClass, true),
      "miniHDFS"
    )
    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    conf.setBoolean("dfs.webhdfs.enabled", false)
    val builder = new MiniDFSCluster.Builder(conf)
    hdfsCluster = builder
      .nameNodePort(port)
      .manageNameDfsDirs(true)
      .manageDataDfsDirs(true)
      .format(true)
      .build()
    hdfsCluster.waitClusterUp()
  }
  def stopHdfs(): Unit = {
    hdfsCluster.shutdown(true)
  }
  def getSpark(): SparkSession = {
    // only instantiate Spark once the HDFS cluster is up
    SparkSession
      .builder()
      .master("local[*]")
      .appName("PrepareForUploadTest")
      .config("spark.hadoop.fs.default.name", hdfsURI)
      .config(
        "spark.sql.warehouse.dir",
        "target/test/spark-warehouse"
      )
      .getOrCreate
  }
  def hdfsURI: String =
    "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/"

  def fileSystem: DistributedFileSystem = hdfsCluster.getFileSystem

  def copyFromLocal(file: String, destination: String): Unit =
    fileSystem.copyFromLocalFile(new Path(file), new Path(destination))

  def copyToLocal(file: String, destination: String): Unit =
    fileSystem.copyToLocalFile(new Path(file), new Path(destination))
}
