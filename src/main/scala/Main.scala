import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.log4j.{Level, Logger}

import java.io.IOException

object Main extends  App {
  Logger.getLogger("org").setLevel(Level.OFF)
//  System.setProperty("HADOOP_USER_NAME", "root")

  val conf = new Configuration()
  conf.addResource(new Path("core-site.xml"))
  conf.addResource(new Path("hdfs-site.xml"))

  val hdfs = FileSystem.get(conf)
  //  val hdfs = FileSystem.get(new URI("hdfs://127.0.0.1:9000"), conf)

  val source_dir = "/stage"
  val dest_dir = "/ods"

  val source_path = new Path(source_dir)
  val dest_path = new Path(dest_dir)

  if (!hdfs.exists(dest_path))
    hdfs.mkdirs(dest_path)

  val allStatus = FileUtil.stat2Paths(hdfs.listStatus(source_path))
  val dirs: Array[String] = allStatus
    .filter(hdfs.getFileStatus(_).isDirectory)
    .map(_.getName)

  for (dir <- dirs) {
    var file_name: String = ""
    var inps: FSDataInputStream = null
    var outs: FSDataOutputStream = null

    val cur_path = new Path(source_dir + "/" + dir)
    val out_path = new Path(dest_dir + "/" + dir)
    if (!hdfs.exists(out_path))
        hdfs.mkdirs(out_path)

    val files = hdfs.listFiles(new Path(source_dir + "/" + dir), false)
    while (files.hasNext) {
      val file = files.next

      file_name = file.getPath.getName
      if (file_name.startsWith("part")) {
        file_name = file_name.substring(0, file_name.lastIndexOf(".csv") + 4)

        val cur_file = new Path(source_dir + "/" + dir + "/" + file.getPath.getName)
        val out_file = new Path(dest_dir + "/" + dir + "/" + file_name)

        inps = hdfs.open(cur_file)
        if (outs == null) {
          outs = hdfs.create(out_file)
        }
        var useSepar: Boolean = false
        var bytesRead: Int = 0
        val buffer: Array[Byte] = new Array[Byte](1024)
        try {
          bytesRead = inps.read(buffer)
          useSepar = bytesRead > 0
//          println("read bytes: " + bytesRead)
          while (bytesRead > 0) {
            outs.write(buffer, 0, bytesRead)
            bytesRead = inps.read(buffer)
          }
          if (useSepar)
            outs.write("\r\n".getBytes)

        } catch {
          case e: IOException =>
            println(e.getMessage)
        } finally {
          inps.close()
        }
      }
    }
    if (outs != null) {
//      println("write new file to : " + dest_dir + "/" + dir)
      outs.flush()
      outs.close()
      hdfs.delete(cur_path, true)
    }
  }
}


















