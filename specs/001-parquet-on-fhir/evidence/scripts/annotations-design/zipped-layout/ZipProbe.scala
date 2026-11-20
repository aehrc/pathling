package spike

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.types._

import java.util.concurrent.atomic.AtomicLong
import scala.io.Source
import scala.jdk.CollectionConverters._

/**
 * Reads on the zipped and the parallel layout. Two modes.
 *
 * reads <data root> <cases file>: for each variant (m0, m1) and each case line
 * `id|layout|sql` (layout p or z, the table is `t`), prints the ReadSchema leaves, the footer
 * bytes of those leaves, and the bytes the task input metrics report as read.
 *
 * sql <statements file>: runs `-- label` headed statements (separated by blank lines) and prints
 * the result or the analysis error.
 */
object ZipProbe {
  def leaves(t: DataType, prefix: String): Seq[String] = t match {
    case st: StructType => st.fields.toSeq.flatMap(f =>
      leaves(f.dataType, if (prefix.isEmpty) f.name else s"$prefix.${f.name}"))
    case ArrayType(et, _) => leaves(et, prefix + "[]")
    case _ => Seq(prefix)
  }

  def readLeaves(df: DataFrame): Seq[String] = {
    val schemas = df.queryExecution.executedPlan.collect { case s: FileSourceScanExec => s.requiredSchema }
    schemas.flatMap(s => leaves(s, "")).distinct.sorted
  }

  def leafBytes(dir: String): Map[String, Long] = {
    val conf = new Configuration()
    val files = new java.io.File(dir).listFiles().filter(_.getName.endsWith(".parquet"))
    files.flatMap { f =>
      val r = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(f.getAbsolutePath), conf))
      try r.getFooter.getBlocks.asScala.flatMap(_.getColumns.asScala.map { c =>
        (c.getPath.toDotString.replace(".list.element", "[]"), c.getTotalSize)
      }) finally r.close()
    }.groupBy(_._1).map { case (k, v) => k -> v.map(_._2).sum }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println(s"Spark ${spark.version}")
    args(0) match {
      case "reads" => reads(spark, args(1), args(2))
      case "sql" => sqlMode(spark, args(1))
    }
  }

  def reads(spark: SparkSession, root: String, casesFile: String): Unit = {
    val bytesRead = new AtomicLong(0)
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onTaskEnd(e: SparkListenerTaskEnd): Unit =
        if (e.taskMetrics != null) bytesRead.addAndGet(e.taskMetrics.inputMetrics.bytesRead)
    })
    val cases = Source.fromFile(casesFile).getLines().filter(l => l.nonEmpty && !l.startsWith("#"))
      .map(_.split("\\|", 3)).toSeq
    for (variant <- Seq("m0", "m1"); layout <- Seq("p", "z")) {
      val dir = s"$root/$variant/${if (layout == "p") "parallel" else "zipped"}"
      val bytes = leafBytes(dir)
      spark.read.parquet(dir).createOrReplaceTempView("t")
      for (c <- cases if c(1) == layout) {
        val label = s"$variant ${c(1)} ${c(0)}"
        try {
          val df = spark.sql(c(2))
          val ls = readLeaves(df)
          val before = bytesRead.get
          df.write.format("noop").mode("overwrite").save()
          Thread.sleep(500)
          val actual = bytesRead.get - before
          val footer = ls.map(l => bytes.getOrElse(l, 0L)).sum
          println(s"ROW|$label|footerBytes=$footer|taskBytesRead=$actual|leaves=${ls.mkString(",")}")
        } catch {
          case e: Exception =>
            println(s"ROW|$label|ERROR|${e.getMessage.split("\n").head.take(200)}")
        }
      }
    }
  }

  def sqlMode(spark: SparkSession, file: String): Unit = {
    val blocks = Source.fromFile(file).mkString.split("\n\n").map(_.trim).filter(_.nonEmpty)
    for (b <- blocks) {
      val lines = b.split("\n")
      val label = lines.head.stripPrefix("-- ")
      val sql = lines.tail.mkString("\n")
      println(s"### $label")
      println(sql)
      try {
        val df = spark.sql(sql)
        if (df.schema.nonEmpty) df.show(50, false)
      } catch {
        case e: Exception => println(s"ERROR: ${e.getMessage.split("\n").head.take(400)}")
      }
    }
  }
}
