import java.io.File

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.types._

/** Helpers shared by the probes: sessions, read schemas, scan row counts, footers. */
object Common {

  def session(blockSize: Option[String] = None, pushdown: Boolean = false): SparkSession = {
    val b = SparkSession.builder().master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.maxMetadataStringLength", "100000")
      .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
      .config("spark.sql.shuffle.partitions", "8")
    blockSize.foreach { s =>
      b.config("spark.hadoop.parquet.block.size", s)
        .config("spark.hadoop.parquet.block.size.row.check.min", "100")
        .config("spark.hadoop.parquet.block.size.row.check.max", "1000")
    }
    if (pushdown) {
      b.config("spark.sql.parquet.recordLevelFilter.enabled", "false")
        .config("spark.sql.parquet.filterPushdown", "true")
    }
    val s = b.getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    s
  }

  def leaves(t: DataType, path: String): Seq[String] = t match {
    case s: StructType => s.fields.toSeq.flatMap(f =>
      leaves(f.dataType, if (path.isEmpty) f.name else path + "." + f.name))
    case ArrayType(e, _) => leaves(e, path + "[]")
    case _ => Seq(path)
  }

  /** The scan's `requiredSchema`, merged where the plan has several scans. */
  def readSchema(df: DataFrame): StructType =
    df.queryExecution.executedPlan.collect { case s: FileSourceScanExec => s.requiredSchema }
      .reduceOption((a, b) => StructType(a.fields ++ b.fields)).getOrElse(StructType(Nil))

  /** The leaves of the read schema, shortened by dropping a common prefix. */
  def readLeaves(df: DataFrame, prefix: String = ""): String =
    leaves(readSchema(df), "").map(_.stripPrefix(prefix)).mkString(", ")

  def reads(df: DataFrame, name: String): Boolean = leaves(readSchema(df), "").exists(_.contains(name))

  /** Executes the dataset and returns the rows the scan emitted. */
  def scanned(df: DataFrame): Long = {
    df.collect()
    val plan = df.queryExecution.executedPlan
    plan.collectLeaves().collect { case l: SparkPlan if l.metrics.contains("numOutputRows") =>
      l.metrics("numOutputRows").value }.sum
  }

  def pushed(df: DataFrame): String = {
    val m = "PushedFilters: (\\[.*?\\]), ReadSchema".r.findFirstMatchIn(
      df.queryExecution.executedPlan.toString)
    m.map(_.group(1)).getOrElse("n/a")
  }

  def optimisedFilter(df: DataFrame): String = {
    val m = "Filter (.*?) \\+?-?\\s*Relation".r.findFirstMatchIn(
      df.queryExecution.optimizedPlan.toString.replace('\n', ' '))
    m.map(_.group(1)).getOrElse("(no Filter)")
  }

  /** Row groups of the Parquet files of a directory and the statistics of one column. */
  def footer(spark: SparkSession, dir: String, column: String): String = {
    val conf = spark.sparkContext.hadoopConfiguration
    var groups = 0; var rows = 0L; var stats = true
    new File(dir).listFiles().filter(_.getName.endsWith(".parquet")).foreach { f =>
      val r = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(f.getPath), conf))
      try r.getFooter.getBlocks.forEach { b =>
        groups += 1; rows += b.getRowCount
        b.getColumns.forEach { c =>
          if (c.getPath.toDotString == column) {
            val st = c.getStatistics
            if (st == null || !st.hasNonNullValue) stats = false
          }
        }
      } finally r.close()
    }
    s"$groups row groups, $rows rows, statistics on $column: $stats"
  }
}
