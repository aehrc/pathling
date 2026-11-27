package spike

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.types._

import scala.jdk.CollectionConverters._

/**
 * Reads a repeating annotated primitive's annotations, with and without its value, in three
 * placements written from the same logical data:
 *  - X: inside the metadata group, `_event[] {id, extension, __start, __end}`;
 *  - S: a sibling annotation group, `_event[] {id, extension}` and `__event[] {start, end}`;
 *  - P: parent-level fields, `_event[] {id, extension}`, `__event_start[]` and `__event_end[]`.
 * Contexts: direct (`timing.event`, a singular parent) and inside a lambda over the repeating
 * `dosage`. The pairing is shape (c), `arrays_zip(value, start array, end array)`, with a
 * `coalesce(x, array())` guard on each input. Prints the read leaves, their bytes, whether any
 * extension leaf is read, and whether the result equals layout X's. Usage: TwoLevelProbe <dir>.
 */
object TwoLevelProbe {
  val EXT = "array<struct<url:string,valueString:string>>"
  val META_X = s"struct<id:string,extension:$EXT,__start:timestamp,__end:timestamp>"
  val META = s"struct<id:string,extension:$EXT>"
  def timingType(l: String): String = l match {
    case "X" => s"struct<event:array<string>,_event:array<$META_X>>"
    case "S" => s"struct<event:array<string>,_event:array<$META>,__event:array<struct<start:timestamp,end:timestamp>>>"
    case "P" => s"struct<event:array<string>,_event:array<$META>,__event_start:array<timestamp>,__event_end:array<timestamp>>"
  }
  def schema(l: String): String =
    s"id string, timing ${timingType(l)}, dosage array<struct<text:string,timing:${timingType(l)}>>"

  def q(s: String) = "\"" + s + "\""
  def arr(xs: Seq[String]) = xs.mkString("[", ",", "]")
  def ext(tag: String, n: Int): String =
    arr((1 to n).map(i => s"""{"url":"http://ex/$i","valueString":"$tag-payload-$i-${"x" * 40}"}"""))

  /** One logical item of metadata: null, or (id, extension json, start, end). */
  case class M(id: String, ext: String, start: String, end: String)

  def item(tag: String, d: Int, nExt: Int) =
    M(s"m$d", ext(tag, nExt), s"2020-01-0${1 + d % 9}T00:00:00.000Z", s"2021-01-0${1 + d % 9}T00:00:00.000Z")

  /** Logical timing for row i, dosage j: event values (None = null list) and metadata list. */
  def logical(i: Int, j: Int): (Option[Seq[String]], Option[Seq[Option[M]]]) = {
    val n = 2 + (i + j) % 3
    val ev = (0 until n).map(k => q(s"2020-0${1 + k}-1$j"))
    val ms = (0 until n).map(k => Some(item(s"e$i$j$k", k, 3 + (i + k) % 3)): Option[M])
    i % 40 match {
      case 3 => (Some(ev), None)
      case 6 => (None, Some(ms))
      case 9 => (Some(ev), Some(ms.take(1)))
      case 12 => (Some(ev), Some(ms.updated(1, None)))
      case 15 => (Some(Nil), Some(Nil))
      case 18 => (Some(ev), Some(Nil))
      case 21 => (Some(ev.updated(0, "null")), Some(ms))
      case 24 => (Some(ev :+ q("2030-01-01")), Some(ms))
      case 27 => (None, None)
      case _ => (Some(ev), Some(ms))
    }
  }

  def timingJson(l: String, i: Int, j: Int): String = {
    val (ev, ms) = logical(i, j)
    val e = ev.map(arr).getOrElse("null")
    def metaList(f: M => String) = ms.map(xs => arr(xs.map(_.map(f).getOrElse("null")))).getOrElse("null")
    l match {
      case "X" =>
        val m = metaList(x => s"""{"id":"${x.id}","extension":${x.ext},"__start":"${x.start}","__end":"${x.end}"}""")
        s"""{"event":$e,"_event":$m}"""
      case "S" =>
        val m = metaList(x => s"""{"id":"${x.id}","extension":${x.ext}}""")
        val a = metaList(x => s"""{"start":"${x.start}","end":"${x.end}"}""")
        s"""{"event":$e,"_event":$m,"__event":$a}"""
      case "P" =>
        val m = metaList(x => s"""{"id":"${x.id}","extension":${x.ext}}""")
        val s0 = metaList(x => q(x.start))
        val e0 = metaList(x => q(x.end))
        s"""{"event":$e,"_event":$m,"__event_start":$s0,"__event_end":$e0}"""
    }
  }

  def rows(l: String, n: Int): Seq[String] = (0 until n).map { i =>
    val dosage = i % 40 match {
      case 30 => "null"
      case 31 => "[null]"
      case 32 => """[{"text":"t","timing":null}]"""
      case _ => arr((0 until 1 + i % 2).map(j => s"""{"text":"t","timing":${timingJson(l, i, j)}}"""))
    }
    val tm = if (i % 40 == 33) "null" else timingJson(l, i, 0)
    s"""{"id":"p$i","timing":$tm,"dosage":$dosage}"""
  }

  def leaves(t: DataType, prefix: String): Seq[String] = t match {
    case st: StructType => st.fields.toSeq.flatMap(f =>
      leaves(f.dataType, if (prefix.isEmpty) f.name else s"$prefix.${f.name}"))
    case ArrayType(et, _) => leaves(et, prefix + "[]")
    case _ => Seq(prefix)
  }

  def readLeaves(df: DataFrame): Seq[String] =
    df.queryExecution.executedPlan.collect { case s: FileSourceScanExec => s.requiredSchema }
      .flatMap(s => leaves(s, "")).distinct.sorted

  def leafBytes(dir: String): Map[String, Long] = {
    val conf = new Configuration()
    new java.io.File(dir).listFiles().filter(_.getName.endsWith(".parquet")).flatMap { f =>
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
    import spark.implicits._
    println(s"Spark ${spark.version}")
    val layouts = Seq("X", "S", "P")
    val bytes = layouts.map { l =>
      val dir = s"${args(0)}/$l.parquet"
      spark.read.schema(schema(l)).json(rows(l, 4000).toDS()).coalesce(1).write.mode("overwrite").parquet(dir)
      spark.read.parquet(dir).createOrReplaceTempView(s"t$l")
      val b = leafBytes(dir)
      println(s"File $l: total ${b.values.sum} bytes; extension leaves ${b.filter(_._1.contains("extension")).values.sum} bytes")
      l -> b
    }.toMap

    def startEnd(l: String, t: String): (String, String) = l match {
      case "X" => (s"$t._event.__start", s"$t._event.__end")
      case "S" => (s"$t.__event.start", s"$t.__event.end")
      case "P" => (s"$t.__event_start", s"$t.__event_end")
    }
    def g(e: String) = s"coalesce($e, array())"
    /** Shape (c), guarded, over timing expression t, giving array<struct<value,start,end>>. */
    def paired(l: String, t: String, inLambda: Boolean): String = {
      val (s, e) = startEnd(l, t)
      val z = s"arrays_zip(${g(s"$t.event")}, ${g(s)}, ${g(e)})"
      val probe = if (inLambda) s"select transform(dosage, d -> $z) as z from t$l limit 0"
      else s"select $z as z from t$l limit 0"
      var dt: DataType = spark.sql(probe).schema("z").dataType
      while (!dt.isInstanceOf[StructType]) dt = dt.asInstanceOf[ArrayType].elementType
      val n = dt.asInstanceOf[StructType].fieldNames
      s"transform($z, zz -> named_struct('value', zz.`${n(0)}`, 'start', zz.`${n(1)}`, 'end', zz.`${n(2)}`))"
    }
    def startOnly(l: String, t: String): String = g(startEnd(l, t)._1)

    val queries: Seq[(String, String => String)] = Seq(
      "direct: value + start + end (shape c)" -> (l => paired(l, "timing", inLambda = false)),
      "direct: start only" -> (l => startOnly(l, "timing")),
      "in dosage lambda: value + start + end (shape c)" ->
        (l => s"flatten(transform(dosage, d -> ${paired(l, "d.timing", inLambda = true)}))"),
      "in dosage lambda: start only" -> (l => s"flatten(transform(dosage, d -> ${startOnly(l, "d.timing")}))"),
      "in dosage lambda: value only (control)" -> (_ => "flatten(transform(dosage, d -> d.timing.event))"),
    )

    for ((title, qf) <- queries) {
      println(s"\n### $title\n")
      println("| Layout | Reads | Bytes | Extension read | Equal to X |")
      println("| --- | --- | --- | --- | --- |")
      val dfs = layouts.map(l => l -> spark.sql(s"select id, ${qf(l)} as v from t$l")).toMap
      def key(df: DataFrame) = df.collect().map(_.toString).sorted.toSeq
      val ref = key(dfs("X"))
      for (l <- layouts) {
        val ls = readLeaves(dfs(l))
        val b = ls.map(x => bytes(l).getOrElse(x, 0L)).sum
        val ext = ls.exists(_.contains("extension"))
        val eq = if (key(dfs(l)) == ref) "yes" else "**NO**"
        println(s"| $l | ${ls.mkString(", ")} | $b | ${if (ext) "**yes**" else "no"} | $eq |")
      }
    }
    spark.stop()
  }
}
