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
 * Pairing shapes for a repeating annotated primitive: event[i] with _event[i].__start/__end.
 * Writes a Parquet file with extension-heavy _event lists and edge-case nulls, then runs each
 * shape in each context, printing the ReadSchema leaves, the bytes of the read leaves, and result
 * equality with shape (a). Usage: PairingProbe <data dir> [-rowreader].
 */
object PairingProbe {
  val EXT = "array<struct<url:string,valueString:string>>"
  val META = s"struct<id:string,extension:$EXT,__start:timestamp,__end:timestamp>"
  val TIMING = s"struct<event:array<string>,_event:array<$META>>"
  val SCHEMA = s"id string, timing $TIMING, dosage array<struct<text:string,timing:$TIMING>>"

  def ext(tag: String, n: Int): String =
    (1 to n).map(i => s"""{"url":"http://ex/$i","valueString":"$tag-payload-$i-${"x" * 40}"}""")
      .mkString("[", ",", "]")
  def meta(tag: String, d: Int, nExt: Int): String =
    s"""{"id":"m$d","extension":${ext(tag, nExt)},"__start":"2020-01-0${1 + d % 9}T00:00:00.000Z",""" +
      s""""__end":"2021-01-0${1 + d % 9}T00:00:00.000Z"}"""

  /** A timing struct for row i, dosage j, with the edge shapes selected by i. */
  def timing(i: Int, j: Int): String = {
    val n = 2 + (i + j) % 3
    val ev = (0 until n).map(k => s""""2020-0${1 + k}-1$j"""")
    val meta0 = (0 until n).map(k => meta(s"e$i$j$k", k, 3 + (i + k) % 3))
    val ordinaryE = ev.mkString("[", ",", "]")
    val ordinaryM = meta0.mkString("[", ",", "]")
    val (e, m) = i % 40 match {
      case 3 => (ordinaryE, "null") // null _event, non-null event.
      case 6 => ("null", ordinaryM) // null event, non-null _event.
      case 9 => (ordinaryE, meta0.take(1).mkString("[", ",", "]")) // _event shorter.
      case 12 => (ordinaryE, meta0.updated(1, "null").mkString("[", ",", "]")) // null element.
      case 15 => ("[]", "[]")
      case 18 => (ordinaryE, "[]") // empty _event.
      case 21 => (ev.updated(0, "null").mkString("[", ",", "]"), ordinaryM) // null value.
      case 24 => ((ev :+ "\"2030-01-01\"").mkString("[", ",", "]"), ordinaryM) // event longer.
      case 27 => ("null", "null")
      case _ => (ordinaryE, ordinaryM)
    }
    s"""{"event":$e,"_event":$m}"""
  }

  def rows(n: Int): Seq[String] = (0 until n).map { i =>
    val dosage = i % 40 match {
      case 30 => "null"
      case 31 => "[null]"
      case 32 => """[{"text":"t","timing":null}]"""
      case _ => (0 until 1 + i % 2).map(j => s"""{"text":"t","timing":${timing(i, j)}}""")
        .mkString("[", ",", "]")
    }
    val tm = if (i % 40 == 33) "null" else timing(i, 0)
    s"""{"id":"p$i","timing":$tm,"dosage":$dosage}"""
  }

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

  /** Compressed bytes per leaf of the Parquet file, keyed like `leaves`. */
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

  def show(ls: Seq[String]): String =
    ls.map(_.replace("dosage[].timing.", "dosage.t.").replace("timing.", "t.")).mkString(", ")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.parquet.enableVectorizedReader", (!args.contains("-rowreader")).toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    println(s"Spark ${spark.version}")
    val dir = args(0) + "/pairing.parquet"
    spark.read.schema(SCHEMA).json(rows(4000).toDS()).coalesce(1).write.mode("overwrite").parquet(dir)
    spark.read.parquet(dir).createOrReplaceTempView("t")
    val bytes = leafBytes(dir)
    val total = bytes.values.sum
    val extBytes = bytes.filter(_._1.contains("_event[].extension")).values.sum
    println(s"File leaves total $total bytes; _event extension leaves $extBytes bytes.")

    // Names of the arrays_zip struct fields, learned from the analysed expression.
    def zipNames(ctx: Int, z: String): Seq[String] = {
      val q = if (ctx == 1) s"select $z as z from t limit 0"
      else s"select transform(dosage, d -> $z) as z from t limit 0"
      var dt: DataType = spark.sql(q).schema("z").dataType
      while (!dt.isInstanceOf[StructType]) dt = dt.asInstanceOf[ArrayType].elementType
      dt.asInstanceOf[StructType].fieldNames.toSeq
    }

    val g = (e: String) => s"coalesce($e, array())"
    val shapes = Seq("a", "b", "c", "d")
    def shapesFor(ctx: Int) = if (ctx == 2) shapes :+ "e" else shapes

    /** The array<struct<value,start,end>> expression for a shape over E and M. */
    def shape(ctx: Int, sh: String, guard: Boolean, e0: String, m0: String): String = {
      val e = if (guard) g(e0) else e0
      val m = if (guard) g(m0) else m0
      sh match {
        case "d" =>
          s"zip_with($e, $m, (v, mm) -> named_struct('value', v, 'start', mm.__start, 'end', mm.__end))"
        case _ =>
          val z = sh match {
            case "a" => s"arrays_zip($e, $m)"
            case "b" =>
              s"arrays_zip($e, transform($m, e -> named_struct('__start', e.__start, '__end', e.__end)))"
            case "c" =>
              val s = if (guard) g(s"$m0.__start") else s"$m0.__start"
              val en = if (guard) g(s"$m0.__end") else s"$m0.__end"
              s"arrays_zip($e, $s, $en)"
          }
          val n = zipNames(ctx, z)
          val body = if (sh == "c")
            s"named_struct('value', zz.`${n(0)}`, 'start', zz.`${n(1)}`, 'end', zz.`${n(2)}`)"
          else
            s"named_struct('value', zz.`${n(0)}`, 'start', zz.`${n(1)}`.__start, 'end', zz.`${n(1)}`.__end)"
          s"transform($z, zz -> $body)"
      }
    }

    /** Shape (e), context 2 only: zips at the dosage level, outside any lambda over dosage. */
    def shapeE(): String = {
      val outer = "arrays_zip(dosage.timing.event, transform(dosage.timing._event, m -> transform(m, x -> x.__start)), transform(dosage.timing._event, m -> transform(m, x -> x.__end)))"
      val on = spark.sql(s"select $outer as z from t limit 0").schema("z").dataType
        .asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fieldNames.toSeq
      def f(i: Int) = s"coalesce(o.`${on(i)}`, array())"
      val inner = s"arrays_zip(${f(0)}, ${f(1)}, ${f(2)})"
      val inn = spark.sql(s"select transform($outer, o -> $inner) as z from t limit 0").schema("z").dataType
        .asInstanceOf[ArrayType].elementType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
        .fieldNames.toSeq
      s"flatten(transform($outer, o -> transform($inner, zz -> named_struct('value', zz.`${inn(0)}`, " +
        s"'start', zz.`${inn(1)}`, 'end', zz.`${inn(2)}`))))"
    }

    def expr(ctx: Int, sh: String, guard: Boolean): String =
      if (sh == "e") shapeE()
      else if (ctx == 1) shape(1, sh, guard, "timing.event", "timing._event")
      else s"flatten(transform(dosage, d -> ${shape(2, sh, guard, "d.timing.event", "d.timing._event")}))"

    case class Result(read: Seq[String], rows: Option[DataFrame], err: Option[String])
    def run(sql: String): Result = try {
      val df = spark.sql(sql)
      Result(readLeaves(df), Some(df), None)
    } catch { case e: Throwable => Result(Nil, None, Some(String.valueOf(e.getMessage).take(300).replace("\n", " "))) }

    def bytesOf(ls: Seq[String]): Long = ls.map(l => bytes.getOrElse(l, 0L)).sum
    def plain(x: String) = s"select id, $x as nm from t"
    def inl(x: String) = s"select id, inline(nm) from (select id, $x as nm from t)"
    def key(df: DataFrame) = df.collect().map(_.toString).sorted.toSeq

    def table(title: String, queries: Seq[(String, String)], refName: String): Unit = {
      println(s"\n### $title\n")
      println("| Shape | Reads | Bytes | Reads _event extension | Equal to (a) of this block |")
      println("| --- | --- | --- | --- | --- |")
      val results = queries.map { case (n, q) => n -> run(q) }.toMap
      val ref = results(refName).rows.map(key)
      for ((n, _) <- queries) {
        val r = results(n)
        r.err match {
          case Some(e) => println(s"| $n | ERROR: $e | | | |")
          case None =>
            val ls = r.read
            val extRead = ls.exists(_.contains("_event[].extension"))
            val eq = ref.map { rk =>
              val k = key(r.rows.get)
              if (k == rk) "yes" else {
                val a = r.rows.get; val rdf = results(refName).rows.get
                val sch = a.schema == rdf.schema
                val d1 = if (sch) a.except(rdf).count() else -1
                val d2 = if (sch) rdf.except(a).count() else -1
                s"**NO** (${k.size} vs ${rk.size} rows; except $d1/$d2)"
              }
            }.getOrElse("n/a")
            println(s"| $n | ${show(ls)} | ${bytesOf(ls)} | ${if (extRead) "**yes**" else "no"} | $eq |")
        }
      }
    }

    for (ctx <- Seq(1, 2)) {
      val ctl = if (ctx == 1) "timing.event" else "flatten(transform(dosage, d -> d.timing.event))"
      for (guard <- Seq(false, true)) {
        val gs = if (guard) "guarded" else "unguarded"
        val qs = Seq("0 control (value only)" -> plain(ctl)) ++
          shapesFor(ctx).map(s => s"($s)" -> plain(expr(ctx, s, guard)))
        table(s"Context $ctx, plain projection, $gs", qs, "(a)")
        val qi = shapesFor(ctx).map(s => s"($s)" -> inl(expr(ctx, s, guard)))
        table(s"Context $ctx, compute-first inline, $gs", qi, "(a)")
      }
    }

    // Cross-check: every shape, guarded or not, against the guarded (a), in both modes.
    println("\n### Cross-check against guarded (a), by context and mode\n")
    println("| Context | Mode | Shape | Guard | Equal to guarded (a) |")
    println("| --- | --- | --- | --- | --- |")
    for (ctx <- Seq(1, 2); mode <- Seq("plain", "inline"); s <- shapesFor(ctx); gd <- Seq(false, true)) {
      val wrap = (x: String) => if (mode == "plain") plain(x) else inl(x)
      val ref = key(spark.sql(wrap(expr(ctx, "a", true))))
      val got = try { if (key(spark.sql(wrap(expr(ctx, s, gd)))) == ref) "yes" else "**NO**" }
      catch { case e: Throwable => "ERROR" }
      println(s"| $ctx | $mode | ($s) | ${if (gd) "guarded" else "unguarded"} | $got |")
    }

    println("\n### Extra controls: does start-only access prune inside a dosage lambda?\n")
    println("| Control | Reads | Bytes | Reads _event extension |")
    println("| --- | --- | --- | --- |")
    val controls = Seq(
      "x1 flatten(transform(dosage, d -> transform(d.timing._event, x -> x.__start)))" ->
        "flatten(transform(dosage, d -> transform(d.timing._event, x -> x.__start)))",
      "x2 flatten(transform(dosage, d -> d.timing._event.__start))" ->
        "flatten(transform(dosage, d -> d.timing._event.__start))",
      "x3 transform(dosage.timing._event, m -> transform(m, x -> x.__start))" ->
        "transform(dosage.timing._event, m -> transform(m, x -> x.__start))",
      "x4 flatten(transform(dosage, d -> arrays_zip(d.timing.event, d.timing._event.__start)))" ->
        "flatten(transform(dosage, d -> arrays_zip(d.timing.event, d.timing._event.__start)))",
      "x5 flatten(transform(dosage, d -> arrays_zip(d.timing.event, transform(d.timing._event, x -> x.__start))))" ->
        "flatten(transform(dosage, d -> arrays_zip(d.timing.event, transform(d.timing._event, x -> x.__start))))")
    for ((n, x) <- controls) {
      val r = run(plain(x))
      r.err match {
        case Some(e) => println(s"| $n | ERROR: $e | | |")
        case None =>
          println(s"| $n | ${show(r.read)} | ${bytesOf(r.read)} | ${if (r.read.exists(_.contains("_event[].extension"))) "**yes**" else "no"} |")
      }
    }

    println("\n### Null and length behaviour (context 1, plain, edge rows)\n")
    val edgeIds = Seq("p3", "p6", "p9", "p12", "p15", "p18", "p21", "p24", "p27", "p33")
    for (gd <- Seq(false, true); s <- shapes) {
      val df = spark.sql(s"select id, ${expr(1, s, gd)} as nm from t where id in " +
        s"(${edgeIds.map("'" + _ + "'").mkString(",")}) order by id")
      println(s"\n(${s}) ${if (gd) "guarded" else "unguarded"}:")
      df.collect().foreach { r =>
        println(s"  ${r.getString(0)} -> ${Option(r.get(1)).map(_.toString.take(170)).getOrElse("NULL")}")
      }
    }
    spark.stop()
  }
}
