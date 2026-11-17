package spike

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.types._

/**
 * The D3a-complex sketch: for each case, a baseline in the engine's shape (filter lambdas over
 * whole columns) and a D3a-complex expression (leaf extraction chains outside any lambda, re-zipped
 * where a lambda needs a narrow element), each run bare and inside the view wrapper. Prints the
 * leaves each scan reads, the Parquet bytes those leaves occupy, and whether the results are equal.
 * Usage: D3aProbe <data dir> [-rowreader].
 */
object D3aProbe {

  val META = "struct<id:string,extension:array<struct<url:string,valueString:string>>," +
    "__start:string,__end:string,__foreign:string>"
  val EXT = "array<struct<url:string,valueString:string>>"
  // `__foreign` stands for an annotation written by another engine, which Pathling does not know.
  val PERIOD = s"struct<id:string,extension:$EXT,start:string,end:string,_start:$META,_end:$META,__foreign:string>"
  val ENC = s"id string, location array<struct<id:string,extension:$EXT,status:string,period:$PERIOD,__foreign:string>>"
  val TIMING = s"struct<id:string,extension:$EXT,event:array<string>,_event:array<$META>,__foreign:string>"
  val MED = s"id string, dosageInstruction array<struct<id:string,extension:$EXT,text:string,timing:$TIMING>>"

  /** Prints report lines with a marker, so that run.sh can drop Spark's logging from the output. */
  def println(s: Any = ""): Unit =
    String.valueOf(s).split("\n", -1).foreach(l => System.out.println("@@" + l))

  // ---- Data -------------------------------------------------------------------------------------

  def q(v: String): String = if (v == null) "null" else "\"" + v + "\""
  def payload(rnd: Random): String = (1 to 40).map(_ => "0123456789abcdef"(rnd.nextInt(16))).mkString
  def ext(rnd: Random): String =
    (1 to 5).map(i => s"""{"url":"http://ex/$i","valueString":"${payload(rnd)}"}""").mkString("[", ",", "]")
  def foreign(rnd: Random): String = "\"" + (1 to 200).map(_ => "abcdefghij"(rnd.nextInt(10))).mkString + "\""
  def meta(rnd: Random, start: String, id: String = "m"): String =
    s"""{"id":${q(id)},"extension":${ext(rnd)},"__start":${q(start)},"__end":${q(start)},"__foreign":${foreign(rnd)}}"""
  def period(rnd: Random, start: String, end: String, withMeta: Boolean = true): String = {
    val ms = if (withMeta && start != null) meta(rnd, start) else "null"
    val me = if (withMeta && end != null) meta(rnd, end) else "null"
    s"""{"id":"p","extension":${ext(rnd)},"start":${q(start)},"end":${q(end)},"_start":$ms,"_end":$me,"__foreign":${foreign(rnd)}}"""
  }
  val NULLPERIOD = """{"id":null,"extension":null,"start":null,"end":null,"_start":null,"_end":null}"""
  def loc(rnd: Random, per: String, status: String = "active", withExt: Boolean = true): String =
    s"""{"id":"l","extension":${if (withExt) ext(rnd) else "null"},"status":${q(status)},"period":$per,"__foreign":${foreign(rnd)}}"""

  def encRows(n: Int): Seq[String] = {
    val rnd = new Random(1)
    val ordinary = (0 until n).map { i =>
      val y = 1950 + i % 70
      s"""{"id":"p$i","location":[${loc(rnd, period(rnd, s"$y-03-03", s"$y-04-04"))},
         |${loc(rnd, period(rnd, "2021-01-01", "2021-02-02"))}]}""".stripMargin.replace("\n", "")
    }
    val edge = Seq(
      """{"id":"E1_location_null","location":null}""",
      """{"id":"E2_location_empty","location":[]}""",
      s"""{"id":"E3_element_extension_only","location":[{"id":null,"extension":${ext(rnd)},"status":null,"period":null}]}""",
      s"""{"id":"E4_period_null","location":[${loc(rnd, "null")}]}""",
      s"""{"id":"E5_period_end_only","location":[${loc(rnd, period(rnd, null, "2021-01-01"))}]}""",
      s"""{"id":"E6_period_extension_only","location":[${loc(rnd,
        s"""{"id":null,"extension":${ext(rnd)},"start":null,"end":null,"_start":null,"_end":null}""")}]}""",
      s"""{"id":"E7_mixed","location":[${loc(rnd, period(rnd, "2005-05-05", "2006-06-06"))},
         |${loc(rnd, period(rnd, "1999-01-01", "1999-02-02"))},
         |{"id":null,"extension":${ext(rnd)},"status":null,"period":null},
         |${loc(rnd, period(rnd, null, "2010-01-01"))}]}""".stripMargin.replace("\n", ""),
      s"""{"id":"E8_null_element_hypothetical","location":[null,${loc(rnd, period(rnd, "2011-01-01", "2011-02-02"))}]}""",
      s"""{"id":"E9_period_all_null_struct","location":[${loc(rnd, NULLPERIOD)}]}""",
      s"""{"id":"E10_start_only_in_annotation","location":[${loc(rnd,
        s"""{"id":null,"extension":null,"start":null,"end":null,"_start":${meta(rnd, "2012-01-01")},"_end":null}""")}]}""",
      s"""{"id":"E11_start_annotation_extension_only","location":[${loc(rnd,
        s"""{"id":null,"extension":null,"start":null,"end":null,"_start":{"id":null,"extension":${ext(rnd)},"__start":null,"__end":null},"_end":null}""")}]}"""
    )
    ordinary ++ edge
  }

  def timing(rnd: Random, events: Seq[String], metas: Seq[String]): String = {
    val ev = if (events == null) "null" else events.map(q).mkString("[", ",", "]")
    val me = if (metas == null) "null" else metas.mkString("[", ",", "]")
    s"""{"id":"t","extension":${ext(rnd)},"event":$ev,"_event":$me,"__foreign":${foreign(rnd)}}"""
  }
  def dosage(rnd: Random, tim: String): String =
    s"""{"id":"d","extension":${ext(rnd)},"text":"t","timing":$tim}"""

  def medRows(n: Int): Seq[String] = {
    val rnd = new Random(2)
    val ordinary = (0 until n).map { i =>
      val y = 1990 + i % 40
      val t1 = timing(rnd, Seq(s"$y-04-04", s"$y-05-05"), Seq(meta(rnd, s"$y-04-04"), meta(rnd, s"$y-05-05")))
      val t2 = timing(rnd, Seq("2015-01-01"), Seq(meta(rnd, "2015-01-01")))
      s"""{"id":"p$i","dosageInstruction":[${dosage(rnd, t1)},${dosage(rnd, t2)}]}"""
    }
    val edge = Seq(
      """{"id":"M1_dosage_null","dosageInstruction":null}""",
      """{"id":"M2_dosage_empty","dosageInstruction":[]}""",
      s"""{"id":"M3_timing_null","dosageInstruction":[${dosage(rnd, "null")}]}""",
      s"""{"id":"M4_event_empty","dosageInstruction":[${dosage(rnd, timing(rnd, Seq(), Seq()))}]}""",
      s"""{"id":"M5_timing_extension_only","dosageInstruction":[${dosage(rnd, timing(rnd, null, null))}]}""",
      s"""{"id":"M6_two_dosages","dosageInstruction":[${dosage(rnd, timing(rnd, Seq("2021-01-01", "2019-01-01"),
        Seq(meta(rnd, "2021-01-01"), meta(rnd, "2019-01-01"))))},${dosage(rnd, timing(rnd, Seq("2022-02-02"),
        Seq(meta(rnd, "2022-02-02"))))}]}""",
      s"""{"id":"M7_null_event_entry","dosageInstruction":[${dosage(rnd, timing(rnd, Seq(null, "2023-01-01"),
        Seq(meta(rnd, null), meta(rnd, "2023-01-01"))))}]}""",
      s"""{"id":"M8_event_without_annotations","dosageInstruction":[${dosage(rnd, timing(rnd, Seq("2024-01-01", "2025-01-01"), null))}]}""",
      s"""{"id":"M9_event_longer_than_annotations_hypothetical","dosageInstruction":[${dosage(rnd,
        timing(rnd, Seq("2024-01-01", "2025-01-01"), Seq(meta(rnd, "2024-01-01"))))}]}""",
      s"""{"id":"M10_dosage_extension_only","dosageInstruction":[{"id":null,"extension":${ext(rnd)},"text":null,"timing":null}]}""",
      s"""{"id":"M11_second_dosage_first_null_timing","dosageInstruction":[${dosage(rnd, "null")},${dosage(rnd,
        timing(rnd, Seq("2030-01-01"), Seq(meta(rnd, "2030-01-01"))))}]}"""
    )
    ordinary ++ edge
  }

  // ---- Cases ------------------------------------------------------------------------------------

  /** One case: the engine-shaped baseline and the D3a-complex form of the same expression. */
  case class Case(name: String, table: String, base: String, d3a: String, expectEqual: Boolean = true)

  val X = "'2000-10-10'"
  val PER_B = "filter(filter(location, x -> x is not null).period, p -> p is not null)"
  val EMPTY_STR = "cast(array() as array<string>)"
  val TIM_B = "filter(filter(dosageInstruction, x -> x is not null).timing, t -> t is not null)"
  val EV_B = s"filter(flatten(transform($TIM_B, t -> coalesce(t.event, $EMPTY_STR))), e -> e is not null)"
  val EV_D = s"filter(flatten(transform(dosageInstruction.timing.event, e -> coalesce(e, $EMPTY_STR))), e -> e is not null)"
  // Where on the annotation: the event is kept when its aligned annotation start exceeds 2020.
  val EV_WHERE_B =
    s"""filter(flatten(transform($TIM_B, t -> coalesce(transform(filter(zip_with(t.event, t._event,
       |(e, m) -> named_struct('e', e, 's', m.__start)), z -> z.s > '2020'), z -> z.e), $EMPTY_STR))),
       |e -> e is not null)""".stripMargin.replace("\n", " ")
  // Two-level re-zip: the outer arrays are zipped at the dosage level, the inner ones at the event
  // level. The annotation array is read whole (see "Nested arrays" in the write-up).
  val EV_WHERE_D =
    s"""filter(flatten(zip_with(dosageInstruction.timing.event, dosageInstruction.timing._event,
       |(e, m) -> coalesce(transform(filter(zip_with(e, m, (a, b) -> named_struct('e', a, 's', b.__start)),
       |z -> z.s > '2020'), z -> z.e), $EMPTY_STR))), e -> e is not null)""".stripMargin.replace("\n", " ")

  val PRES_LEAVES: Seq[String] = Seq("id", "start", "end", "_start.id", "_start.__start", "_start.__end",
    "_end.id", "_end.__start", "_end.__end").map(l => s"location.period.$l")
  // Element-wise "any of these leaves is non-null", folded with zip_with over the aligned leaf arrays.
  def anyLeaf(ls: Seq[String]): String = ls.tail.foldLeft(s"transform(${ls.head}, v -> v is not null)") {
    (acc, l) => s"zip_with($acc, $l, (a, v) -> a or v is not null)"
  }
  val ANY_LEAF: String = anyLeaf(PRES_LEAVES)
  val EXT_LEAVES: Seq[String] = Seq("location.period._start.extension", "location.period._end.extension")
  // Under ANSI, size(null) is null; a count is 0 for an empty collection, so both sides coalesce.
  def n(x: String): String = s"coalesce(size($x), 0)"

  val CASES: Seq[Case] = Seq(
    Case("S4a  location.period.where(start > X).start", "enc",
      s"filter(transform(filter($PER_B, p -> p.start > $X), p -> p.start), v -> v is not null)",
      s"filter(location.period.start, s -> s > $X)"),
    Case("S4b  location.period.where(start > X), the narrowed period", "enc",
      s"filter($PER_B, p -> p.start > $X)",
      s"filter(location.period, p -> p.start > $X)"),
    Case("S4c  location.period.where(start > X).count()", "enc",
      n(s"filter($PER_B, p -> p.start > $X)"),
      n(s"filter(location.period.start, s -> s > $X)")),
    Case("S4d  location.period.where(start > X).exists()", "enc",
      s"${n(s"filter($PER_B, p -> p.start > $X)")} > 0",
      s"coalesce(exists(location.period.start, s -> s > $X), false)"),
    Case("S4e  location.period.count(), period read whole", "enc",
      n(PER_B),
      n("filter(location.period, p -> p is not null)")),
    Case("S4f  location.period.exists(), period read whole", "enc",
      s"${n(PER_B)} > 0",
      s"${n("filter(location.period, p -> p is not null)")} > 0"),
    Case("S4g  location.period.where(_start.__start > X).start", "enc",
      s"filter(transform(filter($PER_B, p -> p._start.__start > $X), p -> p.start), v -> v is not null)",
      s"""filter(transform(filter(arrays_zip(location.period.start, location.period._start.__start),
         |z -> z.__start > $X), z -> z.start), v -> v is not null)""".stripMargin.replace("\n", " ")),
    Case("S4h  location.period.count(), nine non-extension leaves (control)", "enc",
      n(PER_B),
      n(s"filter($ANY_LEAF, b -> b)"), expectEqual = false),
    Case("S4k  location.period.count(), nine leaves plus period.extension (control)", "enc",
      n(PER_B),
      n(s"filter(${anyLeaf(PRES_LEAVES :+ "location.period.extension")}, b -> b)"), expectEqual = false),
    Case("S4l  location.period.count(), every period leaf incl. all three extensions (control)", "enc",
      n(PER_B),
      n(s"filter(${anyLeaf(PRES_LEAVES ++ Seq("location.period.extension") ++ EXT_LEAVES)}, b -> b)"),
      expectEqual = false),
    Case("S4i  location.period.count(), start leaf only (control)", "enc",
      n(PER_B),
      n("filter(location.period.start, s -> s is not null)"), expectEqual = false),
    Case("S4j  location.period.count(), element count of location (control)", "enc",
      n(PER_B),
      n("location.period.start"), expectEqual = false),
    Case("N1  dosageInstruction.timing.event", "med", EV_B, EV_D),
    Case("N2  dosageInstruction.timing.event.where($this > 2020) on _event.__start", "med",
      EV_WHERE_B, EV_WHERE_D),
    Case("N3  dosageInstruction.timing.event.count()", "med", n(EV_B), n(EV_D)),
    Case("N4  dosageInstruction.timing.event.exists()", "med", s"${n(EV_B)} > 0", s"${n(EV_D)} > 0")
  )

  // ---- Reporting --------------------------------------------------------------------------------

  def leaves(t: DataType, prefix: String): Seq[String] = t match {
    case st: StructType => st.fields.toSeq.flatMap(f =>
      leaves(f.dataType, if (prefix.isEmpty) f.name else s"$prefix.${f.name}"))
    case ArrayType(et, _) => leaves(et, prefix + "[]")
    case _ => Seq(prefix)
  }

  /** Compact form: collapses a fully read subtree to its root. */
  def compact(read: StructType, full: StructType): String = {
    def go(r: DataType, f: DataType, path: String): Seq[String] = (r, f) match {
      case _ if r == f => Seq(if (path.isEmpty) "*" else s"$path (whole)")
      case (rs: StructType, fs: StructType) => rs.fields.toSeq.flatMap { rf =>
        go(rf.dataType, fs(rf.name).dataType, if (path.isEmpty) rf.name else s"$path.${rf.name}")
      }
      case (ArrayType(re, _), ArrayType(fe, _)) => go(re, fe, path + "[]")
      case _ => Seq(path)
    }
    read.fields.toSeq.flatMap(rf => go(rf.dataType, full(rf.name).dataType, rf.name)).mkString(", ")
  }

  def readSchema(df: DataFrame): StructType =
    df.queryExecution.executedPlan.collect { case s: FileSourceScanExec => s.requiredSchema }
      .reduceOption((a, b) => StructType(a.fields ++ b.fields)).getOrElse(StructType(Nil))

  /** Compressed bytes of every leaf column chunk in a Parquet directory, keyed like `leaves`. */
  def leafBytes(dir: String): Map[String, Long] = {
    val conf = new Configuration()
    val fs = new Path(dir).getFileSystem(conf)
    val files = fs.listStatus(new Path(dir)).map(_.getPath).filter(_.getName.endsWith(".parquet"))
    files.flatMap { p =>
      val r = ParquetFileReader.open(HadoopInputFile.fromPath(p, conf))
      try {
        import scala.jdk.CollectionConverters._
        r.getFooter.getBlocks.asScala.flatMap(_.getColumns.asScala.map { c =>
          c.getPath.toArray.mkString(".").replace(".list.element", "[]").replace(".list", "[]") ->
            c.getTotalSize
        })
      } finally r.close()
    }.groupMapReduce(_._1)(_._2)(_ + _)
  }

  def bytesOf(read: StructType, lb: Map[String, Long]): Long =
    leaves(read, "").map(l => lb.getOrElse(l, 0L)).sum

  def kib(b: Long): String = f"${b / 1024.0}%.0f"

  def rowsOf(df: DataFrame): Seq[String] = df.collect().map(_.toString).sorted.toSeq

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.parquet.enableVectorizedReader", (!args.contains("-rowreader")).toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val dirs = Map("enc" -> (args(0) + "/enc.parquet"), "med" -> (args(0) + "/med.parquet"))
    spark.read.schema(ENC).json(encRows(1500).toDS()).coalesce(1).write.mode("overwrite").parquet(dirs("enc"))
    spark.read.schema(MED).json(medRows(1500).toDS()).coalesce(1).write.mode("overwrite").parquet(dirs("med"))
    val fulls = dirs.map { case (k, d) => k -> spark.read.parquet(d).schema }
    val lbs = dirs.map { case (k, d) => k -> leafBytes(d) }
    dirs.foreach { case (k, d) => spark.read.parquet(d).createOrReplaceTempView(k) }
    println(s"reader: ${if (args.contains("-rowreader")) "row-based" else "vectorized"}")
    for ((k, lb) <- lbs) println(s"$k: whole file ${kib(lb.values.sum)} KiB")

    println("\n## Cases\n")
    println("| Case | Form | Baseline reads (KiB) | D3a reads (KiB) | `__foreign` read: baseline / D3a | Results equal |")
    println("| --- | --- | --- | --- | --- | --- |")
    def hasForeign(rs: StructType): String = if (leaves(rs, "").exists(_.contains("__foreign"))) "yes" else "no"
    val edgeLines = scala.collection.mutable.ArrayBuffer[String]()
    for (c <- CASES; wrap <- Seq(false, true)) {
      def sql(e: String): String =
        if (wrap) s"select inline(array(named_struct('id', id, 'r', $e))) from ${c.table}"
        else s"select id, $e as r from ${c.table}"
      val full = fulls(c.table)
      val b = spark.sql(sql(c.base)); val d = spark.sql(sql(c.d3a))
      val br = readSchema(b); val dr = readSchema(d)
      val brows = rowsOf(b); val drows = rowsOf(d)
      val same = brows == drows && b.schema.simpleString == d.schema.simpleString
      val verdict = (if (same) "yes" else "**NO**") + (if (!c.expectEqual) " (control)" else "")
      println(s"| ${c.name} | ${if (wrap) "view" else "bare"} | ${compact(br, full)} (${kib(bytesOf(br, lbs(c.table)))}) " +
        s"| ${compact(dr, full)} (${kib(bytesOf(dr, lbs(c.table)))}) | ${hasForeign(br)} / ${hasForeign(dr)} | $verdict |")
      if (!wrap) {
        val bm = b.collect().map(r => r.getString(0) -> String.valueOf(r.get(1))).toMap
        val dm = d.collect().map(r => r.getString(0) -> String.valueOf(r.get(1))).toMap
        val ids = bm.keys.filter(k => k.startsWith("E") || k.startsWith("M")).toSeq.sorted
        val diff = ids.filter(i => bm(i) != dm(i))
        if (!c.expectEqual || diff.nonEmpty)
          edgeLines += s"${c.name}: edge rows that differ: ${if (diff.isEmpty) "none" else diff.mkString(", ")}" +
            diff.map(i => s"\n    $i  baseline=${bm(i)}  d3a=${dm(i)}").mkString
        val ordinary = bm.keys.filter(k => k.startsWith("p")).count(i => bm(i) != dm(i))
        if (ordinary > 0) edgeLines += s"${c.name}: ordinary rows that differ: $ordinary"
      }
    }
    println("\n## Edge rows that differ\n"); edgeLines.foreach(println)

    println("\n## Edge row values, selected cases (baseline == d3a)\n")
    for (n <- Seq("S4b", "S4c", "S4e", "N1", "N2")) {
      val c = CASES.find(_.name.startsWith(n)).get
      val d = spark.sql(s"select id, ${c.d3a} as r from ${c.table}")
      println(s"${c.name}")
      d.collect().filter(r => r.getString(0).matches("[EM]\\d.*")).sortBy(_.getString(0)).foreach { r =>
        val s = String.valueOf(r.get(1)); println(s"    ${r.getString(0)} -> ${if (s.length > 110) s.take(110) + "..." else s}")
      }
    }

    presence(spark, dirs, fulls, lbs)
    indexed(spark, fulls, lbs)
    nested(spark, fulls("med"), lbs("med"))
  }

  // ---- Presence ---------------------------------------------------------------------------------

  def presence(spark: SparkSession, dirs: Map[String, String], fulls: Map[String, StructType],
      lbs: Map[String, Map[String, Long]]): Unit = {
    println("\n## Presence\n")
    val full = fulls("enc")
    val lb = lbs("enc")
    def show(label: String, sql: String): Unit = {
      try {
        val df = spark.sql(sql)
        val rs = readSchema(df)
        println(s"- $label -> reads ${compact(rs, full)} (${kib(bytesOf(rs, lb))} KiB)")
      } catch { case e: Exception => println(s"- $label -> ${e.getClass.getSimpleName}: ${e.getMessage.split("\n").head}") }
    }

    println("\nP1. Does GetArrayStructFields preserve length, and are there null elements?")
    val p1 = spark.sql("select id, size(location) as n, size(location.id) as nid, size(location.status) as ns, " +
      "size(location.period.start) as nps from enc").collect()
    println(s"- rows where size(location.<leaf>) differs from size(location): " +
      s"${p1.count(r => !r.isNullAt(1) && (r.getInt(1) != r.getInt(2) || r.getInt(1) != r.getInt(3) || r.getInt(1) != r.getInt(4)))} of ${p1.length}")
    println(s"- null location array: size() = ${p1.find(_.getString(0) == "E1_location_null").get.get(1)} " +
      "(null under ANSI), and the leaf arrays are null too")
    val nulls = spark.sql("select id, size(filter(location, x -> x is null)) as n from enc where size(filter(location, x -> x is null)) > 0").collect()
    println(s"- rows holding a null element in location: ${nulls.map(_.getString(0)).mkString(", ")}")
    println(s"- E8: size(location)=${p1.find(_.getString(0).startsWith("E8")).get.get(1)}, " +
      s"location.id=${spark.sql("select location.id from enc where id like 'E8%'").first.get(0)}, " +
      s"location.period.start=${spark.sql("select location.period.start from enc where id like 'E8%'").first.get(0)}")

    println("\nP2. Which leaf paths can a chain reach? (analysis, then reads)")
    show("location.period.start", "select location.period.start from enc")
    show("location.period._start.__start", "select location.period._start.__start from enc")
    show("location.period.extension.url (two array levels)", "select location.period.extension.url from enc")
    show("location.extension.url (two array levels)", "select location.extension.url from enc")
    show("location.period.extension", "select location.period.extension from enc")
    show("location.period", "select location.period from enc")
    show("size(location.period) (struct level, no lambda)", "select size(location.period) from enc")
    show("location[0].period is null, location[0].period.start", "select location[0].period is null, location[0].period.start from enc")
    show("location.period.start where location[0].period is not null",
      "select location.period.start from enc where location[0].period is not null")

    println("\nP3. Does a reader schema narrowed to one leaf keep struct-null versus field-null?")
    val narrow = StructType(Seq(StructField("id", StringType), StructField("location", ArrayType(StructType(Seq(
      StructField("period", StructType(Seq(StructField("start", StringType))))))))))
    val narrowEnd = StructType(Seq(StructField("id", StringType), StructField("location", ArrayType(StructType(Seq(
      StructField("period", StructType(Seq(StructField("end", StringType))))))))))
    val narrowExt = StructType(Seq(StructField("id", StringType), StructField("location", ArrayType(StructType(Seq(
      StructField("status", StringType)))))))
    val fullDf = spark.read.parquet(dirs("enc"))
    def pnull(df: DataFrame, field: String) =
      df.selectExpr("id", s"transform(location, l -> $field) as v").collect().map(r => r.getString(0) -> String.valueOf(r.get(1))).toMap
    val fullP = pnull(fullDf, "l.period is null")
    for ((label, sch) <- Seq("period.start only" -> narrow, "period.end only" -> narrowEnd)) {
      val nd = spark.read.schema(sch).parquet(dirs("enc"))
      println(s"- reader schema `$label`: read = ${compact(readSchema(nd), full)}")
      val np = pnull(nd, "l.period is null")
      val bad = fullP.keys.filter(k => fullP(k) != np(k)).toSeq.sorted
      println(s"  period-is-null per element equal to the full read on all ${fullP.size} rows: ${bad.isEmpty}" +
        (if (bad.nonEmpty) s" (differs: ${bad.take(5).mkString(", ")})" else ""))
      println(s"  E4 (period null) -> ${np.find(_._1.startsWith("E4")).get._2}, E5 -> ${np.find(_._1.startsWith("E5")).get._2}, " +
        s"E6 (extension only) -> ${np.find(_._1.startsWith("E6")).get._2}, E9 (all-null struct) -> ${np.find(_._1.startsWith("E9")).get._2}")
      val leaf = if (label.contains("start")) "start" else "end"
      val ns = nd.selectExpr("id", s"transform(location, l -> l.period.$leaf) as v").collect()
        .find(_.getString(0).startsWith("E5")).get.get(1)
      println(s"  E5 period.$leaf read as $ns")
    }
    val ne = spark.read.schema(narrowExt).parquet(dirs("enc"))
    val fullL = pnull(fullDf, "l is null"); val nl = pnull(ne, "l is null")
    println(s"- reader schema `location[].status` only: location-element-is-null equal to full read: " +
      s"${fullL.keys.forall(k => fullL(k) == nl(k))}; E3 (extension-only element) -> ${nl.find(_._1.startsWith("E3")).get._2}, " +
      s"E8 (null element) -> ${nl.find(_._1.startsWith("E8")).get._2}")

    println("\nP4. Bytes of the candidate presence leaf sets for `location[].period` (KiB)")
    def sum(ls: Seq[String]) = ls.map(l => lb.getOrElse(l, 0L)).sum
    val allPeriod = lb.keys.filter(_.startsWith("location[].period")).toSeq
    println(s"- location (whole): ${kib(lb.filter(_._1.startsWith("location")).values.sum)}")
    println(s"- location[].period (whole, the D3a struct-level read): ${kib(sum(allPeriod))}")
    println(s"- of which period.extension payload: ${kib(sum(allPeriod.filter(_.startsWith("location[].period.extension"))))}; " +
      s"_start and _end groups: ${kib(sum(allPeriod.filter(l => l.startsWith("location[].period._"))))} " +
      s"(of which their extensions ${kib(sum(allPeriod.filter(l => l.startsWith("location[].period._") && l.contains(".extension"))))})")
    println(s"- one leaf, D3b's choice (period.start): ${kib(sum(Seq("location[].period.start")))}")
    println(s"- nine non-extension leaves (S4h): ${kib(sum(PRES_LEAVES.map(_.replace("location.", "location[].")))) }")
    println(s"- extension.url alone, if it could be reached (location[].period.extension[].url): " +
      s"${kib(sum(Seq("location[].period.extension[].url")))} against the whole extension " +
      s"${kib(sum(allPeriod.filter(_.startsWith("location[].period.extension"))))}")
  }

  // ---- Indexed access ---------------------------------------------------------------------------

  /**
   * Probes accesses rooted at the scan attribute through an array index. Spark's pruning follows
   * them even inside a lambda, because the lambda variable is only the index.
   */
  def indexed(spark: SparkSession, fulls: Map[String, StructType], lbs: Map[String, Map[String, Long]]): Unit = {
    println("\n## Indexed access (probe)\n")
    def show(table: String, label: String, sql: String): Unit = {
      try {
        val df = spark.sql(sql)
        val rs = readSchema(df)
        println(s"- $label -> reads ${compact(rs, fulls(table))} (${kib(bytesOf(rs, lbs(table)))} KiB)")
      } catch { case e: Exception => println(s"- $label -> ${e.getClass.getSimpleName}: ${e.getMessage.split("\n").head}") }
    }
    show("enc", "location[0].period is null", "select location[0].period is null from enc")
    show("enc", "location[0].period.start", "select location[0].period.start from enc")
    show("enc", "id where location[0].period is not null (a Filter condition)",
      "select id from enc where location[0].period is not null")
    show("enc", "id where location[0].period.start is not null",
      "select id from enc where location[0].period.start is not null")
    show("enc", "transform(location.status, (x, i) -> location[i].period is not null)",
      "select transform(location.status, (x, i) -> location[i].period is not null) from enc")
    show("enc", "transform(location.id, (x, i) -> location[i].period.start)",
      "select transform(location.id, (x, i) -> location[i].period.start) from enc")
    show("med", "transform(dosageInstruction.timing.event, (e, i) -> transform(e, (v, j) -> dosageInstruction[i].timing._event[j].__start))",
      "select transform(dosageInstruction.timing.event, (e, i) -> transform(e, (v, j) -> dosageInstruction[i].timing._event[j].__start)) from med")
    show("med", "same, with try_element_at", "select transform(dosageInstruction.timing.event, (e, i) -> transform(e, " +
      "(v, j) -> try_element_at(try_element_at(dosageInstruction, i + 1).timing._event, j + 1).__start)) from med")
  }

  // ---- Nested arrays ----------------------------------------------------------------------------

  def nested(spark: SparkSession, full: StructType, lb: Map[String, Long]): Unit = {
    println("\n## Nested arrays: what a chain can reach\n")
    def show(label: String, sql: String): Unit = {
      try {
        val df = spark.sql(sql)
        val rs = readSchema(df)
        println(s"- $label -> reads ${compact(rs, full)} (${kib(bytesOf(rs, lb))} KiB)")
      } catch { case e: Exception => println(s"- $label -> ${e.getClass.getSimpleName}: ${e.getMessage.split("\n").head}") }
    }
    show("dosageInstruction.timing.event", "select dosageInstruction.timing.event from med")
    show("dosageInstruction.timing._event", "select dosageInstruction.timing._event from med")
    show("dosageInstruction.timing._event.__start (chain)", "select dosageInstruction.timing._event.__start from med")
    show("transform(dosageInstruction.timing._event, a -> transform(a, m -> m.__start))",
      "select transform(dosageInstruction.timing._event, a -> transform(a, m -> m.__start)) from med")
    show("flatten(dosageInstruction.timing._event).__start",
      "select flatten(dosageInstruction.timing._event).__start from med")
    show("explode(dosageInstruction) then d.timing._event.__start",
      "select d.timing._event.__start from med lateral view explode(dosageInstruction) t as d")
    show("explode(dosageInstruction) then d.timing.event",
      "select d.timing.event from med lateral view explode(dosageInstruction) t as d")
    show("inline(dosageInstruction) then timing._event.__start",
      "select timing._event.__start from (select inline(dosageInstruction) from med)")
    show("size(dosageInstruction.timing.event) (event whole for the count)",
      "select size(flatten(dosageInstruction.timing.event)) from med")
    println(s"\nBytes (KiB): dosageInstruction whole ${kib(lb.filter(_._1.startsWith("dosageInstruction")).values.sum)}; " +
      s"timing.event ${kib(lb.filter(_._1.startsWith("dosageInstruction[].timing.event")).values.sum)}; " +
      s"timing._event whole ${kib(lb.filter(_._1.startsWith("dosageInstruction[].timing._event")).values.sum)}; " +
      s"timing._event.__start ${kib(lb.getOrElse("dosageInstruction[].timing._event[].__start", 0L))}")
  }
}
