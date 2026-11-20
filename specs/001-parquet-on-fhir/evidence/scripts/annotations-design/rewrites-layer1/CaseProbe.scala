package spike

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.types._

/**
 * Cases 1 to 4: writes a small Parquet file with extension-heavy `_x` groups and edge-case nulls,
 * then runs each query with the rule off and on, printing the leaves Parquet reads and whether the
 * results are identical. Usage: CaseProbe <data dir>.
 */
object CaseProbe {

  val META = "struct<id:string,extension:array<struct<url:string,valueString:string>>," +
    "__start:string,__end:string>"
  val EXT = "array<struct<url:string,valueString:string>>"
  val PERIOD = s"struct<start:string,end:string,_start:$META,_end:$META>"
  val SCHEMA: String = Seq(
    "id string",
    "birthDate string",
    s"_birthDate $META",
    s"name array<struct<id:string,extension:$EXT,family:string,_family:$META," +
      s"given:array<string>,_given:array<$META>,period:$PERIOD>>",
    s"location array<struct<extension:$EXT,period:$PERIOD>>",
    s"dosage array<struct<extension:$EXT,text:string,timing:struct<extension:$EXT," +
      s"event:array<string>,_event:array<$META>>>>"
  ).mkString(", ")

  def ext(tag: String): String =
    (1 to 5).map(i => s"""{"url":"http://ex/$i","valueString":"$tag-payload-$i-${"x" * 40}"}""")
      .mkString("[", ",", "]")
  def q(v: String): String = if (v == null) "null" else "\"" + v + "\""
  def meta(tag: String, start: String = "2020-01-01", id: String = "m"): String =
    s"""{"id":${q(id)},"extension":${ext(tag)},"__start":${q(start)},"__end":${q(start)}}"""
  def period(start: String): String =
    s"""{"start":${q(start)},"end":${q(start)},"_start":${meta("ps", start)},"_end":${meta("pe", start)}}"""

  /** Rows: ordinary ones plus the null shapes the rule must keep. */
  def rows(n: Int): Seq[String] = {
    val ordinary = (0 until n).map { i =>
      val y = 1950 + i % 70
      s"""{"id":"p$i","birthDate":"$y-01-01","_birthDate":${meta("bd", s"$y-01-01")},
         |"name":[{"id":"n","extension":${ext("n")},"family":"F$i","_family":${meta("f")},
         |"given":["a$i","b$i"],"_given":[${meta("g1")},null],"period":${period(s"$y-02-02")}}],
         |"location":[{"extension":${ext("l")},"period":${period(s"$y-03-03")}},
         |{"extension":${ext("l")},"period":${period("2021-01-01")}}],
         |"dosage":[{"extension":${ext("d")},"text":"t","timing":{"extension":${ext("t")},
         |"event":["$y-04-04","$y-05-05"],"_event":[${meta("e1", s"$y-04-04")},${meta("e2", null)}]}}]}"""
        .stripMargin.replace("\n", "")
    }
    val edge = Seq(
      // A null element; an element present only through its extension (every read leaf null);
      // a null _birthDate.
      s"""{"id":"e1","birthDate":"1980-01-01","_birthDate":null,
         |"name":[null,{"id":null,"extension":${ext("only")},"family":null,"_family":null,
         |"given":null,"_given":null,"period":null}],
         |"location":[null,{"extension":${ext("l")},"period":null},
         |{"extension":null,"period":{"start":null,"end":null,"_start":${meta("x")},"_end":null}}],
         |"dosage":[null,{"extension":null,"text":null,"timing":null},
         |{"extension":null,"text":"t","timing":{"extension":null,"event":null,"_event":null}}]}"""
        .stripMargin.replace("\n", ""),
      // A non-null _birthDate whose id and __start are null: the group exists only through its
      // extensions.
      s"""{"id":"e2","birthDate":null,"_birthDate":${meta("bd", null, null)},
         |"name":null,"location":[],"dosage":[{"extension":null,"text":null,
         |"timing":{"extension":null,"event":["2001-01-01",null],"_event":[null,${meta("e", null, null)}]}}]}"""
        .stripMargin.replace("\n", ""),
      """{"id":"e3"}"""
    )
    ordinary ++ edge
  }

  val S = "named_struct('value', birthDate, 'meta', _birthDate)"
  val CASES: Seq[(String, String)] = Seq(
    "1a filter(name, x -> x.family is not null).family" ->
      "select filter(name, x -> x.family is not null).family as r from t",
    "1b engine traversal: filter(filter(name, x -> x is not null).family, y -> y is not null)" ->
      "select filter(filter(name, x -> x is not null).family, y -> y is not null) as r from t",
    "1c transform(name, x -> x.family)" ->
      "select transform(name, x -> x.family) as r from t",
    "1d 1b inside inline(array(struct(...)))" ->
      ("select inline(array(struct(filter(filter(name, x -> x is not null).family, " +
        "y -> y is not null) as r))) from t"),
    "1e control: filter(name, x -> x is not null) output whole" ->
      "select filter(name, x -> x is not null) as r from t",
    "1f control: transform(name, x -> to_json(x)) (whole use)" ->
      "select transform(name, x -> to_json(x)) as r from t",
    "1g exists(name, x -> x.period.`start` > '1990')" ->
      "select id, exists(filter(name, x -> x is not null), x -> x.period.`start` > '1990') as r from t",
    "1h size(filter(name, x -> x is not null)) (presence only)" ->
      "select size(filter(name, x -> x is not null)) as r from t",
    "1i size(filter(location, l -> l is not null)) (presence, no scalar field at the top)" ->
      "select size(filter(location, l -> l is not null)) as r from t",
    "1j control: two arrays of _x pruned differently meet in concat (guard)" ->
      ("select _birthDate.id as i, transform(concat(array(_birthDate), transform(name, " +
        "x -> x._family)), m -> m.__start) as r from t"),
    "1k nested group null check: filter(name, x -> x._family is not null).family" ->
      "select filter(name, x -> x._family is not null).family as r from t",
    "2a location.period.where(start > c).start, engine shape" ->
      ("select filter(filter(filter(location, l -> l is not null).period, " +
        "p -> p is not null), p -> p.`start` > '1990').`start` as r from t"),
    "2b the same, comparing the annotation p._start.__start" ->
      ("select filter(filter(filter(location, l -> l is not null).period, " +
        "p -> p is not null), p -> p._start.__start > '1990').`start` as r from t"),
    "2c 2a inside inline" ->
      ("select inline(array(struct(filter(filter(filter(location, l -> l is not null)" +
        ".period, p -> p is not null), p -> p.`start` > '1990').`start` as r))) from t"),
    "3a flatten(...timing.event) paired with _event[].__start" ->
      ("select arrays_zip(flatten(filter(dosage, d -> d is not null).timing.event), " +
        "flatten(filter(dosage, d -> d is not null).timing._event).__start) as r from t"),
    "3b 3a inside inline" ->
      ("select inline(array(struct(arrays_zip(flatten(filter(dosage, d -> d is not null)" +
        ".timing.event), flatten(filter(dosage, d -> d is not null).timing._event).__start) " +
        "as r))) from t"),
    "3c repeating primitive under repeating parent, raw meta paired by zip_with, output .value" ->
      (s"select inline(array(struct(transform(flatten(transform(filter(dosage, d -> d is not null), " +
        "d -> zip_with(d.timing.event, d.timing._event, (v, m) -> named_struct('value', v, 'meta', m)))), " +
        "e -> e.value) as r))) from t"),
    "3d 3c filtering on e.meta.__start" ->
      (s"select inline(array(struct(transform(filter(flatten(transform(filter(dosage, d -> d is not null), " +
        "d -> zip_with(d.timing.event, d.timing._event, (v, m) -> named_struct('value', v, 'meta', m)))), " +
        "e -> e.meta.__start > '1990'), e -> e.value) as r))) from t"),
    "3e 3c paired by arrays_zip instead, output .event" ->
      (s"select inline(array(struct(transform(flatten(transform(filter(dosage, d -> d is not null), " +
        "d -> arrays_zip(d.timing.event, d.timing._event))), e -> e.event) as r))) from t"),
    "3f 3e filtering on e._event.__start" ->
      (s"select inline(array(struct(transform(filter(flatten(transform(filter(dosage, d -> d is not null), " +
        "d -> arrays_zip(d.timing.event, d.timing._event))), e -> e._event.__start > '1990'), " +
        "e -> e.event) as r))) from t"),
    "4a singular struct(value, meta), output only, in inline" ->
      (s"select inline(array(struct(transform(filter(array($S), v -> v is not null), " +
        "e -> e.value) as r))) from t"),
    "4b 4a with a comparison on e.meta.__start" ->
      (s"select inline(array(struct(transform(filter(array($S), v -> v is not null " +
        "and v.meta.__start > '1990'), e -> e.value) as r))) from t"),
    "4c nullness of a pruned meta: filter(array(s), v -> v.meta is not null)" ->
      (s"select inline(array(struct(transform(filter(array($S), v -> v.meta is not null), " +
        "e -> e.value) as r))) from t"),
    "4d plain select s.value (control, already prunes)" ->
      s"select $S.value as r from t"
  )

  /** The leaves a scan reads, as dotted paths with [] for arrays. */
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

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.adaptive.enabled", args.contains("-aqe").toString)
      .config("spark.sql.parquet.enableVectorizedReader", (!args.contains("-rowreader")).toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val dir = args(0) + "/cases.parquet"
    spark.read.schema(SCHEMA).json(rows(200).toDS()).coalesce(1).write.mode("overwrite").parquet(dir)
    val full = spark.read.parquet(dir).schema
    spark.read.parquet(dir).createOrReplaceTempView("t")
    LambdaPruning.verbose = args.contains("-v")

    println("| Case | Reads without the rule | Reads with it | Results equal |")
    println("| --- | --- | --- | --- |")
    for ((name, sql) <- CASES) {
      LambdaPruning.uninstall(spark)
      val off = spark.sql(sql)
      val offRead = compact(readSchema(off), full)
      val offRows = off.collect().map(_.toString).sorted.toSeq
      LambdaPruning.install(spark)
      val on = spark.sql(sql)
      val onRead = compact(readSchema(on), full)
      val onRows = on.collect().map(_.toString).sorted.toSeq
      val same = offRows == onRows && off.schema == on.schema
      println(s"| $name | $offRead | $onRead | ${if (same) "yes" else "**NO**"} (${onRows.size} rows) |")
      if (!same) {
        offRows.zip(onRows).filter { case (a, b) => a != b }.take(3).foreach { case (a, b) =>
          println(s"    off: $a\n    on:  $b")
        }
      }
    }
    // The edge rows, to show that the null shapes are present and read the same.
    LambdaPruning.install(spark)
    spark.sql("select id, size(filter(name, x -> x is not null)) as names, " +
      "size(filter(location, l -> l is not null)) as locations, " +
      s"transform(filter(array($S), v -> v.meta is not null), e -> e.value) as meta_present, " +
      "filter(name, x -> x._family is not null).family as fam " +
      "from t where id like 'e%' order by id").show(false)
    LambdaPruning.uninstall(spark)
    spark.stop()
  }
}
