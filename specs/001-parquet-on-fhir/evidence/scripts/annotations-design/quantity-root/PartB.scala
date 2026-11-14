import au.csiro.pathling.encoders.ColumnFunctions
import au.csiro.pathling.library.PathlingContext
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.qroot.Proto
import org.apache.spark.sql.types._
import spike.LambdaPruning

/**
 * Part B: absent root columns and the tolerant reference. B1 the engine over datasets without
 * `_birthDate` and without `valueQuantity`, and a model of `resolve_or_none`. B2 the flat
 * parent-level layout with `columnOrElse`. B3 every query with the pruning rule installed too.
 * Args: data dir.
 */
object PartB {
  var spark: SparkSession = _

  def digest(df: DataFrame): String = {
    val r = df.select(struct(df.columns.map(c => col("`" + c + "`")): _*).as("s"))
      .agg(count(lit(1)), coalesce(sum(hash(col("s"))).cast("long"), lit(0L))).head()
    s"${r.getLong(0)} rows, hash ${r.getLong(1)}"
  }

  /** Builds the dataset without and with the rule: read schemas and results, and whether they agree. */
  def both(label: String, ref: Option[() => DataFrame] = None)(build: () => DataFrame): Unit = {
    val cells = Seq(false, true).map { on =>
      if (on) LambdaPruning.install(spark) else LambdaPruning.uninstall(spark)
      val df = build()
      (Common.readLeaves(df), digest(df))
    }
    LambdaPruning.uninstall(spark)
    val refCell = ref.map(r => digest(r())).getOrElse("-")
    println(s"| $label | ${cells(0)._1} | ${cells(1)._1} | " +
      s"${if (cells(0)._2 == cells(1)._2) "yes" else "**NO**"} | ${cells(0)._2} | " +
      s"${if (ref.isEmpty) "-" else if (refCell == cells(0)._2) "= reference" else "**differs: " + refCell + "**"} |")
  }

  def main(args: Array[String]): Unit = {
    val data = args(0)
    spark = Common.session()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val ptc = PathlingContext.create(spark)
    def ds(name: String) = ptc.read().parquet(data + "/" + name)
    val patX = ds("pat_x").read("Patient")
    val patNone = ds("pat_none").read("Patient")
    val obsNone = ds("obs_no_vq").read("Observation")
    val obsEng = ds("obs_engine").read("Observation")
    println("## B1: the engine over datasets with the root column absent")
    println("| Case | Read without rule | Read with rule | Rule transparent | Result | Against the dataset with the column |")
    println("| --- | --- | --- | --- | --- | --- |")
    def pathCol(r: String, e: String) = ptc.fhirPathToColumn(r, e)
    for ((label, e, f) <- Seq(
      ("`Patient.birthDate`", "birthDate", false),
      ("`Patient.birthDate > @2000`", "birthDate > @2000", true))) {
      def q(d: DataFrame) = if (f) d.filter(pathCol("Patient", e)).select("id")
      else d.select(pathCol("Patient", e).as("r"))
      both(s"no `_birthDate`: $label", Some(() => q(patX)))(() => q(patNone))
    }
    for ((label, e, f) <- Seq(
      ("`value.ofType(Quantity)`", "value.ofType(Quantity)", false),
      ("`value.ofType(Quantity) > 5 'mg'`", "value.ofType(Quantity) > 5 'mg'", true))) {
      def q(d: DataFrame) = if (f) d.filter(pathCol("Observation", e)).select("id")
      else d.select(col("id"), pathCol("Observation", e).as("r"))
      both(s"no `valueQuantity`: $label")(() => q(obsNone))
      both(s"with `valueQuantity` (control): $label")(() => q(obsEng))
    }

    println("\n## B1: resolve_or_none over columnOrNull(\"_birthDate\", NullType)")
    val computedStart = to_timestamp(col("birthDate"))
    val computedEnd = timestamp_micros(unix_micros(to_timestamp(col("birthDate"))) + 86399999999L)
    val tolerant = ColumnFunctions.columnOrNull("_birthDate", NullType)
    val ann = Proto.resolveOrNone(tolerant, Seq("__start", "__end"))
    for ((name, d) <- Seq("with `_birthDate`" -> patX, "without it" -> patNone)) {
      val r = d.select(col("id"), ann.as("ann"),
        Proto.resolveOrElse(ann, "__start", computedStart).as("start_by_none"),
        ColumnFunctions.resolveOrNull(tolerant, "__start", TimestampType).as("start_by_null"),
        computedStart.as("computed_start")).limit(3)
      println(s"$name: ann type ${r.schema("ann").dataType.simpleString}")
      r.show(false)
      println("rows where the accessor over resolve_or_none differs from the computation: " +
        d.filter(!Proto.resolveOrElse(ann, "__start", computedStart).eqNullSafe(computedStart)).count())
      println("rows where the resolve_or_null form differs from the computation: " +
        d.filter(!ColumnFunctions.resolveOrNull(tolerant, "__start", TimestampType)
          .eqNullSafe(computedStart)).count())
    }

    println("\n## B2: flat parent-level layout, `birthDate >= @2000` over stored or computed bounds")
    val ts = lit(java.time.Instant.parse("2000-01-01T00:00:00Z"))
    val tsEnd = lit(java.time.Instant.parse("2000-12-31T23:59:59.999999Z"))
    // The comparison as TemporalComparator shapes it: empty where the bounds cannot decide.
    def compare(start: Column, end: Column): Column = {
      val canCompare = end.lt(ts).or(tsEnd.lt(start))
        .or(ts.equalTo(start).and(end.equalTo(tsEnd)))
      when(canCompare, start.geq(ts))
    }
    val reference = patX.filter(compare(col("_birthDate.__start"), col("_birthDate.__end"))).select("id")
    println("reference (stored `_birthDate.__start`, `__end`): " + digest(reference))
    println("| Case | Read without rule | Read with rule | Rule transparent | Result | Against the reference |")
    println("| --- | --- | --- | --- | --- | --- |")
    type Mk = (String, Column) => Column
    val direct: Mk = (n, c) => Proto.columnOrElseDirect(n, c)
    val composed: Mk = (n, c) => Proto.columnOrElse(n, c)
    val variants = Seq("pat_flat_both" -> "both columns", "pat_flat_start" -> "`__birthDate_end` absent",
      "pat_none" -> "both absent")
    for ((mkName, mk) <- Seq("direct" -> direct, "composed" -> composed); (dir, vname) <- variants) {
      val d = ds(dir).read("Patient")
      both(s"$vname, columnOrElse ($mkName)", Some(() => reference))(() =>
        d.filter(compare(mk("__birthDate_start", computedStart), mk("__birthDate_end", computedEnd)))
          .select("id"))
    }
    // The set form: a structure of whichever of the columns exist, read by the accessors.
    val set = Proto.columnsStruct(Seq("__start" -> "__birthDate_start", "__end" -> "__birthDate_end"))
    for ((dir, vname) <- variants) {
      val d = ds(dir).read("Patient")
      println(s"set form over $vname: struct type " +
        d.select(set.as("s")).schema("s").dataType.simpleString)
      both(s"$vname, set form + accessors", Some(() => reference))(() =>
        d.filter(compare(Proto.resolveOrElse(set, "__start", computedStart),
          Proto.resolveOrElse(set, "__end", computedEnd))).select("id"))
    }

    println("\n## B2: a null fallback would be wrong (`__birthDate_end` absent)")
    val d = ds("pat_flat_start").read("Patient")
    val nullEnd = ColumnFunctions.columnOrNull("__birthDate_end", TimestampType)
    val orElseEnd = Proto.columnOrElse("__birthDate_end", computedEnd)
    val refEnd = patX.filter(col("_birthDate.__end") >= ts).count()
    for ((label, e) <- Seq("columnOrNull (null fallback)" -> nullEnd, "columnOrElse (computed)" -> orElseEnd)) {
      val q = d.filter(compare(col("__birthDate_start"), e)).select("id")
      println(s"$label: `birthDate >= @2000` as a filter matches ${q.count()} (reference ${reference.count()})")
      println(s"$label: `end >= @2000` (the ge-search shape) matches ${d.filter(e >= ts).count()} (reference $refEnd)")
    }
    d.filter(col("birthDate").startsWith("1990-05")).limit(1)
      .select(col("id"), col("birthDate"), col("__birthDate_start").as("stored_start"),
        nullEnd.as("end_null_fallback"), orElseEnd.as("end_or_else"),
        compare(col("__birthDate_start"), nullEnd).as("birthDate>=@2000 null fallback"),
        compare(col("__birthDate_start"), orElseEnd).as("birthDate>=@2000 columnOrElse"),
        (nullEnd >= ts).as("end>=@2000 null fallback"), (orElseEnd >= ts).as("end>=@2000 columnOrElse"))
      .show(false)
    spark.stop()
  }
}
