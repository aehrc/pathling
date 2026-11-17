import java.math.BigDecimal

import au.csiro.pathling.fhirpath.encoding.QuantityEncoding
import au.csiro.pathling.library.PathlingContext
import au.csiro.pathling.schema.annotation.kinds.QuantityCanonicalKind
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spike.LambdaPruning

/**
 * Part A: a singular complex top-level column, Observation.valueQuantity. Args: data dir.
 * A1 output, A2 filter, A4 the same with the pruning rule; A3 pushdown is in [[PartA3]].
 */
object PartA {
  val Q = col("valueQuantity")

  def digest(df: DataFrame): String = {
    val r = df.select(struct(df.columns.map(c => col("`" + c + "`")): _*).as("s"))
      .agg(count(lit(1)), coalesce(sum(hash(col("s"))).cast("long"), lit(0L))).head()
    s"${r.getLong(0)} rows, hash ${r.getLong(1)}"
  }

  /** Runs the build without and with the rule, and prints a table row. */
  def both(spark: SparkSession, label: String)(build: () => DataFrame): Unit = {
    val cells = Seq(false, true).map { on =>
      if (on) LambdaPruning.install(spark) else LambdaPruning.uninstall(spark)
      val df = build()
      val read = Common.readLeaves(df, "valueQuantity.")
      val flags = s"ext=${Common.reads(df, "extension")}, __foreign=${Common.reads(df, "__foreign")}"
      (read, flags, digest(df))
    }
    LambdaPruning.uninstall(spark)
    val (off, on) = (cells(0), cells(1))
    println(s"| $label | ${off._1} | ${off._2} | ${on._1} | ${on._2} | " +
      s"${if (off._3 == on._3) "yes" else "**NO**"} (${off._3}) |")
  }

  def main(args: Array[String]): Unit = {
    val data = args(0)
    val spark = Common.session(pushdown = true)
    val ptc = PathlingContext.create(spark)
    println("| Case | Read without rule | Flags | Read with rule | Flags | Equal (result) |")
    println("| --- | --- | --- | --- | --- | --- |")
    val B = new BigDecimal("5")
    for (variant <- Seq("obs_engine", "obs_flat")) {
      val src = ptc.read().parquet(data + "/" + variant)
      def base = src.read("Observation")
      def view(where: String, columns: String) = src.view("Observation").json(
        """{"resourceType":"ViewDefinition","resource":"Observation","status":"active",""" +
          (if (where.isEmpty) "" else """"where":[{"path":"""" + where + """"}],""") +
          """"select":[{"column":[""" + columns + "]}]}").execute()
      val v = variant.stripPrefix("obs_")
      val whole = """{"path":"value.ofType(Quantity).code","name":"c"}"""
      // A1: the quantity through the engine. A view column must be primitive, so the whole
      // quantity is the select expression of fhirPathToColumn; the view reads one field of it.
      val wholeCol = ptc.fhirPathToColumn("Observation", "value.ofType(Quantity)")
      both(spark, s"A1 [$v] fhirPathToColumn: `value.ofType(Quantity)` whole")(() =>
        base.select(wholeCol.as("q")))
      both(spark, s"A1 [$v] view column: `value.ofType(Quantity).code`")(() => view("", whole))
      both(spark, s"A1 [$v] view columns: `.value` and `.code`")(() =>
        view("", whole + ""","""+"""{"path":"value.ofType(Quantity).value","name":"v"}"""))
      // A1 hand-built: every `__` field stripped by named_struct.
      val names = base.schema("valueQuantity").dataType.asInstanceOf[StructType].fieldNames.toSeq
      val keep = names.filterNot(_.startsWith("__"))
      val stripped = struct(keep.map(n => Q.getField(n).as(n)): _*)
      both(spark, s"A1 [$v] hand: named_struct of non-`__` fields")(() =>
        base.select(stripped.as("q")))
      both(spark, s"A1 [$v] hand: inline(array(named_struct))")(() =>
        base.select(inline(array(struct(stripped.as("q"))))))
      // The stripped struct taken over the engine's own whole-quantity output.
      both(spark, s"A1 [$v] hand over the engine output: named_struct of non-`__` fields of q")(() =>
        base.select(wholeCol.as("q")).select(
          struct(base.select(wholeCol.as("q")).schema("q").dataType.asInstanceOf[StructType]
            .fieldNames.toSeq.filterNot(_.startsWith("__")).map(n => col("q").getField(n).as(n)): _*).as("q")))
      // A2: a comparison in filter position.
      val cmp = ptc.fhirPathToColumn("Observation", "value.ofType(Quantity) > 5 'mg'")
      both(spark, s"A2 [$v] filter: `value.ofType(Quantity) > 5 'mg'`, select id")(() =>
        base.filter(cmp).select("id"))
      both(spark, s"A2 [$v] view where: `value.ofType(Quantity) > 5 'mg'`")(() =>
        view("value.ofType(Quantity) > 5 'mg'", """{"path":"id","name":"id"}"""))
      if (v == "engine") {
        println("<!-- optimised plan of the A1 view -->")
        println(view("", whole).queryExecution.optimizedPlan.toString.take(1500))
        println("<!-- optimised filter of A2 -->")
        println(Common.optimisedFilter(base.filter(cmp).select("id")).take(2500))
      }
    }
    // Hand-modelled proposed accessors over the flat fields: compare the canonical form built from
    // `__canonical_value_exact` and `__canonical_code`.
    val src = ptc.read().parquet(data + "/obs_flat")
    val base = src.read("Observation")
    val lit5 = QuantityCanonicalKind.toExactLiteral(
      QuantityCanonicalKind.canonicalOf(B, QuantityCanonicalKind.UCUM_SYSTEM, "mg"))
    val canon = struct(Q.getField("__canonical_value_exact").as("value"),
      Q.getField("__canonical_code").as("code"))
    both(spark, "A2 [flat] hand: compareCanonical(struct(flat fields), 5 mg) > 0")(() =>
      base.filter(QuantityEncoding.compareCanonical(canon, lit5).gt(0)).select("id"))
    spark.stop()
  }
}
