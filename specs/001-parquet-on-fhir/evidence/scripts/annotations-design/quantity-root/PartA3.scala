import java.math.BigDecimal

import au.csiro.pathling.fhirpath.encoding.QuantityEncoding
import au.csiro.pathling.library.PathlingContext
import au.csiro.pathling.schema.annotation.kinds.QuantityCanonicalKind
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * A3: row-group skipping for a quantity comparison, on observations sorted by canonical value into
 * 284 (today's layout) and 296 (proposed layout) row groups. The measure is the rows the scan
 * emitted, with record-level filtering off. Args: data dir.
 */
object PartA3 {
  var spark: org.apache.spark.sql.SparkSession = _
  var total = 0L

  def run(label: String, df: DataFrame, cond: Column, expected: Long): Unit = {
    val q = df.filter(cond).select("id")
    val matched = q.count()
    val q2 = df.filter(cond).select("id")
    val scannedRows = Common.scanned(q2)
    println(f"| $label | ${Common.pushed(q2).take(150)} | $scannedRows%,d | $matched%,d ${
      if (matched == expected) "(= engine)" else s"(**engine: $expected**)"} |")
  }

  def main(args: Array[String]): Unit = {
    val data = args(0)
    spark = Common.session(pushdown = true)
    val ptc = PathlingContext.create(spark)
    val cmp = ptc.fhirPathToColumn("Observation", "value.ofType(Quantity) > 195 'mg'")
    val Q = col("valueQuantity")
    val u = Q.getField("__canonical_exact").getField("value").getField("value")
    val sc = Q.getField("__canonical_exact").getField("value").getField("scale")
    println("### today's layout (obs_engine)")
    println(Common.footer(spark, data + "/obs_engine/Observation.parquet",
      "valueQuantity.__canonical_exact.value.value"))
    val eng = ptc.read().parquet(data + "/obs_engine").read("Observation")
    total = eng.count()
    val expected = eng.filter(cmp).count()
    println(s"rows $total, engine matches $expected")
    println("| Arm | PushedFilters | Rows scanned | Matches |")
    println("| --- | --- | --- | --- |")
    run("(i) engine: value.ofType(Quantity) > 195 'mg'", eng, cmp, expected)
    // 5 mg is 0.195 g, FlexiDecimal (5, 3); the stored values are at scales 0 to 6.
    run("(ii) plain: u > 195000 (195 mg at scale 6)", eng, u.gt(lit(new BigDecimal(195000))), expected)
    run("(ii) plain: u > 195 (the literal's unscaled value)", eng, u.gt(lit(new BigDecimal(195))), expected)
    def guard(k: Int): Column =
      sc.lt(k).or(u.gt(lit(new BigDecimal(195).multiply(BigDecimal.TEN.pow(k - 3)))))
    run("(ii) superset k=6: scale < 6 OR u > 195000", eng, guard(6), expected)
    val all = (3 to 6).map(guard).reduce(_.and(_))
    run("(ii) superset k=3..6 conjunction: scale<k OR u>195*10^(k-3)", eng, all, expected)
    run("(ii) engine AND superset k=3..6 (D11 pattern)", eng, cmp.and(all), expected)
    run("(ii) engine AND naive u > 195000 (unsound as a superset)", eng,
      cmp.and(u.gt(lit(new BigDecimal(195000)))), expected)
    println("per-scale counts of the rows the engine matches, and of those with u > 195000:")
    eng.filter(cmp).groupBy(sc.as("scale")).agg(count(lit(1)).as("matched"),
      sum(when(u.gt(lit(new BigDecimal(195000))), 1).otherwise(0)).as("u_gt_5000")).orderBy("scale").show()
        println("### proposed layout (obs_flat)")
    println(Common.footer(spark, data + "/obs_flat/Observation.parquet",
      "valueQuantity.__canonical_value_exact.value"))
    println(Common.footer(spark, data + "/obs_flat/Observation.parquet",
      "valueQuantity.__canonical_value"))
    val flat = ptc.read().parquet(data + "/obs_flat").read("Observation")
    val fu = Q.getField("__canonical_value_exact").getField("value")
    val fs = Q.getField("__canonical_value_exact").getField("scale")
    val fixed = Q.getField("__canonical_value")
    val lit5 = QuantityCanonicalKind.toExactLiteral(QuantityCanonicalKind.canonicalOf(
      new BigDecimal("195"), QuantityCanonicalKind.UCUM_SYSTEM, "mg"))
    val canon = struct(Q.getField("__canonical_value_exact").as("value"),
      Q.getField("__canonical_code").as("code"))
    val accessor = QuantityEncoding.compareCanonical(canon, lit5).gt(0)
    println("| Arm | PushedFilters | Rows scanned | Matches |")
    println("| --- | --- | --- | --- |")
    run("(i') accessor over flat fields: compareCanonical(...) > 0", flat, accessor, expected)
    run("(ii') plain: __canonical_value_exact.value > 5000", flat, fu.gt(lit(new BigDecimal(195000))), expected)
    run("(ii') superset k=3..6 on the flat leaves", flat,
      (3 to 6).map(k => fs.lt(k).or(fu.gt(lit(new BigDecimal(195).multiply(BigDecimal.TEN.pow(k - 3))))))
        .reduce(_.and(_)), expected)
    run("(ii') accessor AND superset k=3..6", flat, accessor.and(
      (3 to 6).map(k => fs.lt(k).or(fu.gt(lit(new BigDecimal(195).multiply(BigDecimal.TEN.pow(k - 3))))))
        .reduce(_.and(_))), expected)
    run("(iii') optional fixed scale: __canonical_value > 0.195", flat,
      fixed.gt(lit(new BigDecimal("0.195")).cast("decimal(38,12)")), expected)
    run("(iii') accessor AND __canonical_value >= 0.195 (D11 pattern)", flat,
      accessor.and(fixed.geq(lit(new BigDecimal("0.195")).cast("decimal(38,12)"))), expected)
    run("(iii') accessor AND code = 'g' AND __canonical_value >= 0.195", flat,
      accessor.and(Q.getField("__canonical_code").equalTo("g"))
        .and(fixed.geq(lit(new BigDecimal("0.195")).cast("decimal(38,12)"))), expected)

    // A small crafted table: what the leaves of a FlexiDecimal do under a plain predicate.
    println("### mixed scales, crafted: (u, scale) = value")
    val sp = spark
    import sp.implicits._
    val t = Seq((25, 2), (5, 1), (3, 0), (3000, 3), (7, 3), (4, 3)).toDF("u", "scale")
      .withColumn("value", col("u") / pow(lit(10), col("scale")))
    t.show()
    println("x > 0.195: " + t.filter(col("value") > 0.195).count() +
      " rows; plain u > 195 (the unscaled value of the bound): " + t.filter(col("u") > 195).count() + " rows")
    spark.stop()
  }
}
