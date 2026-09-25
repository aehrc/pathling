/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2026 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package au.csiro.pathling.sql

import au.csiro.pathling.encoders.ColumnFunctions.{columnOrNull, resolveOrNull, traverseExtension, traverseRootExtension}
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, GetStructField}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, functions => F}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
 * Tests for the tolerant traversal expression, the tolerant table-column reference and extension
 * traversal (T038a to T038d).
 *
 * Every column under test is built with no reference to any dataset, as the engine builds them,
 * and then applied to data read back from Parquet. Where the same column is applied to data that
 * has the referenced element and to data that lacks it, the present case is the positive control:
 * a replacement forced before its child resolved would either throw or cache the fallback, and the
 * present case would then answer wrongly.
 */
class ResolveOrNullTest extends SparkSessionSupport with AdaptiveSparkPlanHelper {

  private val ExtensionMapType = MapType(IntegerType, ArrayType(StringType))

  // A table with the columns the tolerant reference is asked for.
  private def present: DataFrame = parquet("present",
    """select 1 as id, 5 as score,
      |  array(named_struct('_fid', 10, 'v', 'a'), named_struct('_fid', 11, 'v', 'b')) as arr,
      |  map(10, array('urn:x')) as _extension
      |union all
      |select 2, 7, array(named_struct('_fid', 20, 'v', 'c')),
      |  cast(null as map<int, array<string>>)""".stripMargin)

  // The same table without the columns the tolerant reference is asked for.
  private def absent: DataFrame = parquet("absent",
    """select 1 as id, array(named_struct('_fid', 10, 'v', 'a'), named_struct('_fid', 11, 'v', 'b'))
      |  as arr
      |union all
      |select 2, array(named_struct('_fid', 20, 'v', 'c'))""".stripMargin)

  private def other: DataFrame = parquet("other", "select 0 as k")

  private def extensionMap: Column = columnOrNull("_extension", ExtensionMapType)

  private def score: Column = columnOrNull("score", IntegerType)

  // -----------------------------------------------------------------------------------------------
  // T038a, decision 75: the tolerant table-column reference.
  // -----------------------------------------------------------------------------------------------

  @Test
  def tableColumnInProject(): Unit = {
    assertEquals(Seq("[1,Map(10 -> ArraySeq(urn:x))]", "[2,null]"),
      rows(present.select(F.col("id"), extensionMap)))
    val fallback = absent.select(F.col("id"), extensionMap.alias("m"))
    assertEquals(Seq("[1,null]", "[2,null]"), rows(fallback))
    assertEquals(ExtensionMapType, fallback.schema("m").dataType)
  }

  @Test
  def tableColumnInFilter(): Unit = {
    assertEquals(Seq("[1]"), rows(present.filter(extensionMap.isNotNull).select("id")))
    assertEquals(Seq(), rows(absent.filter(extensionMap.isNotNull).select("id")))
  }

  @Test
  def tableColumnAsGroupingKey(): Unit = {
    assertEquals(Seq("[5,1]", "[7,1]"), rows(present.groupBy(score.alias("g")).count()))
    assertEquals(Seq("[null,2]"), rows(absent.groupBy(score.alias("g")).count()))
  }

  @Test
  def tableColumnAsAggregateValue(): Unit = {
    assertEquals(Seq("[12]"), rows(present.agg(F.sum(score))))
    assertEquals(Seq("[null]"), rows(absent.agg(F.sum(score))))
    assertEquals(Seq("[1,5]", "[2,7]"), rows(present.groupBy(F.col("id")).agg(F.first(score))))
  }

  @Test
  def tableColumnInSort(): Unit = {
    assertEquals(Seq("2", "1"), orderedRows(present.orderBy(score.desc).select("id")).map(strip))
    assertEquals(2, absent.orderBy(score.desc).collect().length)
  }

  @Test
  def tableColumnInGenerator(): Unit = {
    assertEquals(Seq("[1,10,ArraySeq(urn:x)]", "[2,null,null]"),
      rows(present.select(F.col("id"), F.explode_outer(extensionMap))))
    assertEquals(Seq("[1,null,null]", "[2,null,null]"),
      rows(absent.select(F.col("id"), F.explode_outer(extensionMap))))
  }

  @Test
  def tableColumnInsideLambda(): Unit = {
    val lookup = F.transform(F.col("arr"), x => extensionMap.getItem(x.getField("_fid")))
    assertEquals(Seq("[1,ArraySeq(ArraySeq(urn:x), null)]", "[2,ArraySeq(null)]"),
      rows(present.select(F.col("id"), lookup)))
    assertEquals(Seq("[1,ArraySeq(null, null)]", "[2,ArraySeq(null)]"),
      rows(absent.select(F.col("id"), lookup)))
    // The same lookup inside a lambda under a filter.
    val found = F.exists(F.col("arr"), x => extensionMap.getItem(x.getField("_fid")).isNotNull)
    assertEquals(Seq("[1]"), rows(present.filter(found).select("id")))
    assertEquals(Seq(), rows(absent.filter(found).select("id")))
  }

  @Test
  def tableColumnPlanReadsTheColumnOrNothing(): Unit = {
    // Where the column is present the optimised plan reads it directly, and where it is absent the
    // fallback folds away and nothing extra is read.
    val presentPlan = present.select(extensionMap).queryExecution.optimizedPlan.toString
    assertTrue(presentPlan.contains("_extension#"), presentPlan)
    val absentPlan = absent.select(extensionMap).queryExecution.optimizedPlan.toString
    assertFalse(absentPlan.contains("_extension#"), absentPlan)
  }

  /**
   * Known limit, recorded in `evidence/t038a-root-column-spike.md` and decision 75: the reference
   * fails in a join condition, even where the column exists. The analyzer resolves the view column
   * only against an operator with a single child, and asserts that it has one
   * (`ColumnResolutionHelper.resolveExpressionByPlanChildren`), so a join fails with an
   * `AssertionError` that the catch does not see. This test notices if that changes.
   */
  @Test
  def knownLimitJoinConditionFailsWithInternalError(): Unit = {
    for (data <- Seq(present, absent)) {
      val error = assertThrows(classOf[SparkException],
        () => data.join(other, score >= other("k")).collect())
      assertEquals("INTERNAL_ERROR", error.getCondition)
      assertTrue(causes(error).exists(_.isInstanceOf[AssertionError]), error.toString)
    }
  }

  /**
   * Known limit, recorded in `evidence/t038a-root-column-spike.md` and decision 75: an ambiguous
   * name raises the same error as an absent one, so after a self-join the reference resolves to
   * the fallback, silently, where a plain reference fails with AMBIGUOUS_REFERENCE. This test
   * notices if that changes.
   */
  @Test
  def knownLimitAmbiguousNameGivesSilentNull(): Unit = {
    val selfJoin = present.as("l").join(present.as("r"), F.col("l.id") === F.col("r.id"))
    assertEquals(Seq("[null]", "[null]"), rows(selfJoin.select(extensionMap)))
    val control = assertThrows(classOf[AnalysisException],
      () => selfJoin.select(F.col("_extension")).collect())
    assertEquals("AMBIGUOUS_REFERENCE", control.getCondition)
  }

  /**
   * A further limit, found while writing these tests and not recorded in the spike: a column that
   * the table has, but that a projection beneath the operator has dropped, is treated as absent.
   * A plain reference is resolved there by `ResolveMissingReferences`, which adds the column back
   * to the projection. The tolerant reference is resolved first against the operator's child, whose
   * output lacks the name, so the catch answers the fallback before that rule can run. The answer
   * is silently wrong rather than an error. The engine applies its columns to the table itself,
   * where this does not arise, but a caller of the public API could. This test notices if that
   * changes.
   */
  @Test
  def knownLimitColumnDroppedByProjectionBeneathIsTreatedAsAbsent(): Unit = {
    assertEquals(Seq("[2]", "[1]"),
      orderedRows(present.select("id").orderBy(F.col("score").desc)))
    assertEquals(Seq("[2]"), rows(present.select("id").filter(F.col("score") === 7)))
    // The tolerant reference sorts by a null and filters everything out.
    assertEquals(Seq(), rows(present.select("id").filter(score === 7)))
    val plan = present.select("id").orderBy(score.desc).queryExecution.analyzed.toString
    assertTrue(plan.contains("Sort [null DESC NULLS LAST]"), plan)
  }

  // -----------------------------------------------------------------------------------------------
  // T038a: the tolerant traversal expression over an unresolved child.
  // -----------------------------------------------------------------------------------------------

  private def structs: DataFrame = parquet("structs",
    """select 1 as id, named_struct('a', 'x', 'b', 'y') as s,
      |  array(named_struct('a', 'p', 'b', 'q'), named_struct('a', 'r', 'b', 's')) as arr
      |union all
      |select 2, named_struct('a', 'z', 'b', 'w'), array(named_struct('a', 't', 'b', 'u'))"""
      .stripMargin)

  private def narrowStructs: DataFrame = parquet("narrowStructs",
    """select 1 as id, named_struct('a', 'x') as s,
      |  array(named_struct('a', 'p'), named_struct('a', 'r')) as arr
      |union all
      |select 2, named_struct('a', 'z'), array(named_struct('a', 't'))""".stripMargin)

  private def sb: Column = resolveOrNull(F.col("s"), "b", StringType)

  @Test
  def childIsUnresolvedAtConstruction(): Unit = {
    // The positive control T009a names: were the child bound to a dataset before the analyzer ran,
    // every other test here would pass without testing anything.
    val child = expression(sb).asInstanceOf[ResolveOrNull].child
    assertFalse(child.isInstanceOf[AttributeReference], child.getClass.getName)
    val catalystChild = ResolveOrNull(UnresolvedAttribute("s"), "b", StringType)
    assertFalse(catalystChild.child.resolved)
    assertEquals(Seq("[1,y]", "[2,w]"),
      rows(structs.select(F.col("id"), column(catalystChild))))
  }

  @Test
  def survivesAnalysisInProjectFilterGroupingAndSort(): Unit = {
    assertEquals(Seq("[1,y]", "[2,w]"), rows(structs.select(F.col("id"), sb)))
    assertEquals(Seq("[1]"), rows(structs.filter(sb === "y").select("id")))
    assertEquals(Seq("[w,1]", "[y,1]"), rows(structs.groupBy(sb.alias("g")).count()))
    assertEquals(Seq("1", "2"), orderedRows(structs.orderBy(sb.desc).select("id")).map(strip))
    // The same column over data lacking the field.
    assertEquals(Seq("[1,null]", "[2,null]"), rows(narrowStructs.select(F.col("id"), sb)))
    assertEquals(Seq(), rows(narrowStructs.filter(sb === "y").select("id")))
    assertEquals(Seq("[null,2]"), rows(narrowStructs.groupBy(sb.alias("g")).count()))
    assertEquals(2, narrowStructs.orderBy(sb.desc).collect().length)
  }

  @Test
  def analyzedPlanKeepsTheExpressionAndOptimisedPlanReplacesIt(): Unit = {
    // The analyzed plan still carries the node over a resolved child, which shows the replacement
    // was not forced during analysis; the optimised plan has the direct access in its place.
    val qe = structs.select(sb).queryExecution
    assertTrue(qe.analyzed.toString.contains("resolve_or_null(s#"), qe.analyzed.toString)
    val optimised = qe.optimizedPlan.toString
    assertFalse(optimised.contains("resolve_or_null(s#"), optimised)
    assertTrue(optimised.matches("(?s).*s#\\d+\\.b.*"), optimised)
  }

  @Test
  def survivesAnalysisOverLambdaVariable(): Unit = {
    // The latest-resolving child the engine produces: a lambda variable inside `transform`.
    val values = F.transform(F.col("arr"), x => resolveOrNull(x, "b", StringType))
    assertEquals(Seq("[1,ArraySeq(q, s)]", "[2,ArraySeq(u)]"),
      rows(structs.select(F.col("id"), values)))
    assertEquals(Seq("[1,ArraySeq(null, null)]", "[2,ArraySeq(null)]"),
      rows(narrowStructs.select(F.col("id"), values)))
    // A lambda under a filter and in a grouping key.
    val hasS = F.exists(F.col("arr"), x => resolveOrNull(x, "b", StringType) === "s")
    assertEquals(Seq("[1]"), rows(structs.filter(hasS).select("id")))
    assertEquals(Seq("[ArraySeq(q, s),1]", "[ArraySeq(u),1]"),
      rows(structs.groupBy(values.alias("g")).count()))
  }

  @Test
  def nestedOverPresentAndAbsent(): Unit = {
    // Emitted at every traversal step, so one over another must work, including over an inner that
    // fell back to the bottom type, which is not a structure at all.
    val data = parquet("nested",
      "select 1 as id, named_struct('inner', named_struct('leaf', 'v')) as s")
    val present = resolveOrNull(resolveOrNull(F.col("s"), "inner", NullType), "leaf", StringType)
    assertEquals(Seq("[v]"), rows(data.select(present)))
    val absentInner =
      resolveOrNull(resolveOrNull(F.col("s"), "missing", NullType), "leaf", StringType)
    assertEquals(Seq("[null]"), rows(data.select(absentInner)))
    val absentRepeatingInner = resolveOrNull(
      resolveOrNull(F.col("s"), "missing", ArrayType(NullType)), "leaf", ArrayType(StringType))
    assertEquals(Seq("[null]"), rows(data.select(absentRepeatingInner)))
  }

  // -----------------------------------------------------------------------------------------------
  // T038a, decision 75: extension traversal over the parent, on both layouts.
  // -----------------------------------------------------------------------------------------------

  // The previous layout: every complex element carries `_fid`, and the extensions of each element
  // are the entry for its `_fid` in the `_extension` table column. The nested extension (_fid 4)
  // has one extension of its own (_fid 5).
  private def previousLayout: DataFrame = parquet("previousLayout",
    """select 'r1' as id, 1 as _fid,
      |  array(named_struct('family', 'F1', '_fid', 2), named_struct('family', 'F2', '_fid', 3))
      |    as name,
      |  map(1, array(named_struct('url', 'urn:a', 'valueString', 'A', '_fid', 4)),
      |      4, array(named_struct('url', 'urn:a1', 'valueString', 'A1', '_fid', 5)),
      |      2, array(named_struct('url', 'urn:n1', 'valueString', 'N1', '_fid', 6))) as _extension
      |union all
      |select 'r2', 1, array(named_struct('family', 'G', '_fid', 2)),
      |  cast(map() as map<int, array<struct<url: string, valueString: string, _fid: int>>>)"""
      .stripMargin)

  // The same resources in the new layout: extensions are inline, and there is no `_fid` and no
  // `_extension`. The nested extension has no extensions, so its type has no `extension` field.
  private def newLayout: DataFrame = parquet("newLayout",
    """select 'r1' as id,
      |  array(named_struct('url', 'urn:a', 'valueString', 'A',
      |    'extension', array(named_struct('url', 'urn:a1', 'valueString', 'A1')))) as extension,
      |  array(named_struct('family', 'F1',
      |      'extension', array(named_struct('url', 'urn:n1', 'valueString', 'N1'))),
      |    named_struct('family', 'F2', 'extension',
      |      cast(null as array<struct<url: string, valueString: string>>))) as name
      |union all
      |select 'r2', null, array(named_struct('family', 'G', 'extension', null))""".stripMargin)

  // A pruned new-layout table whose resources have no extensions at all, so that neither the
  // `extension` column, nor an `extension` field, nor `_fid`, nor `_extension` exists.
  private def prunedLayout: DataFrame = parquet("prunedLayout",
    """select 'r1' as id, array(named_struct('family', 'F1'), named_struct('family', 'F2')) as name
      |union all
      |select 'r2', array(named_struct('family', 'G'))""".stripMargin)

  private def bothLayouts: Seq[DataFrame] = Seq(previousLayout, newLayout)

  private def urls(extensions: Column): Column =
    resolveOrNull(extensions, "url", ArrayType(StringType))

  private def rootUrls: Column = urls(traverseRootExtension())

  @Test
  def rootExtensionOnBothLayouts(): Unit = {
    for (data <- bothLayouts) {
      assertEquals(Seq("[r1,ArraySeq(urn:a)]", "[r2,null]"),
        rows(data.select(F.col("id"), rootUrls)))
    }
    // The previous layout's plan is the map lookup, and the new layout's the column itself.
    val previousPlan = previousLayout.select(rootUrls).queryExecution.optimizedPlan.toString
    assertTrue(previousPlan.contains("_extension#"), previousPlan)
    val newPlan = newLayout.select(rootUrls).queryExecution.optimizedPlan.toString
    assertFalse(newPlan.contains("_extension"), newPlan)
  }

  @Test
  def rootExtensionWithNeitherColumn(): Unit = {
    val result = prunedLayout.select(F.col("id"), traverseRootExtension().alias("e"))
    assertEquals(Seq("[r1,null]", "[r2,null]"), rows(result))
    assertEquals(ExtensionTraversal.ABSENT_TYPE, result.schema("e").dataType)
  }

  @Test
  def rootExtensionInEveryPlanShape(): Unit = {
    // The shapes decision 75 requires the root to work in: select, filter, grouping key, aggregate
    // value, sort, and inside a lambda. Each is checked on both layouts, which must agree.
    val hasA = F.array_contains(rootUrls, "urn:a")
    for (data <- bothLayouts) {
      assertEquals(Seq("[r1]"), rows(data.filter(hasA).select("id")))
      assertEquals(Seq("[ArraySeq(urn:a),1]", "[null,1]"),
        rows(data.groupBy(rootUrls.alias("g")).count()))
      assertEquals(Seq("[r1,ArraySeq(urn:a)]", "[r2,null]"),
        rows(data.groupBy(F.col("id")).agg(F.first(rootUrls))))
      assertEquals(Seq("[r1]", "[r2]"),
        orderedRows(data.orderBy(F.coalesce(F.size(rootUrls), F.lit(0)).desc).select("id")))
      val inLambda = F.transform(F.col("name"), n => F.struct(n.getField("family"), rootUrls))
      assertEquals(
        Seq("[r1,ArraySeq([F1,ArraySeq(urn:a)], [F2,ArraySeq(urn:a)])]",
          "[r2,ArraySeq([G,null])]"),
        rows(data.select(F.col("id"), inLambda)))
    }
    // The pruned table, where neither column exists, in the same shapes.
    assertEquals(Seq(), rows(prunedLayout.filter(hasA).select("id")))
    assertEquals(Seq("[null,2]"), rows(prunedLayout.groupBy(rootUrls.alias("g")).count()))
    assertEquals(2, prunedLayout.orderBy(F.size(rootUrls)).collect().length)
  }

  @Test
  def repeatingParentOnBothLayouts(): Unit = {
    val nameUrls = F.transform(traverseExtension(F.col("name")), e => urls(e))
    for (data <- bothLayouts) {
      assertEquals(Seq("[r1,ArraySeq(ArraySeq(urn:n1), null)]", "[r2,ArraySeq(null)]"),
        rows(data.select(F.col("id"), nameUrls)))
    }
    val result = prunedLayout.select(traverseExtension(F.col("name")).alias("e"))
    assertEquals(Seq("[null]", "[null]"), rows(result))
    assertEquals(ExtensionTraversal.ABSENT_TYPE, result.schema("e").dataType)
  }

  @Test
  def singularParentInsideLambdaOnBothLayouts(): Unit = {
    val perName = F.transform(F.col("name"), n => urls(traverseExtension(n)))
    val withExtension = F.exists(F.col("name"), n => F.size(traverseExtension(n)) > 0)
    for (data <- bothLayouts) {
      assertEquals(Seq("[r1,ArraySeq(ArraySeq(urn:n1), null)]", "[r2,ArraySeq(null)]"),
        rows(data.select(F.col("id"), perName)))
      // Under a filter and in a grouping key, where the parent and `_extension` resolve on
      // different passes.
      assertEquals(Seq("[r1]"), rows(data.filter(withExtension).select("id")))
      assertEquals(Seq("[ArraySeq(ArraySeq(urn:n1), null),1]", "[ArraySeq(null),1]"),
        rows(data.groupBy(perName.alias("g")).count()))
      assertEquals(Seq("[r1,ArraySeq(ArraySeq(urn:n1), null)]", "[r2,ArraySeq(null)]"),
        rows(data.groupBy(F.col("id")).agg(F.first(perName))))
    }
    assertEquals(Seq("[r1,ArraySeq(null, null)]", "[r2,ArraySeq(null)]"),
      rows(prunedLayout.select(F.col("id"), perName)))
    assertEquals(Seq(), rows(prunedLayout.filter(withExtension).select("id")))
  }

  @Test
  def extensionOfExtensionOnBothLayouts(): Unit = {
    val level1 = traverseRootExtension()
    val level2 = traverseExtension(level1)
    val level3 = traverseExtension(F.flatten(level2))
    for (data <- bothLayouts) {
      assertEquals(Seq("[r1,ArraySeq(ArraySeq(urn:a1))]", "[r2,null]"),
        rows(data.select(F.col("id"), F.transform(level2, e => urls(e)))))
      // The same step inside a lambda.
      val inLambda = F.transform(level1, e => urls(traverseExtension(e)))
      assertEquals(Seq("[r1,ArraySeq(ArraySeq(urn:a1))]", "[r2,null]"),
        rows(data.select(F.col("id"), inLambda)))
    }
    // The nested extension has none of its own. On the new layout its type has neither `extension`
    // nor `_fid`, so the whole step is null. On the previous layout its `_fid` has no entry, so the
    // step is an array holding one null. Both are the empty collection once nulls are removed, as
    // the engine removes them after every traversal step.
    assertEquals(Seq("[r1,null]", "[r2,null]"),
      rows(newLayout.select(F.col("id"), level3)))
    assertEquals(Seq("[r1,ArraySeq(null)]", "[r2,null]"),
      rows(previousLayout.select(F.col("id"), level3)))
  }

  @Test
  def previousLayoutWithoutExtensionMapFailsAsToday(): Unit = {
    // Data encoded with extensions disabled has `_fid` but no `_extension`, and extension
    // traversal fails there as it does today, rather than answering empty.
    val data = parquet("noExtensionMap",
      "select 'r1' as id, 1 as _fid, array(named_struct('family', 'F1', '_fid', 2)) as name")
    for (traversal <- Seq(traverseRootExtension(), traverseExtension(F.col("name")))) {
      val error = assertThrows(classOf[AnalysisException], () => data.select(traversal).collect())
      assertTrue(error.getCondition.startsWith("UNRESOLVED_COLUMN"), error.getCondition)
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Helpers.
  // -----------------------------------------------------------------------------------------------

  private def strip(row: String): String = row.stripPrefix("[").stripSuffix("]")

  private def causes(t: Throwable): Seq[Throwable] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).toSeq
}
