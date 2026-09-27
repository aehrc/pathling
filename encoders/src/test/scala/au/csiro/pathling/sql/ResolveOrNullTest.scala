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

import au.csiro.pathling.encoders.ColumnFunctions.{columnOrNull, decimalColumnOrNull, quantityColumnOrNull, resolveOrNull, traverseExtension, traverseRootExtension}
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, GetArrayStructFields, GetStructField, Literal}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, functions => F}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
 * Tests for the tolerant traversal expression, the tolerant table-column reference and extension
 * traversal (T038a to T038d), and for the traversal expression's normalisation of the previous
 * layout to the new one (T089a).
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
   * is silently wrong rather than an error. It reaches the root extension traversal too, which
   * goes through the same reference. The engine's present sites apply their columns to the table
   * itself, but T110 emits the reference at every root element, so it must establish whether any
   * engine site meets this; a caller of the public API certainly can. This test notices if that
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
  // T038b: a direct field reference where the input carries the field, and a typed null where not.
  // -----------------------------------------------------------------------------------------------

  @Test
  def presentFieldBecomesDirectFieldReference(): Unit = {
    val singular = structs.select(sb)
    assertEquals(Seq("[w]", "[y]"), rows(singular))
    assertTrue(optimisedExpressions(singular).exists {
      case field: GetStructField =>
        field.child.isInstanceOf[AttributeReference] && field.extractFieldName == "b"
      case _ => false
    }, singular.queryExecution.optimizedPlan.toString)

    val repeating = structs.select(resolveOrNull(F.col("arr"), "b", ArrayType(StringType)))
    assertEquals(Seq("[ArraySeq(q, s)]", "[ArraySeq(u)]"), rows(repeating))
    assertTrue(optimisedExpressions(repeating).exists {
      case GetArrayStructFields(_: AttributeReference, field, _, _, _) => field.name == "b"
      case _ => false
    }, repeating.queryExecution.optimizedPlan.toString)
    assertEquals(ArrayType(StringType), repeating.schema.head.dataType)
  }

  @Test
  def absentFieldBecomesNullOfDeclaredType(): Unit = {
    for ((fallback, data) <- Seq(
      StringType -> narrowStructs.select(sb),
      IntegerType -> narrowStructs.select(resolveOrNull(F.col("s"), "b", IntegerType)),
      ArrayType(StringType) ->
        narrowStructs.select(resolveOrNull(F.col("arr"), "b", ArrayType(StringType))))) {
      assertEquals(Seq("[null]", "[null]"), rows(data))
      assertEquals(fallback, data.schema.head.dataType)
      assertTrue(optimisedExpressions(data).exists {
        case Literal(null, dataType) => dataType == fallback
        case _ => false
      }, data.queryExecution.optimizedPlan.toString)
    }
  }

  // -----------------------------------------------------------------------------------------------
  // T038c: the fallback types of FR-055.
  // -----------------------------------------------------------------------------------------------

  private val SingularPrimitive: DataType = StringType
  private val RepeatingPrimitive: DataType = ArrayType(StringType)
  private val SingularComplex: DataType = NullType
  private val RepeatingComplex: DataType = ArrayType(NullType)

  private def missing(fallback: DataType): Column = resolveOrNull(F.col("s"), "missing", fallback)

  @Test
  def fallbackTypesFollowFr055(): Unit = {
    for (fallback <- Seq(SingularPrimitive, RepeatingPrimitive, SingularComplex, RepeatingComplex)) {
      val result = structs.select(missing(fallback).alias("m"))
      assertEquals(fallback, result.schema("m").dataType)
      assertEquals(Seq("[null]", "[null]"), rows(result))
    }
  }

  @Test
  def repeatingFallbackSurvivesTransform(): Unit = {
    // The engine wraps every repeating element in `transform`, which needs an array type.
    for (fallback <- Seq(RepeatingPrimitive, RepeatingComplex)) {
      assertEquals(Seq("[null]", "[null]"),
        rows(structs.select(F.transform(missing(fallback), x => x))))
    }
    // Bare `void` is not an array, so it does not survive.
    val error = assertThrows(classOf[AnalysisException],
      () => structs.select(F.transform(missing(SingularComplex), x => x)).collect())
    assertTrue(error.getCondition.startsWith("DATATYPE_MISMATCH"), error.getCondition)
  }

  @Test
  def complexFallbackCombinesWithPopulatedStructure(): Unit = {
    // The bottom type widens to the populated structure, singular and repeating.
    val singular = structs.select(F.coalesce(missing(SingularComplex), F.col("s")).alias("c"))
    assertEquals(Seq("[[x,y]]", "[[z,w]]"), rows(singular))
    assertEquals(structs.schema("s").dataType, singular.schema("c").dataType)
    val repeating = structs.select(F.concat(missing(RepeatingComplex), F.col("arr")).alias("c"))
    assertEquals(structs.schema("arr").dataType, repeating.schema("c").dataType)
    val combined = structs.select(
      F.concat(F.coalesce(missing(RepeatingComplex), F.array()), F.col("arr")).alias("c"))
    assertEquals(Seq("[ArraySeq([p,q], [r,s])]", "[ArraySeq([t,u])]"), rows(combined))

    // A concrete minimal structure does not combine with a wider one (FR-027).
    val minimal = StructType(Seq(StructField("a", StringType)))
    val singularError = assertThrows(classOf[AnalysisException],
      () => structs.select(F.coalesce(missing(minimal), F.col("s"))).collect())
    assertTrue(singularError.getCondition.startsWith("DATATYPE_MISMATCH"),
      singularError.getCondition)
    val repeatingError = assertThrows(classOf[AnalysisException],
      () => structs.select(F.concat(missing(ArrayType(minimal)), F.col("arr"))).collect())
    assertTrue(repeatingError.getCondition.startsWith("DATATYPE_MISMATCH"),
      repeatingError.getCondition)
  }

  // -----------------------------------------------------------------------------------------------
  // T038d: pruning is identical to a direct field reference (finding 14).
  // -----------------------------------------------------------------------------------------------

  @Test
  def prunesIdenticallyToDirectFieldReference(): Unit = {
    val pairs: Seq[(DataFrame, DataFrame)] = Seq(
      structs.select(sb) -> structs.select(F.col("s.b")),
      structs.select(resolveOrNull(F.col("arr"), "b", ArrayType(StringType))) ->
        structs.select(F.col("arr.b")),
      structs.filter(sb === "y").select(F.col("id")) ->
        structs.filter(F.col("s.b") === "y").select(F.col("id")),
      structs.select(F.transform(F.col("arr"), x => resolveOrNull(x, "b", StringType))) ->
        structs.select(F.transform(F.col("arr"), x => x.getField("b"))),
      // Over a structure lacking the field, the fallback reads nothing, as a literal would.
      narrowStructs.select(sb) -> narrowStructs.select(F.lit(null).cast(StringType)))
    for ((tolerant, direct) <- pairs) {
      assertEquals(readSchema(direct), readSchema(tolerant))
    }
    assertEquals("struct<s:struct<b:string>>", readSchema(structs.select(sb)))
    // The control: the comparison can fail, because a plan reading another field differs.
    assertNotEquals(readSchema(structs.select(F.col("s.a"))), readSchema(structs.select(sb)))
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
  // T089a, T094b: a previous-layout decimal is normalised to the new layout's text.
  // -----------------------------------------------------------------------------------------------

  // The previous layout: a decimal is a DECIMAL(32,6) value beside an integer `_scale` companion
  // holding the scale of the source, at the root, under a singular parent, under a repeating one,
  // and as a repeating decimal under a repeating parent. The source values are 1.50, 120, 0.1234567
  // (scale 7, rounded on encoding), 0.25, 1.00E+3 (scale -1) and 3. The second row holds a value
  // with no scale, as the result of arithmetic has, and a null value.
  private def previousDecimals: DataFrame = parquet("previousDecimals",
    """select 'r1' as id, cast(1.50 as decimal(32,6)) as factor, 2 as factor_scale,
      |  named_struct('latitude', cast(1.50 as decimal(32,6)), 'latitude_scale', 2) as position,
      |  array(named_struct('value', cast(120 as decimal(32,6)), 'value_scale', 0),
      |    named_struct('value', cast(0.1234567 as decimal(32,6)), 'value_scale', 7)) as component,
      |  array(named_struct(
      |    'sensitivity', array(cast(0.25 as decimal(32,6)), cast(1000 as decimal(32,6))),
      |    'sensitivity_scale', array(2, -1))) as roc
      |union all
      |select 'r2', cast(3 as decimal(32,6)), cast(null as int),
      |  named_struct('latitude', cast(null as decimal(32,6)), 'latitude_scale', cast(null as int)),
      |  array(named_struct('value', cast(2.5 as decimal(32,6)), 'value_scale', cast(null as int))),
      |  array(named_struct('sensitivity', array(cast(3 as decimal(32,6))),
      |    'sensitivity_scale', cast(null as array<int>)))""".stripMargin)

  // The same values in the new layout, stored as the text that a double round trip gives.
  private def newDecimals: DataFrame = parquet("newDecimals",
    """select 'r1' as id, '1.5' as factor, named_struct('latitude', '1.5') as position,
      |  array(named_struct('value', '120.0'), named_struct('value', '0.1234567')) as component,
      |  array(named_struct('sensitivity', array('0.25', '1000.0'))) as roc
      |union all
      |select 'r2', '3.0', named_struct('latitude', cast(null as string)),
      |  array(named_struct('value', '2.5')), array(named_struct('sensitivity', array('3.0')))"""
      .stripMargin)

  // A pruned new-layout table, which carries none of the decimals.
  private def prunedDecimals: DataFrame = parquet("prunedDecimals",
    """select 'r1' as id, named_struct('longitude', '2.0') as position,
      |  array(named_struct('code', 'c')) as component, array(named_struct('code', 'c')) as roc"""
      .stripMargin)

  private def decimalLayouts: Seq[DataFrame] = Seq(previousDecimals, newDecimals, prunedDecimals)

  private def factor: Column = decimalColumnOrNull("factor", StringType)

  private def latitude: Column = resolveOrNull(F.col("position"), "latitude", StringType)

  private def componentValue: Column =
    resolveOrNull(F.col("component"), "value", ArrayType(StringType))

  private def sensitivity: Column =
    resolveOrNull(F.col("roc"), "sensitivity", ArrayType(ArrayType(StringType)))

  private def componentValueInLambda: Column =
    F.transform(F.col("component"), c => resolveOrNull(c, "value", StringType))

  private def decimalTraversals: Seq[Column] =
    Seq(factor, latitude, componentValue, sensitivity, componentValueInLambda)

  @Test
  def decimalTraversalYieldsTheSameTypeOnEveryLayout(): Unit = {
    // The same-dataType rule: the previous layout, the new layout and the absent fallback agree, at
    // every cardinality and inside a lambda, and the type is the new layout's text.
    val expected = Seq(StringType, StringType, ArrayType(StringType), ArrayType(ArrayType(StringType)),
      ArrayType(StringType))
    for (data <- decimalLayouts) {
      val types = data.select(decimalTraversals: _*).schema.fields.map(_.dataType).toSeq
      assertEquals(expected, types.map(stripNullability))
    }
    val previous = previousDecimals.select(decimalTraversals: _*).schema
    val current = newDecimals.select(decimalTraversals: _*).schema
    assertEquals(current.fields.map(_.dataType).toSeq, previous.fields.map(_.dataType).toSeq)
  }

  @Test
  def previousLayoutDecimalCarriesTheSourceScale(): Unit = {
    // The text is the value at the source scale, which is capped at the value column's scale of 6,
    // and a negative scale gives an integer. A value with no scale is rendered at the scale of the
    // value column, and a null value stays null.
    assertEquals(
      Seq("[r1,1.50,1.50,ArraySeq(120, 0.123457),ArraySeq(ArraySeq(0.25, 1000))]",
        "[r2,3.000000,null,ArraySeq(2.500000),ArraySeq(ArraySeq(3.000000))]"),
      rows(previousDecimals.select(F.col("id"), factor, latitude, componentValue, sensitivity)))
    assertEquals(Seq("[r1,ArraySeq(120, 0.123457)]", "[r2,ArraySeq(2.500000)]"),
      rows(previousDecimals.select(F.col("id"), componentValueInLambda)))
  }

  @Test
  def newLayoutDecimalIsTheStoredText(): Unit = {
    assertEquals(
      Seq("[r1,1.5,1.5,ArraySeq(120.0, 0.1234567),ArraySeq(ArraySeq(0.25, 1000.0))]",
        "[r2,3.0,null,ArraySeq(2.5),ArraySeq(ArraySeq(3.0))]"),
      rows(newDecimals.select(F.col("id"), factor, latitude, componentValue, sensitivity)))
  }

  @Test
  def absentDecimalIsNullText(): Unit = {
    // A repeating decimal absent from every element of its parent falls back to a null array, as
    // any absent field does.
    assertEquals(Seq("[r1,null,null,null,null]"),
      rows(prunedDecimals.select(F.col("id"), factor, latitude, componentValue, sensitivity)))
  }

  @Test
  def decimalTextParsesBackToTheStoredValue(): Unit = {
    // The normalisation loses nothing the value column holds: cast back to the query-time type, the
    // text is the value that was stored.
    val decimalType = DecimalType(32, 6)
    val roundTrip = previousDecimals.select(
      (latitude.cast(decimalType) === F.col("position.latitude")).alias("singular"),
      F.forall(F.zip_with(componentValue, F.col("component.value"),
        (text, value) => text.cast(decimalType) === value), same => same).alias("repeating"))
    assertEquals(Seq("[null,true]", "[true,true]"), rows(roundTrip))
  }

  @Test
  def decimalNormalisationReadsOnlyTheValueAndItsScale(): Unit = {
    assertEquals("struct<position:struct<latitude:decimal(32,6),latitude_scale:int>>",
      readSchema(previousDecimals.select(latitude)))
    assertEquals("struct<component:array<struct<value:decimal(32,6),value_scale:int>>>",
      readSchema(previousDecimals.select(componentValue)))
    assertEquals("struct<factor:decimal(32,6),factor_scale:int>",
      readSchema(previousDecimals.select(factor)))
    assertEquals("struct<position:struct<latitude:string>>",
      readSchema(newDecimals.select(latitude)))
  }

  @Test
  def decimalWithoutScaleCompanionIsRenderedAtItsOwnScale(): Unit = {
    // A structure built by the engine, such as a quantity literal, may carry a decimal with no
    // companion. It is normalised all the same, so that the type does not depend on its origin.
    val data = parquet("noScale",
      "select named_struct('value', cast(1.5 as decimal(32,6))) as q")
    val value = data.select(resolveOrNull(F.col("q"), "value", StringType).alias("v"))
    assertEquals(StringType, value.schema("v").dataType)
    assertEquals(Seq("[1.500000]"), rows(value))
  }

  private def stripNullability(dataType: DataType): DataType = dataType match {
    case ArrayType(element, _) => ArrayType(stripNullability(element))
    case other => other
  }

  // -----------------------------------------------------------------------------------------------
  // T089a, T094b: a previous-layout quantity is normalised to the new layout's shape.
  // -----------------------------------------------------------------------------------------------

  private val canonicalType = "struct<value:decimal(38,0),scale:int>"

  private val previousQuantityType =
    "struct<id:string,value:decimal(32,6),value_scale:int,comparator:string,unit:string," +
      s"system:string,code:string,_value_canonicalized:$canonicalType," +
      "_code_canonicalized:string,_fid:int>"

  private def previousQuantity(value: String, scale: Int, code: String, canonical: String,
                               canonicalScale: Int, canonicalCode: String, fid: Int): String =
    s"""named_struct('id', cast(null as string), 'value', cast($value as decimal(32,6)),
       |  'value_scale', $scale, 'comparator', cast(null as string), 'unit', '$code',
       |  'system', 'http://unitsofmeasure.org', 'code', '$code',
       |  '_value_canonicalized', named_struct('value', cast($canonical as decimal(38,0)),
       |    'scale', $canonicalScale),
       |  '_code_canonicalized', '$canonicalCode', '_fid', $fid)""".stripMargin

  // The previous layout: a quantity carries the scale of its value and its canonical form, at the
  // root and under a repeating parent. The source values are 1.50 g and 500 mg, and the second row
  // holds a null quantity.
  private def previousQuantities: DataFrame = parquet("previousQuantities",
    s"""select 'r1' as id, ${previousQuantity("1.50", 2, "g", "15", 1, "g", 1)} as valueQuantity,
       |  array(named_struct('valueQuantity',
       |    ${previousQuantity("500", 0, "mg", "5", 1, "g", 2)})) as component
       |union all
       |select 'r2', cast(null as $previousQuantityType),
       |  array(named_struct('valueQuantity', cast(null as $previousQuantityType)))""".stripMargin)

  // The same values in a new-layout table, pruned to the elements they populate.
  private def newQuantities: DataFrame = parquet("newQuantities",
    """select 'r1' as id,
      |  named_struct('value', '1.5', 'unit', 'g', 'system', 'http://unitsofmeasure.org',
      |    'code', 'g') as valueQuantity,
      |  array(named_struct('valueQuantity', named_struct('value', '500', 'unit', 'mg',
      |    'system', 'http://unitsofmeasure.org', 'code', 'mg'))) as component
      |union all
      |select 'r2', cast(null as struct<value:string,unit:string,system:string,code:string>),
      |  array(named_struct('valueQuantity',
      |    cast(null as struct<value:string,unit:string,system:string,code:string>)))"""
      .stripMargin)

  // A pruned new-layout table, which carries no quantities.
  private def prunedQuantities: DataFrame = parquet("prunedQuantities",
    "select 'r1' as id, array(named_struct('code', 'c')) as component")

  private def quantityLayouts: Seq[DataFrame] =
    Seq(previousQuantities, newQuantities, prunedQuantities)

  private def rootQuantity: Column = quantityColumnOrNull("valueQuantity", NullType)

  private def componentQuantity: Column =
    resolveOrNull(F.col("component"), "valueQuantity", ArrayType(NullType))

  private def componentQuantityInLambda: Column =
    F.transform(F.col("component"), c => resolveOrNull(c, "valueQuantity", NullType))

  private def quantityLeaves(quantity: Column, repeating: Boolean): Seq[Column] = {
    val leafType = if (repeating) ArrayType(StringType) else StringType
    Seq("value", "unit", "system", "code").map(name => resolveOrNull(quantity, name, leafType))
  }

  private def quantityLeafTraversals: Seq[Column] =
    quantityLeaves(rootQuantity, repeating = false) ++
      quantityLeaves(componentQuantity, repeating = true) ++
      Seq(F.transform(F.col("component"),
        c => resolveOrNull(resolveOrNull(c, "valueQuantity", NullType), "value", StringType)))

  @Test
  def quantityLeavesHaveTheSameTypeOnEveryLayout(): Unit = {
    // The same-dataType rule, on every leaf reached through the quantity: the previous layout, the
    // new layout and the absent fallback agree, at both cardinalities and inside a lambda.
    val expected = Seq.fill(4)(StringType) ++ Seq.fill(5)(ArrayType(StringType))
    for (data <- quantityLayouts) {
      val types = data.select(quantityLeafTraversals: _*).schema.fields.map(_.dataType).toSeq
      assertEquals(expected, types.map(stripNullability))
    }
  }

  @Test
  def previousLayoutQuantityTakesTheNewLayoutsShape(): Unit = {
    // The canonical form and the scale companion are dropped, and the value is its text. The fields
    // the new layout also has take the new layout's types. The quantity keeps `_fid`, which the
    // next step needs to look up its extensions, so the structure as a whole cannot equal the new
    // layout's, just as the extension structure cannot (decisions 74 and 75).
    val normalised = StructType(Seq(
      StructField("id", StringType), StructField("value", StringType),
      StructField("comparator", StringType), StructField("unit", StringType),
      StructField("system", StringType), StructField("code", StringType),
      StructField("_fid", IntegerType)))
    val previous = previousQuantities.select(rootQuantity.alias("root"),
      componentQuantity.alias("component"), componentQuantityInLambda.alias("lambda")).schema
    assertEquals(normalised, stripStructNullability(previous("root").dataType))
    assertEquals(ArrayType(normalised), stripStructNullability(previous("component").dataType))
    assertEquals(ArrayType(normalised), stripStructNullability(previous("lambda").dataType))

    val current = stripStructNullability(
      newQuantities.select(rootQuantity.alias("root")).schema("root").dataType)
      .asInstanceOf[StructType]
    for (field <- current.fields) {
      assertEquals(field.dataType, normalised(field.name).dataType, field.name)
    }
  }

  @Test
  def previousLayoutQuantityCarriesTheSourceScale(): Unit = {
    // A null quantity stays null rather than becoming a structure of nulls.
    assertEquals(
      Seq("[r1,[null,1.50,null,g,http://unitsofmeasure.org,g,1]," +
        "ArraySeq([null,500,null,mg,http://unitsofmeasure.org,mg,2])]",
        "[r2,null,ArraySeq(null)]"),
      rows(previousQuantities.select(F.col("id"), rootQuantity, componentQuantity)))
  }

  @Test
  def newLayoutQuantityIsTheStoredStructure(): Unit = {
    assertEquals(
      Seq("[r1,[1.5,g,http://unitsofmeasure.org,g]," +
        "ArraySeq([500,mg,http://unitsofmeasure.org,mg])]", "[r2,null,ArraySeq(null)]"),
      rows(newQuantities.select(F.col("id"), rootQuantity, componentQuantity)))
  }

  @Test
  def quantityNormalisationReadsNoCanonicalForm(): Unit = {
    // A singular quantity is pruned to the fields the normalised structure keeps. Under a
    // repeating parent the quantity is normalised in a lambda, which nested schema pruning does
    // not see into, so the whole quantity is read there, as it was before the normalisation.
    val quantityFields = "id:string,value:decimal(32,6),value_scale:int,comparator:string," +
      "unit:string,system:string,code:string,_fid:int"
    assertEquals(s"struct<valueQuantity:struct<$quantityFields>>",
      readSchema(previousQuantities.select(rootQuantity)))
    assertEquals(s"struct<component:array<struct<valueQuantity:$previousQuantityType>>>",
      readSchema(previousQuantities.select(componentQuantity)))
  }

  @Test
  def structureWithoutCanonicalFormIsNotAQuantity(): Unit = {
    // Only the previous layout's discriminator selects the branch: a structure with a decimal value
    // but no canonical form, such as a Range bound, is traversed as it is stored.
    val data = parquet("notQuantity",
      "select named_struct('low', named_struct('value', cast(1.5 as decimal(32,6)))) as range")
    val low = data.select(resolveOrNull(F.col("range"), "low", NullType).alias("low"))
    assertEquals(StructType(Seq(StructField("value", DecimalType(32, 6)))),
      stripStructNullability(low.schema("low").dataType))
  }

  private def stripStructNullability(dataType: DataType): DataType = dataType match {
    case ArrayType(element, _) => ArrayType(stripStructNullability(element))
    case StructType(fields) =>
      StructType(fields.map(f => StructField(f.name, stripStructNullability(f.dataType))))
    case other => other
  }

  // -----------------------------------------------------------------------------------------------
  // Helpers.
  // -----------------------------------------------------------------------------------------------

  private def optimisedExpressions(df: DataFrame): Seq[Expression] =
    df.queryExecution.optimizedPlan.expressions.flatMap(_.collect { case e => e })

  /** Executes the plan and returns the schema its file scan read. */
  private def readSchema(df: DataFrame): String = {
    df.collect()
    val scans = collect(df.queryExecution.executedPlan) { case scan: FileSourceScanExec => scan }
    assertEquals(1, scans.size, df.queryExecution.executedPlan.toString)
    scans.head.metadata("ReadSchema")
  }

  private def strip(row: String): String = row.stripPrefix("[").stripSuffix("]")

  private def causes(t: Throwable): Seq[Throwable] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).toSeq
}
