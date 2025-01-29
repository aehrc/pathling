/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
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

package au.csiro.pathling.encoders

import org.apache.spark.SparkException
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedException}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.trees.TreePattern.{ARRAYS_ZIP, TreePattern}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._

import scala.language.existentials

/**
 * Invokes a static function, returning the result.  By default, any of the arguments being null
 * will result in returning null instead of calling the function.
 *
 * @param staticObject The target of the static call.  This can either be the object itself
 *                     (methods defined on scala objects), or the class object
 *                     (static methods defined in java).
 * @param dataType     The expected type of the static field
 * @param fieldName    The name of the field to retrieve
 */
case class StaticField(staticObject: Class[_],
                       dataType: DataType,
                       fieldName: String) extends Expression with NonSQLExpression {

  private val objectName: String = staticObject.getName.stripSuffix("$")

  override def nullable: Boolean = false

  override def children: Seq[Expression] = Nil

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = CodeGenerator.javaType(dataType)

    val code =
      code"""
      final $javaType ${ev.value} = $objectName.$fieldName;
        """
    ev.copy(code = code, isNull = FalseLiteral)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    StaticField(staticObject, dataType, fieldName)
  }

}

/**
 * Determines if the given value is an instanceof a given class
 *
 * @param value       the value to check
 * @param checkedType the class to check the value against
 */
case class InstanceOf(value: Expression,
                      checkedType: Class[_]) extends Expression with NonSQLExpression {

  override def nullable: Boolean = false

  override def children: Seq[Expression] = value :: Nil

  override def dataType: DataType = BooleanType

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    val obj = value.genCode(ctx)

    val code =
      code"""
         ${obj.code}
         final boolean ${ev.value} = ${obj.value} instanceof ${checkedType.getName};
       """

    ev.copy(code = code, isNull = FalseLiteral)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    InstanceOf(newChildren.head, checkedType)
  }

}


/**
 * Gets value of an element of a HAPI object. This is could be composed from `If` and `Invoke`
 * expression but having it as a dedicated expression makes the serializer expression more readable 
 * and also avoids the problems with 'If' optimisations.
 *
 * @param value     the expression with the reference to the HAPI object
 * @param dataType  the data type of the element to get          
 * @param hasMethod the name of method to check if the element is present, e.g. hasNameElement()
 * @param getMethod the name of method to get the value of the element, e.g. getNameElement()                  
 * @return the value of the element or null if the element is not present or the object is null
 */
case class GetHapiValue(value: Expression,
                        dataType: DataType,
                        hasMethod: String, getMethod: String)
  extends Expression with NonSQLExpression {

  override def nullable: Boolean = true

  override def children: Seq[Expression] = value :: Nil

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    val obj = value.genCode(ctx)
    val javaType = CodeGenerator.javaType(dataType)
    val code =
      code"""
            |// BEGIN: GetHapiValue
            |${obj.code}
            |boolean ${ev.isNull} = true;
            |$javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
            |if (!${obj.isNull} && ${obj.value}.$hasMethod()) {
            | ${ev.isNull} = false;
            | ${ev.value} = ${obj.value}.$getMethod();
            |} 
            |// END: GetHapiValue        
       """.stripMargin
    ev.copy(code = code)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    GetHapiValue(newChildren.head, dataType, hasMethod, getMethod)
  }
}

/**
 * Casts the result of an expression to another type.
 *
 * @param value      The value to cast
 * @param resultType The type to which the value should be cast
 */
case class ObjectCast(value: Expression, resultType: DataType)
  extends Expression with NonSQLExpression {

  override def nullable: Boolean = value.nullable

  override def dataType: DataType = resultType

  override def children: Seq[Expression] = value :: Nil

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    val javaType = CodeGenerator.javaType(resultType)
    val obj = value.genCode(ctx)

    val code =
      code"""
         ${obj.code}
         final $javaType ${ev.value} = ($javaType) ${obj.value};
       """

    ev.copy(code = code, isNull = obj.isNull)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    ObjectCast(newChildren.head, resultType)
  }

}

/**
 * Registers the mapping of a deserialized object to its _fid value. The mapping is saved in '_fid_mapping' immutable state variable.
 * Evaluates as identity.
 *
 * @param targetObject the expression, which value should be registered.
 * @param fidValue     the fid value to use.
 */
case class RegisterFid(targetObject: Expression,
                       fidValue: Expression)
  extends Expression with NonSQLExpression {

  override def nullable: Boolean = targetObject.nullable

  override def children: Seq[Expression] = targetObject :: fidValue :: Nil

  override def dataType: DataType = targetObject.dataType

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = CodeGenerator.javaType(dataType)

    val obj = targetObject.genCode(ctx)
    val fid = fidValue.genCode(ctx)

    ctx.addImmutableStateIfNotExists("java.util.HashMap", "_fid_mapping",
      vn => s"$vn = new java.util.HashMap();")

    ev.copy(code =
      code"""
            |${obj.code}
            |${fid.code}
            |$javaType ${ev.value} = null;
            |boolean ${ev.isNull} = true;
            |if (${obj.value} != null) {
            | _fid_mapping.put(${fid.value}, ${obj.value});
            | ${ev.value} = ${obj.value};
            | ${ev.isNull} = false;
            |}
       """.stripMargin)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    RegisterFid(newChildren.head, newChildren.tail.head)
  }
}

/**
 * Attaches the Extensions from the provided fid->Extension map to objects
 * registered in the `_fid_mapping` with [[RegisterFid]].
 *
 * @param targetObject       the container object that should be the result of evaluation.
 * @param extensionMapObject the expression which evaluates to Map(Integer,Array(Extension))
 *                           which maps fid to the list of Extensions for the object with this fid.
 */
case class AttachExtensions(targetObject: Expression,
                            extensionMapObject: Expression)
  extends Expression with NonSQLExpression {

  override def nullable: Boolean = targetObject.nullable

  override def children: Seq[Expression] = targetObject :: extensionMapObject :: Nil

  override def dataType: DataType = targetObject.dataType

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = CodeGenerator.javaType(dataType)

    val obj = targetObject.genCode(ctx)
    val extensionMap = extensionMapObject.genCode(ctx)

    ctx.addImmutableStateIfNotExists("java.util.HashMap", "_fid_mapping",
      vn => s"$vn = new java.util.HashMap();")

    // essentially

    ev.copy(code =
      code"""
            |${obj.code}
            |${extensionMap.code}
            |$javaType ${ev.value} = null;
            |boolean ${ev.isNull} = true;
            |if (${obj.value} != null) {
            |// for each of the object in extension maps find the
            |// corresponding object and set the extension
            | for(java.util.Map.Entry e: scala.collection.JavaConverters.mapAsJavaMap(${
        extensionMap.value
      }).entrySet()) {
            |   org.hl7.fhir.instance.model.api.IBaseHasExtensions extHolder = (org.hl7.fhir.instance.model.api.IBaseHasExtensions)_fid_mapping.get(e.getKey());
            |   if (extHolder != null) {
            |     ((java.util.List)extHolder.getExtension()).addAll((java.util.List)e.getValue());
            |   }
            | }
            | ${ev.value} = ${obj.value};
            | ${ev.isNull} = false;
            |}
       """.stripMargin)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    AttachExtensions(newChildren.head, newChildren.tail.head)
  }
}


case class UnresolvedIfArray(value: Expression, arrayExpressions: Expression => Expression,
                             elseExpression: Expression => Expression)
  extends Expression with Unevaluable with NonSQLExpression {

  override def mapChildren(f: Expression => Expression): Expression = {

    val newValue = f(value)

    if (newValue.resolved) {
      newValue.dataType match {
        case ArrayType(_, _) => f(arrayExpressions(newValue))
        case _ => f(elseExpression(newValue))
      }
    }
    else {
      copy(value = newValue)
    }
  }

  override def dataType: DataType = throw new UnresolvedException("dataType")

  override def nullable: Boolean = throw new UnresolvedException("nullable")

  override lazy val resolved = false

  override def toString: String = s"$value"

  override def children: Seq[Expression] = value :: Nil

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    UnresolvedIfArray(newChildren.head, arrayExpressions, elseExpression)
  }
}

case class UnresolvedIfArray2(value: Expression, arrayExpressions: Expression => Expression,
                              elseExpression: Expression => Expression)
  extends Expression with Unevaluable with NonSQLExpression {

  override def mapChildren(f: Expression => Expression): Expression = {

    val newValue = f(value)

    if (newValue.resolved) {
      newValue.dataType match {
        case ArrayType(ArrayType(_, _), _) => f(arrayExpressions(newValue))
        case ArrayType(_, _) => f(elseExpression(newValue))
        case _ => throw new SparkException(
          errorClass = "ARRAY_TYPE_EXPECTED",
          messageParameters = Map(
            "actualType" -> newValue.dataType.toString),
          cause = null)
      }
    }
    else {
      copy(value = newValue)
    }
  }

  override def dataType: DataType = throw new UnresolvedException("dataType")

  override def nullable: Boolean = throw new UnresolvedException("nullable")

  override lazy val resolved = false

  override def toString: String = s"$value"

  override def children: Seq[Expression] = value :: Nil

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    UnresolvedIfArray2(newChildren.head, arrayExpressions, elseExpression)
  }
}


case class UnresolvedUnnest(value: Expression)
  extends Expression with Unevaluable with NonSQLExpression {

  override def mapChildren(f: Expression => Expression): Expression = {

    val newValue = f(value)

    if (newValue.resolved) {
      newValue.dataType match {
        case ArrayType(ArrayType(_, _), _) => Flatten(newValue)
        case _ => newValue
      }
    }
    else {
      copy(value = newValue)
    }
  }

  override def dataType: DataType = throw new UnresolvedException("dataType")

  override def nullable: Boolean = throw new UnresolvedException("nullable")

  override lazy val resolved = false

  override def toString: String = s"$value"

  override def children: Seq[Expression] = value :: Nil

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    UnresolvedUnnest(newChildren.head)
  }
}


object ValueFunctions {
  /**
   * Applies an expression to an an array value, or an else expression if the value is not an array.
   *
   * @param value           The value to check
   * @param arrayExpression The expression to apply to the array value
   * @param elseExpression  The expression to apply if the value is not an array
   * @return
   */
  def ifArray(value: Column, arrayExpression: Column => Column,
              elseExpression: Column => Column): Column = {
    val expr = UnresolvedIfArray(value.expr,
      e => arrayExpression(new Column(e)).expr, e => elseExpression(new Column(e)).expr)
    new Column(expr)
  }

  /**
   * Applies an expression to  array of arrays value, or an else expression if the value is an array.
   * Throws an exception if the value is not an array.
   *
   * @param value           The value to check
   * @param arrayExpression The expression to apply to the array of arrays value
   * @param elseExpression  The expression to apply to the arrays of non array values
   * @return
   */
  def ifArray2(value: Column, arrayExpression: Column => Column,
               elseExpression: Column => Column): Column = {
    val expr = UnresolvedIfArray2(value.expr,
      e => arrayExpression(new Column(e)).expr, e => elseExpression(new Column(e)).expr)
    new Column(expr)
  }

  def unnest(value: Column): Column = {
    val expr = UnresolvedUnnest(value.expr)
    new Column(expr)
  }

}


/**
 * An expression which takes a number of columns that contain arrays of structs and produces
 * an array of structs where each element is a product of the elements of the input arrays.
 *
 * @param children The input columns
 * @param outer    If true, the output array will contain nulls for missing elements in the input
 */
case class StructProduct(children: Seq[Expression], outer: Boolean = false)
  extends Expression with NonSQLExpression {

  final override val nodePatterns: Seq[TreePattern] = Seq(ARRAYS_ZIP)


  override lazy val resolved: Boolean =
    childrenResolved && checkInputDataTypes.isSuccess

  @transient override lazy val checkInputDataTypes: TypeCheckResult = {
    if (children.forall(_.dataType.isInstanceOf[ArrayType])) TypeCheckSuccess
    else TypeCheckFailure("Arrays of structs expected")
  }

  @transient override lazy val dataType: DataType = {
    val fields = {
      children
        .flatMap(_.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fields)
    }
    ArrayType(StructType(fields), containsNull = true)
  }

  override def nullable: Boolean = children.exists(_.nullable)

  private def genericArrayData = classOf[GenericArrayData].getName

  @transient private lazy val arrayElementTypes =
    children.map(_.dataType.asInstanceOf[ArrayType].elementType)


  @transient private lazy val aritiesOfChildren = children
    .map(_.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fields.length)
    .toArray
  @transient private lazy val sizeOfOutput = aritiesOfChildren.sum
  @transient private lazy val offsetsScala = aritiesOfChildren.scanLeft(0)(_ + _).init

  private def emptyInputGenCode(ev: ExprCode): ExprCode = {
    ev.copy(
      code"""
            |${CodeGenerator.javaType(dataType)} ${ev.value} = new $genericArrayData(new Object[0]);
            |boolean ${ev.isNull} = false;
    """.stripMargin)
  }

  private def nonEmptyInputGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    // the number of fields for each child
    val aritiesOfChildrenScala = children
      .map(_.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fields.length)
      .toArray;
    val sizeOfOutput = aritiesOfChildrenScala.sum

    val offsetsScala = aritiesOfChildrenScala.scanLeft(0)(_ + _).init
    val offsets = ctx.addReferenceObj("offsets", offsetsScala, "int[]");

    val genericInternalRow = classOf[GenericInternalRow].getName
    val arrVals = ctx.freshName("arrVals")
    val productSize = ctx.freshName("productSize")

    val currentRow = ctx.freshName("currentRow")
    val i = ctx.freshName("i")
    val args = ctx.freshName("args")
    val prodIdxs = ctx.freshName("prodIdxs")


    val evals = children.map(_.genCode(ctx))
    val getValuesAndProductSize = evals.zipWithIndex.map { case (eval, index) =>
      s"""
         |if ($productSize != 0) {
         |  ${eval.code}
         |  if (!${eval.isNull}) {
         |    $arrVals[$index] = ${eval.value};
         |    $productSize *=  ${eval.value}.numElements();
         |  } else {
         |    $productSize = 0;
         |  }
         |}
      """.stripMargin
    }

    val splitGetValuesAndProductSize = ctx.splitExpressionsWithCurrentInputs(
      expressions = getValuesAndProductSize,
      funcName = "getValuesAndProductSize",
      returnType = "int",
      makeSplitFunction = body =>
        s"""
           |$body
           |return $productSize;
        """.stripMargin,
      foldFunctions = _.map(funcCall => s"$productSize = $funcCall;").mkString("\n"),
      extraArguments =
        ("ArrayData[]", arrVals) ::
          ("int", productSize) :: Nil)


    // idx -  the the index of the child expression (the array to cross)
    // i - the index of the product element 
    val getValueForType = arrayElementTypes.zipWithIndex.map { case (eleType, idx) =>
      val g = CodeGenerator.getValue(s"$arrVals[$idx]", eleType, s"$prodIdxs[$idx]")
      val structValue = s"structValue_$idx"
      s"""
         | ${CodeGenerator.javaType(eleType)} $structValue = $g; 
         | // using the base copy the values of all the fields to the output
         | for(int fi = 0; $structValue != null && fi < $structValue.numFields(); fi++) {
         |    if(!$structValue.isNullAt(fi)) {
         |      $currentRow[$offsets[$idx] + fi] = $structValue.get(fi, null);
         |    }
         | }
      """.stripMargin
    }

    val getValueForTypeSplitted = ctx.splitExpressions(
      expressions = getValueForType,
      funcName = "extractValue",
      arguments =
        ("int[]", prodIdxs) ::
          ("Object[]", currentRow) ::
          ("ArrayData[]", arrVals) :: Nil)

    val initVariables =
      s"""
         |ArrayData[] $arrVals = new ArrayData[${children.length}];
         |int $productSize = 1;
         |${CodeGenerator.javaType(dataType)} ${ev.value} = null;
    """.stripMargin

    ev.copy(
      code"""
            |// BEGIN: PS_CODE
            |$initVariables
            |$splitGetValuesAndProductSize
            |//System.out.println("DEBUG: ${this.prettyName}[${this.hashCode()}]" + " productSize: " + $productSize);
            |//boolean ${ev.isNull} = $productSize == 0;
            | boolean ${ev.isNull} = false;
            | if ($productSize > 0 || !$outer) {
            |  Object[] $args = new Object[$productSize];
            |  int[] $prodIdxs = new int[${children.length}];
            |  
            |  for (int $i = 0; $i < $productSize; $i++) {
            |    int productBase = $i;
            |    for (int childIndex = 0; childIndex < ${children.length}; childIndex++) {
            |      int childArity = $arrVals[childIndex].numElements();
            |      $prodIdxs[childIndex] = productBase % childArity;
            |      productBase /= childArity;;       
            |    }
            |    Object[] $currentRow = new Object[$sizeOfOutput];
            |    $getValueForTypeSplitted
            |    $args[$i] = new $genericInternalRow($currentRow);
            |  }
            |  
            |  ${ev.value} = new $genericArrayData($args);
            |} else {
            |  ${ev.value} = new $genericArrayData(new Object[]{null});
            |}
            |// END: PS_CODE
    """.stripMargin)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (children.isEmpty) {
      emptyInputGenCode(ev)
    } else {
      nonEmptyInputGenCode(ctx, ev)
    }
  }

  override def eval(input: InternalRow): Any = {
    val inputArrays = children.map(_.eval(input).asInstanceOf[ArrayData])
    val productSize = if (inputArrays.isEmpty || inputArrays.contains(null)) {
      0
    } else {
      inputArrays.map(_.numElements()).product
    }

    if (productSize > 0 || !outer) {
      val result = new Array[InternalRow](productSize)
      val zippedArrays: Seq[(ArrayData, Int)] = inputArrays.zipWithIndex
      for (i <- 0 until productSize) {
        val productIndexes: Array[Int] = new Array[Int](children.length);
        var productBase = i;
        for (childIndex <- children.indices) {
          val childArity = inputArrays(childIndex).numElements();
          productIndexes(childIndex) = productBase % childArity;
          productBase = productBase / childArity;
        }
        val currentRowData = new Array[Any](sizeOfOutput)
        zippedArrays.foreach { case (arr, index) =>
          if (!arr.isNullAt(productIndexes(index))) {
            val structData = arr.get(productIndexes(index), arrayElementTypes(index))
              .asInstanceOf[InternalRow]
            for (fi <- 0 until structData.numFields) {
              if (!structData.isNullAt(fi)) {
                currentRowData(offsetsScala(index) + fi) = structData.get(fi, null)
              }
            }
          }
        }
        result(i) = InternalRow.apply(currentRowData: _*)
      }
      new GenericArrayData(result)
    } else {
      new GenericArrayData(Array[InternalRow](null))
    }
  }

  override def prettyName: String = if (outer) "struct_prod_outer" else "struct_prod"

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): StructProduct =
    copy(children = newChildren)
}


object ColumnFunctions {
  /**
   * An expression which takes a number of columns that contain arrays of structs and produces
   * an array of structs where each element is a product of the elements of the input arrays.
   *
   * @param e The input columns
   */
  @scala.annotation.varargs
  def structProduct(e: Column*): Column = new Column(StructProduct(e.map(_.expr)))

  /**
   * An expression which takes a number of columns that contain arrays of structs and produces
   * an array of structs where each element is a product of the elements of the input arrays.
   *
   * If the input arrays are of different lengths, the output array will contain nulls for missing
   * elements in the input.
   *
   * @param e The input columns
   */
  @scala.annotation.varargs
  def structProduct_outer(e: Column*): Column = new Column(
    StructProduct(e.map(_.expr), outer = true))
}
