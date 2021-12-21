/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2021, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 */

/*
 * This is a backport of encoder functionality targeted for Spark 2.4.
 *
 * See https://issues.apache.org/jira/browse/SPARK-22739 for details.
 */

package au.csiro.pathling.encoders

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
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

  val objectName: String = staticObject.getName.stripSuffix("$")

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
 * An Expression extracting an object having the given class definition from a List of FHIR
 * Resources.
 */
case class GetClassFromContained(targetObject: Expression,
                                 containedClass: Class[_])
  extends Expression with NonSQLExpression {

  override def nullable: Boolean = targetObject.nullable

  override def children: Seq[Expression] = targetObject :: Nil

  override def dataType: DataType = ObjectType(containedClass)

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    val javaType = containedClass.getName
    val obj = targetObject.genCode(ctx)

    ev.copy(code =
      code"""
            |${obj.code}
            |$javaType ${ev.value} = null;
            |boolean ${ev.isNull} = true;
            |java.util.List<Object> contained = ${obj.value}.getContained();
            |
            |for (int containedIndex = 0; containedIndex < contained.size(); containedIndex++) {
            |  if (contained.get(containedIndex) instanceof $javaType) {
            |    ${ev.value} = ($javaType) contained.get(containedIndex);
            |    ${ev.isNull} = false;
            |  }
            |}
       """.stripMargin)
  }
}
