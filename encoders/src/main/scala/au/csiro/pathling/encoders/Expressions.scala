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

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    GetClassFromContained(newChildren.head, containedClass)
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
