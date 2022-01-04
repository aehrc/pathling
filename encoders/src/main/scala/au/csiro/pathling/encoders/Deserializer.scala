/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2022, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 *
 */

package au.csiro.pathling.encoders

import ca.uhn.fhir.context.RuntimeChildPrimitiveEnumerationDatatypeDefinition
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance}
import org.apache.spark.sql.types.ObjectType

trait Deserializer {

  def enumerationToDeserializer(enumeration: RuntimeChildPrimitiveEnumerationDatatypeDefinition,
                                path: Option[Expression]): Expression = {

    def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, ObjectType(classOf[String])))

    // Get the value and initialize the instance.
    val utfToString = Invoke(getPath, "toString", ObjectType(classOf[String]), Nil)

    val enumFactory = Class.forName(enumeration.getBoundEnumType.getName + "EnumFactory")

    // Creates a new enum factory instance for each invocation, but this is cheap
    // on modern JVMs and probably more efficient than attempting to pool the underlying
    // FHIR enum factory ourselves.
    val factoryInstance = NewInstance(
      cls = enumFactory,
      arguments = Nil,
      propagateNull = false,
      dataType = ObjectType(enumFactory))

    Invoke(factoryInstance, "fromCode",
      ObjectType(enumeration.getBoundEnumType),
      List(utfToString))
  }
}
