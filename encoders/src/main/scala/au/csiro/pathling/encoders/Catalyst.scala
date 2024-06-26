package au.csiro.pathling.encoders

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.DataType


/**
 * This class provides a compatibility layer for Spark Catalyst.
 * It is used to createnew Expressions using constructors compatible with the runtime 
 * version of Spark Catalyst.
 * This is necessary to address running Pathling in Databricks environments, 
 * which habitually use different (newer) version of Catalyst that one used by 
 * the corresponding Spark public release. 
 */
object Catalyst {

  private lazy val staticInvokeAdapter: (Class[_], DataType, String, Seq[Expression]) => StaticInvoke = {
    val constructor = classOf[StaticInvoke].getConstructors.head
    if (constructor.getParameterCount == 8) {
      // catalyst 3.5.x (used by Spark 3.5.x)
      StaticInvoke.apply(_, _, _, _)
    } else if (constructor.getParameterCount == 9) {
      // catalyst 4.0.0-preview-rc1 (used by Databricks runtime 14.3 LTS with Spark 3.5.0)
      (staticObject: Class[_], dataType: DataType, functionName: String, arguments: Seq[Expression]) =>
        constructor.newInstance(staticObject, dataType, functionName,
          arguments,
          Nil,
          Boolean.box(true),
          Boolean.box(true),
          Boolean.box(true),
          None).asInstanceOf[StaticInvoke]
    } else {
      throw new IllegalStateException(
        "Unsupported version of Spark Catalyst with InvokeStatic constructor: " + constructor)
    }
  }

  /**
   * Creates a new [[StaticInvoke]] expression using a constructor compatible with the runtime version of Spark Catalyst.
   *
   * @param staticObject the class object to invoke.
   * @param dataType     the return type of the function.
   * @param functionName the name of the function to invoke.
   * @param arguments    the arguments to pass to the function.
   * @return the new [[StaticInvoke]] expression.
   */
  def staticInvoke(
                    staticObject: Class[_],
                    dataType: DataType,
                    functionName: String,
                    arguments: Seq[Expression] = Nil): StaticInvoke = {
    staticInvokeAdapter(staticObject, dataType, functionName, arguments)
  }
}
