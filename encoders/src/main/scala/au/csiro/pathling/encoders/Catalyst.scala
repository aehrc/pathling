package au.csiro.pathling.encoders

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.DataType

object Catalyst {
  lazy val staticInvokeAdapter:(Class[_],DataType, String, Seq[Expression]) => StaticInvoke = {
    val constructor = classOf[StaticInvoke].getConstructors.head
    if (constructor.getParameterCount == 8) {
      StaticInvoke.apply(_,_,_,_)
    } else {
      (staticObject: Class[_], dataType: DataType, functionName: String, arguments: Seq[Expression]) =>
        constructor.newInstance(staticObject, dataType, functionName, 
          arguments,
          Nil,
          Boolean.box(true),
          Boolean.box(true),
          Boolean.box(true), 
          None).asInstanceOf[StaticInvoke]
    }
  }

  def staticInvoke(
                    staticObject: Class[_],
                    dataType: DataType,
                    functionName: String,
                    arguments: Seq[Expression] = Nil): StaticInvoke = {
    staticInvokeAdapter(staticObject, dataType, functionName, arguments)
  }

}
