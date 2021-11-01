package au.csiro.pathling

import org.apache.spark.sql.catalyst.expressions.Expression

package object encoders2 {

  type Serializer = Expression => Expression

  type NamedSerializer = (String, Expression)

}
