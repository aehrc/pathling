package au.csiro.pathling.test.yaml;

import com.fasterxml.jackson.core.JsonGenerator;
import jakarta.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.CreateNamedStruct;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConverters;
import java.io.IOException;
import java.util.List;

/**
 * A utility class to write JSON representations of Spark SQL expressions.
 * It handles specific expression types and writes them to a JSON generator. 
 * <p>
 * The types supported should allow serialization of FHIRPath literals, created by the corresponding Collection subclasses.
 */
@Value(staticConstructor = "of")
class JsonColumnWriter {

  @Nonnull
  JsonGenerator gen;

  /**
   * Writes a JSON representation of a Spark SQL expression to the provided JSON generator.
   *
   * @param column the column to be written
   * @throws IOException if an I/O error occurs during writing
   */
  void writeColumn(@Nonnull final Column column) throws IOException {
    writeExpression(column.expr());
  }

  private void writeExpression(@Nonnull final Expression expr) throws IOException {
    if (expr instanceof CreateNamedStruct cnd) {
      gen.writeStartObject();
      final List<Expression> children = JavaConverters.seqAsJavaList(cnd.children());
      for (int i = 0; i < children.size() / 2; i++) {
        final Literal nameLiteral = (Literal) children.get(i * 2);
        final String name = nameLiteral.toString();
        final Expression value = children.get(i * 2 + 1);
        gen.writeFieldName(name);
        writeExpression(value);
      }
      gen.writeEndObject();
      // even elemnts are literals with names w
    } else if (expr instanceof Literal ltr) {
      if (DataTypes.StringType.equals(ltr.dataType())) {
        gen.writeString(ltr.toString());
      } else {
        gen.writeRawValue(ltr.toString());
      }
      // Handle literal expression
    } else if (expr instanceof Alias als) {
      writeExpression(als.child());
    } else if (expr instanceof Cast cst) {
      writeExpression(cst.child());
    } else {
      throw new IllegalArgumentException("Unsupported expression type: " + expr.getClass());
    }
  }
}
