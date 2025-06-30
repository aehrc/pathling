package au.csiro.pathling.views.ansi;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Visitor that converts parsed ANSI SQL type syntax into Spark SQL DataTypes.
 */
public class ToDataTypeVisitor extends AnsiSqlTypeBaseVisitor<DataType> {

  @Override
  public DataType visitSqlType(@Nonnull final AnsiSqlTypeParser.SqlTypeContext ctx) {
    if (ctx.characterType() != null) {
      return visitCharacterType(ctx.characterType());
    } else if (ctx.numericType() != null) {
      return visitNumericType(ctx.numericType());
    } else if (ctx.booleanType() != null) {
      return visitBooleanType(ctx.booleanType());
    } else if (ctx.binaryType() != null) {
      return visitBinaryType(ctx.binaryType());
    } else if (ctx.temporalType() != null) {
      return visitTemporalType(ctx.temporalType());
    } else if (ctx.complexType() != null) {
      return visitComplexType(ctx.complexType());
    }
    
    // Default to string for unrecognized types
    return DataTypes.StringType;
  }

  @Override
  public DataType visitCharacterType(@Nonnull final AnsiSqlTypeParser.CharacterTypeContext ctx) {
    // All character types map to StringType in Spark SQL
    return DataTypes.StringType;
  }

  @Override
  public DataType visitNumericType(@Nonnull final AnsiSqlTypeParser.NumericTypeContext ctx) {
    if (ctx.getText().toUpperCase().startsWith("SMALLINT")) {
      return DataTypes.ShortType;
    } else if (ctx.getText().toUpperCase().startsWith("INTEGER") || 
               ctx.getText().toUpperCase().startsWith("INT")) {
      return DataTypes.IntegerType;
    } else if (ctx.getText().toUpperCase().startsWith("BIGINT")) {
      return DataTypes.LongType;
    } else if (ctx.getText().toUpperCase().startsWith("REAL")) {
      return DataTypes.FloatType;
    } else if (ctx.getText().toUpperCase().startsWith("DOUBLE")) {
      return DataTypes.DoubleType;
    } else if (ctx.getText().toUpperCase().startsWith("FLOAT")) {
      if (ctx.precision != null) {
        int precision = Integer.parseInt(ctx.precision.getText());
        // ANSI SQL standard: FLOAT(p) maps to REAL if p <= 24, otherwise DOUBLE PRECISION
        return precision <= 24 ? DataTypes.FloatType : DataTypes.DoubleType;
      }
      // Default FLOAT with no precision to DOUBLE
      return DataTypes.DoubleType;
    } else if (ctx.getText().toUpperCase().startsWith("NUMERIC") || 
               ctx.getText().toUpperCase().startsWith("DECIMAL") || 
               ctx.getText().toUpperCase().startsWith("DEC")) {
      if (ctx.precision != null) {
        int precision = Integer.parseInt(ctx.precision.getText());
        int scale = ctx.scale != null ? Integer.parseInt(ctx.scale.getText()) : 0;
        return DataTypes.createDecimalType(precision, scale);
      }
      return DataTypes.createDecimalType();
    }
    
    // Default to double for unrecognized numeric types
    return DataTypes.DoubleType;
  }

  @Override
  public DataType visitBooleanType(@Nonnull final AnsiSqlTypeParser.BooleanTypeContext ctx) {
    return DataTypes.BooleanType;
  }

  @Override
  public DataType visitBinaryType(@Nonnull final AnsiSqlTypeParser.BinaryTypeContext ctx) {
    // All binary types map to BinaryType in Spark SQL
    return DataTypes.BinaryType;
  }

  @Override
  public DataType visitTemporalType(@Nonnull final AnsiSqlTypeParser.TemporalTypeContext ctx) {
    if (ctx.getText().toUpperCase().startsWith("DATE")) {
      return DataTypes.DateType;
    } else if (ctx.getText().toUpperCase().startsWith("TIMESTAMP")) {
      return DataTypes.TimestampType;
    } else if (ctx.getText().toUpperCase().startsWith("INTERVAL")) {
      // Spark doesn't have a direct interval type, use string
      return DataTypes.StringType;
    }
    
    // Default to timestamp for unrecognized temporal types
    return DataTypes.TimestampType;
  }

  @Override
  public DataType visitComplexType(@Nonnull final AnsiSqlTypeParser.ComplexTypeContext ctx) {
    if (ctx.rowType() != null) {
      return visitRowType(ctx.rowType());
    } else if (ctx.arrayType() != null) {
      return visitArrayType(ctx.arrayType());
    }
    
    // Default to string for unrecognized complex types
    return DataTypes.StringType;
  }

  @Override
  public DataType visitRowType(@Nonnull final AnsiSqlTypeParser.RowTypeContext ctx) {
    if (ctx.fieldDefinition() == null || ctx.fieldDefinition().isEmpty()) {
      // Empty ROW type
      return DataTypes.createStructType(new StructField[0]);
    }
    
    List<StructField> fields = new ArrayList<>();
    for (AnsiSqlTypeParser.FieldDefinitionContext fieldCtx : ctx.fieldDefinition()) {
      String fieldName = fieldCtx.fieldName.getText();
      DataType fieldType = visit(fieldCtx.sqlType());
      fields.add(DataTypes.createStructField(fieldName, fieldType, true));
    }
    
    return DataTypes.createStructType(fields.toArray(new StructField[0]));
  }

  @Override
  public DataType visitArrayType(@Nonnull final AnsiSqlTypeParser.ArrayTypeContext ctx) {
    if (ctx.sqlType() != null) {
      DataType elementType = visit(ctx.sqlType());
      return DataTypes.createArrayType(elementType);
    }
    
    // Default to string array if element type is not specified
    return DataTypes.createArrayType(DataTypes.StringType);
  }
}
