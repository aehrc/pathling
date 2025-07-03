package au.csiro.pathling.views.ansi;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.types.DataType;

/**
 * Visitor that converts parsed ANSI SQL type syntax into Spark SQL DataTypes.
 */
public class ToDataTypeVisitor extends TypesOfAnsiSqlBaseVisitor<DataType> {

  private final AnsiSqlDataTypeFactory factory;

  /**
   * Constructor.
   */
  public ToDataTypeVisitor() {
    this.factory = new AnsiSqlDataTypeFactory();
  }
  
  @Override
  public DataType visitSqlType(@Nonnull final TypesOfAnsiSqlParser.SqlTypeContext ctx) {
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

    // Default for unrecognized types
    return factory.createDefault();
  }

  @Override
  public DataType visitCharacterType(@Nonnull final TypesOfAnsiSqlParser.CharacterTypeContext ctx) {
    String typeText = ctx.getText().toUpperCase();

    if (ctx.length != null) {
      int length = Integer.parseInt(ctx.length.getText());
      if (typeText.startsWith("VARCHAR") || typeText.startsWith("CHARACTER VARYING")) {
        return factory.createVarchar(length);
      } else {
        return factory.createCharacter(length);
      }
    } else {
      if (typeText.startsWith("VARCHAR") || typeText.startsWith("CHARACTER VARYING")) {
        return factory.createVarchar();
      } else {
        return factory.createCharacter();
      }
    }
  }

  @Override
  public DataType visitNumericType(@Nonnull final TypesOfAnsiSqlParser.NumericTypeContext ctx) {
    String typeText = ctx.getText().toUpperCase();

    if (typeText.startsWith("SMALLINT")) {
      return factory.createSmallInt();
    } else if (typeText.startsWith("INTEGER") || typeText.startsWith("INT")) {
      return factory.createInteger();
    } else if (typeText.startsWith("BIGINT")) {
      return factory.createBigInt();
    } else if (typeText.startsWith("REAL")) {
      return factory.createReal();
    } else if (typeText.startsWith("DOUBLE")) {
      return factory.createDouble();
    } else if (typeText.startsWith("FLOAT")) {
      if (ctx.precision != null) {
        int precision = Integer.parseInt(ctx.precision.getText());
        return factory.createFloat(precision);
      }
      return factory.createFloat();
    } else if (typeText.startsWith("NUMERIC") || typeText.startsWith("DECIMAL")
        || typeText.startsWith("DEC")) {
      if (ctx.precision != null) {
        int precision = Integer.parseInt(ctx.precision.getText());
        if (ctx.scale != null) {
          int scale = Integer.parseInt(ctx.scale.getText());
          return factory.createDecimal(precision, scale);
        }
        return factory.createDecimal(precision);
      }
      return factory.createDecimal();
    }

    // Default for unrecognized numeric types
    return factory.createDouble();
  }

  @Override
  public DataType visitBooleanType(@Nonnull final TypesOfAnsiSqlParser.BooleanTypeContext ctx) {
    return factory.createBoolean();
  }

  @Override
  public DataType visitBinaryType(@Nonnull final TypesOfAnsiSqlParser.BinaryTypeContext ctx) {
    String typeText = ctx.getText().toUpperCase();

    if (ctx.length != null) {
      int length = Integer.parseInt(ctx.length.getText());
      if (typeText.startsWith("VARBINARY") || typeText.startsWith("BINARY VARYING")) {
        return factory.createVarbinary(length);
      } else {
        return factory.createBinary(length);
      }
    } else {
      if (typeText.startsWith("VARBINARY") || typeText.startsWith("BINARY VARYING")) {
        return factory.createVarbinary();
      } else {
        return factory.createBinary();
      }
    }
  }

  @Override
  public DataType visitTemporalType(@Nonnull final TypesOfAnsiSqlParser.TemporalTypeContext ctx) {
    String typeText = ctx.getText().toUpperCase();

    if (typeText.startsWith("DATE")) {
      return factory.createDate();
    } else if (typeText.startsWith("TIMESTAMP")) {
      boolean withTimeZone = typeText.contains("WITH TIME ZONE");
      if (ctx.precision != null) {
        int precision = Integer.parseInt(ctx.precision.getText());
        return withTimeZone
               ? factory.createTimestampWithTimeZone(precision)
               : factory.createTimestamp(precision);
      }
      return withTimeZone
             ? factory.createTimestampWithTimeZone()
             : factory.createTimestamp();
    } else if (typeText.startsWith("INTERVAL")) {
      return factory.createInterval();
    }

    // Default for unrecognized temporal types
    return factory.createTimestamp();
  }

  @Override
  public DataType visitComplexType(@Nonnull final TypesOfAnsiSqlParser.ComplexTypeContext ctx) {
    if (ctx.rowType() != null) {
      return visitRowType(ctx.rowType());
    } else if (ctx.arrayType() != null) {
      return visitArrayType(ctx.arrayType());
    }

    // Default for unrecognized complex types
    return factory.createDefault();
  }

  @Override
  public DataType visitRowType(@Nonnull final TypesOfAnsiSqlParser.RowTypeContext ctx) {
    if (ctx.fieldDefinition() == null || ctx.fieldDefinition().isEmpty()) {
      // Empty ROW type
      return factory.createRow(new ArrayList<>());
    }

    List<Pair<String, DataType>> fields = new ArrayList<>();
    for (TypesOfAnsiSqlParser.FieldDefinitionContext fieldCtx : ctx.fieldDefinition()) {
      String fieldName = fieldCtx.fieldName.getText();
      DataType fieldType = visit(fieldCtx.sqlType());
      fields.add(Pair.of(fieldName, fieldType));
    }

    return factory.createRow(fields);
  }

  @Override
  public DataType visitArrayType(@Nonnull final TypesOfAnsiSqlParser.ArrayTypeContext ctx) {
    if (ctx.sqlType() != null) {
      DataType elementType = visit(ctx.sqlType());
      return factory.createArray(elementType);
    }

    // Default to string array if element type is not specified
    return factory.createArray(factory.createCharacter());
  }
}
