package au.csiro.pathling.views.ansi;

import static au.csiro.pathling.utilities.Preconditions.check;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.views.ansi.generated.TypesOfAnsiSqlBaseVisitor;
import au.csiro.pathling.views.ansi.generated.TypesOfAnsiSqlParser;
import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.antlr.v4.runtime.tree.TerminalNode;
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

  /**
   * Checks if any of the provided nodes are non-null.
   *
   * @param nodes the nodes to check
   * @return true if any node is non-null, false otherwise
   */
  private static boolean anyOf(@Nonnull TerminalNode... nodes) {
    return Stream.of(nodes).anyMatch(Objects::nonNull);
  }

  @Override
  @Nonnull
  public DataType visitSqlType(@Nonnull final TypesOfAnsiSqlParser.SqlTypeContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  @Nonnull
  public DataType visitCharacterType(@Nonnull final TypesOfAnsiSqlParser.CharacterTypeContext ctx) {
    if (anyOf(ctx.K_VARCHAR(), ctx.K_VARYING())) {
      return nonNull(ctx.length)
             ? factory.createVarchar(Integer.parseInt(ctx.length.getText()))
             : factory.createVarchar();
    } else {
      check(anyOf(ctx.K_CHARACTER(), ctx.K_CHAR()) && !anyOf(ctx.K_VARYING()));
      return nonNull(ctx.length)
             ? factory.createCharacter(Integer.parseInt(ctx.length.getText()))
             : factory.createCharacter();
    }
  }

  @Override
  @Nonnull
  public DataType visitNumericType(@Nonnull final TypesOfAnsiSqlParser.NumericTypeContext ctx) {
    if (anyOf(ctx.K_SMALLINT())) {
      return factory.createSmallInt();
    } else if (anyOf(ctx.K_INTEGER(), ctx.K_INT())) {
      return factory.createInteger();
    } else if (anyOf(ctx.K_BIGINT())) {
      return factory.createBigInt();
    } else if (anyOf(ctx.K_REAL())) {
      return factory.createReal();
    } else if (anyOf(ctx.K_DOUBLE())) {
      return factory.createDouble();
    } else if (anyOf(ctx.K_FLOAT())) {
      return nonNull(ctx.precision)
             ? factory.createFloat(Integer.parseInt(ctx.precision.getText()))
             : factory.createFloat();
    } else {
      check(anyOf(ctx.K_NUMERIC(), ctx.K_DECIMAL(), ctx.K_DEC()));
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
  }

  @Override
  @Nonnull
  public DataType visitBooleanType(@Nonnull final TypesOfAnsiSqlParser.BooleanTypeContext ctx) {
    return factory.createBoolean();
  }

  @Override
  @Nonnull
  public DataType visitBinaryType(@Nonnull final TypesOfAnsiSqlParser.BinaryTypeContext ctx) {
    if (anyOf(ctx.K_VARBINARY(), ctx.K_VARYING())) {
      return nonNull(ctx.length)
             ? factory.createVarbinary(Integer.parseInt(ctx.length.getText()))
             : factory.createVarbinary();
    } else {
      check(anyOf(ctx.K_BINARY()) && !anyOf(ctx.K_VARYING()));
      return nonNull(ctx.length)
             ? factory.createBinary(Integer.parseInt(ctx.length.getText()))
             : factory.createBinary();
    }
  }

  @Override
  @Nonnull
  public DataType visitTemporalType(@Nonnull final TypesOfAnsiSqlParser.TemporalTypeContext ctx) {
    if (anyOf(ctx.K_DATE())) {
      return factory.createDate();
    } else if (anyOf(ctx.K_INTERVAL())) {
      return factory.createInterval();
    } else {
      check(anyOf(ctx.K_TIMESTAMP()));
      boolean withTimeZone = nonNull(ctx.timeZone()) && anyOf(ctx.timeZone().K_WITH());
      if (ctx.precision != null) {
        int precision = Integer.parseInt(ctx.precision.getText());
        return withTimeZone
               ? factory.createTimestampWithTimeZone(precision)
               : factory.createTimestamp(precision);
      }
      return withTimeZone
             ? factory.createTimestampWithTimeZone()
             : factory.createTimestamp();
    }
  }

  @Override
  @Nonnull
  public DataType visitComplexType(@Nonnull final TypesOfAnsiSqlParser.ComplexTypeContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  @Nonnull
  public DataType visitRowType(@Nonnull final TypesOfAnsiSqlParser.RowTypeContext ctx) {
    return factory.createRow(
        requireNonNull(ctx.fieldDefinition()).stream()
            .map(fieldCtx -> Pair.of(
                requireNonNull(fieldCtx.fieldName).getText(),
                visit(requireNonNull(fieldCtx.sqlType()))))
            .toList()
    );
  }

  @Override
  @Nonnull
  public DataType visitArrayType(@Nonnull final TypesOfAnsiSqlParser.ArrayTypeContext ctx) {
    final DataType elementType = visit(requireNonNull(ctx.sqlType()));
    return factory.createArray(requireNonNull(elementType));
  }
}
