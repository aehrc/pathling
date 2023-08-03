package au.csiro.pathling.fhirpath;

import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.literal.LiteralPath;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.StringType;

public class TypeSpecifier extends LiteralPath<StringType> implements AbstractPath {

  @Nonnull
  private final FHIRDefinedType type;

  protected TypeSpecifier(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final FHIRDefinedType type) {
    super(dataset, idColumn, new StringType(type.toCode()), type.toCode());
    this.type = type;
    this.valueColumn = lit(type.toCode());
  }

  @Nonnull
  public static TypeSpecifier build(@Nonnull final FhirPath context,
      @Nonnull final FHIRDefinedType type) {
    return new TypeSpecifier(context.getDataset(), context.getIdColumn(), type);
  }


  @Nonnull
  @Override
  public FhirPath withDataset(@Nonnull final Dataset<Row> dataset) {
    return new TypeSpecifier(dataset, idColumn, type);
  }

  @Nonnull
  @Override
  public String getExpression() {
    return type.toCode();
  }

  @Nonnull
  @Override
  public Column buildValueColumn() {
    return lit(expression.orElse("[type specifier]"));
  }

}
