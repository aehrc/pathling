/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.parsing;

import au.csiro.clinsight.fhir.definitions.PathTraversal;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Used to represent the results from the parsing of a FHIRPath expression, including information
 * gathered to assist in the creation of a query plan that can be executed using Spark SQL.
 *
 * @author John Grimes
 */
public class ParsedExpression {

  static final SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat(
      "yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
  static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("'T'HH:mm:ss.SSSXXX");
  static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
      "yyyy-MM-dd");

  /**
   * The FHIRPath representation of this expression.
   */
  private String fhirPath;

  /**
   * Information about the FHIRPath type returned by this expression.
   */
  private FhirPathType fhirPathType;

  /**
   * Information about the FHIR type returned by this expression, if primitive.
   */
  private FhirType fhirType;

  /**
   * The result of executing the PathResolver over this expression.
   */
  private PathTraversal pathTraversal;

  /**
   * Flag indicating whether this expression evaluates to a collection of primitive values.
   */
  private boolean primitive;

  /**
   * Flag indicating whether this expression evaluates to a collection with a single item.
   */
  private boolean singular;

  /**
   * The literal value of this expression, if any.
   */
  private Type literalValue;

  /**
   * If this expression evaluates to a resource, this flag will be set.
   */
  private boolean isResource;

  /**
   * If this expression evaluates to a resource, this field holds the type of the resource.
   */
  private Enumerations.ResourceType resourceType;

  /**
   * If this expression is an unresolved polymorphic result, e.g. the result of a call to resolve()
   * on a reference with multiple types, this flag will be set.
   */
  private boolean polymorphic;

  /**
   * A reference to the ParseResult at the beginning of this expression. This is used for reverse
   * reference resolution, in order to get back to the origin resource within the argument. It only
   * needs to be populated at the start of expressions and within path traversal.
   */
  private ParsedExpression origin;

  /**
   * A Spark dataset that represents the query required to access the data described by this
   * expression.
   */
  private Dataset<Row> dataset;

  /**
   * The name of the column within the dataset that contains the result of this expression.
   */
  private String datasetColumn;

  /**
   * A Column which describes the aggregation associated with this expression, where there is one.
   */
  private Column aggregation;

  public ParsedExpression() {
  }

  public ParsedExpression(ParsedExpression parsedExpression) {
    this.fhirPath = parsedExpression.fhirPath;
    this.fhirPathType = parsedExpression.fhirPathType;
    this.fhirType = parsedExpression.fhirType;
    this.pathTraversal = parsedExpression.pathTraversal;
    this.primitive = parsedExpression.primitive;
    this.singular = parsedExpression.singular;
    this.literalValue = parsedExpression.literalValue;
    this.isResource = parsedExpression.isResource;
    this.resourceType = parsedExpression.resourceType;
    this.origin = parsedExpression.origin;
    this.dataset = parsedExpression.dataset;
    this.datasetColumn = parsedExpression.datasetColumn;
    this.aggregation = parsedExpression.aggregation;
  }

  public String getFhirPath() {
    return fhirPath;
  }

  public void setFhirPath(@Nonnull String fhirPath) {
    this.fhirPath = fhirPath;
  }

  public FhirPathType getFhirPathType() {
    return fhirPathType;
  }

  public void setFhirPathType(@Nonnull FhirPathType fhirPathType) {
    this.fhirPathType = fhirPathType;
  }

  public FhirType getFhirType() {
    return fhirType;
  }

  public void setFhirType(FhirType fhirType) {
    this.fhirType = fhirType;
  }

  public PathTraversal getPathTraversal() {
    return pathTraversal;
  }

  public void setPathTraversal(@Nonnull PathTraversal pathTraversal) {
    this.pathTraversal = pathTraversal;
  }

  public boolean isPrimitive() {
    return primitive;
  }

  public void setPrimitive(boolean primitive) {
    this.primitive = primitive;
  }

  public boolean isSingular() {
    return singular;
  }

  public void setSingular(boolean singular) {
    this.singular = singular;
  }

  public Type getLiteralValue() {
    return literalValue;
  }

  public void setLiteralValue(@Nonnull Type literalValue) {
    this.literalValue = literalValue;
  }

  public Dataset<Row> getDataset() {
    return dataset;
  }

  public void setDataset(Dataset<Row> dataset) {
    this.dataset = dataset;
  }

  public boolean isResource() {
    return isResource;
  }

  public void setResource(boolean resource) {
    isResource = resource;
  }

  public Enumerations.ResourceType getResourceType() {
    return resourceType;
  }

  public void setResourceType(Enumerations.ResourceType resourceType) {
    this.resourceType = resourceType;
  }

  public boolean isPolymorphic() {
    return polymorphic;
  }

  public void setPolymorphic(boolean polymorphic) {
    this.polymorphic = polymorphic;
  }

  public ParsedExpression getOrigin() {
    return origin;
  }

  public void setOrigin(ParsedExpression origin) {
    this.origin = origin;
  }

  public String getDatasetColumn() {
    return datasetColumn;
  }

  public void setDatasetColumn(String datasetColumn) {
    this.datasetColumn = datasetColumn;
  }

  public Column getAggregation() {
    return aggregation;
  }

  public void setAggregation(Column aggregation) {
    this.aggregation = aggregation;
  }

  public Object getJavaLiteralValue() {
    if (literalValue == null) {
      throw new IllegalStateException(
          "This method cannot be called on an expression that is not literal");
    }
    switch (fhirType) {
      case DECIMAL:
        return ((DecimalType) literalValue).getValue();
      case MARKDOWN:
        return ((MarkdownType) literalValue).getValue();
      case ID:
        return ((IdType) literalValue).getValue();
      case DATE_TIME:
        return DATE_TIME_FORMAT.format(((DateTimeType) literalValue).getValue());
      case TIME:
        return ((TimeType) literalValue).getValue();
      case DATE:
        return DATE_FORMAT.format(((DateType) literalValue).getValue());
      case CODE:
        return ((CodeType) literalValue).getValue();
      case STRING:
        return ((StringType) literalValue).getValue();
      case URI:
        return ((UriType) literalValue).getValue();
      case OID:
        return ((OidType) literalValue).getValue();
      case INTEGER:
        return ((IntegerType) literalValue).getValue();
      case UNSIGNED_INT:
        return new Long(((UnsignedIntType) literalValue).getValue());
      case POSITIVE_INT:
        return new Long(((PositiveIntType) literalValue).getValue());
      case BOOLEAN:
        return ((BooleanType) literalValue).getValue();
      case INSTANT:
        return ((InstantType) literalValue).getValue();
    }
    assert false : "Encountered FHIR type not accounted for";
    return null;
  }

  /**
   * Describes a ParseResult in terms of the FHIRPath type that it evaluates to.
   */
  public enum FhirPathType {
    BOOLEAN(BooleanType.class, "Boolean"),
    STRING(StringType.class, "String"),
    INTEGER(IntegerType.class, "Integer"),
    DECIMAL(DecimalType.class, "Decimal"),
    DATE(DateType.class, "Date"),
    DATE_TIME(DateTimeType.class, "DateTime"),
    TIME(TimeType.class, "Time"),
    QUANTITY(Quantity.class, "Quantity"),
    CODING(Coding.class, "Coding");

    private static final Map<FHIRDefinedType, FhirPathType> fhirTypeCodeToFhirPathType = new EnumMap<FHIRDefinedType, FhirPathType>(
        FHIRDefinedType.class) {{
      put(FHIRDefinedType.DECIMAL, DECIMAL);
      put(FHIRDefinedType.MARKDOWN, STRING);
      put(FHIRDefinedType.ID, STRING);
      put(FHIRDefinedType.DATETIME, DATE_TIME);
      put(FHIRDefinedType.TIME, TIME);
      put(FHIRDefinedType.DATE, DATE_TIME);
      put(FHIRDefinedType.CODE, STRING);
      put(FHIRDefinedType.STRING, STRING);
      put(FHIRDefinedType.URI, STRING);
      put(FHIRDefinedType.OID, STRING);
      put(FHIRDefinedType.INTEGER, INTEGER);
      put(FHIRDefinedType.UNSIGNEDINT, INTEGER);
      put(FHIRDefinedType.POSITIVEINT, INTEGER);
      put(FHIRDefinedType.BOOLEAN, BOOLEAN);
      put(FHIRDefinedType.INSTANT, DATE_TIME);
    }};

    // Java class that can be used for representing the value of this expression.
    @Nonnull
    private final Class fhirType;

    // One of the data types defined in the FHIRPath specification.
    @Nonnull
    private final String fhirPathType;

    FhirPathType(@Nonnull Class fhirType, @Nonnull String fhirPathType) {
      this.fhirType = fhirType;
      this.fhirPathType = fhirPathType;
    }

    // Maps a FHIR type code to a FHIRPath data type.
    public static FhirPathType forFhirTypeCode(FHIRDefinedType fhirTypeCode) {
      return fhirTypeCodeToFhirPathType.get(fhirTypeCode);
    }

    @Nonnull
    public Class getFhirType() {
      return fhirType;
    }

    @Nonnull
    public String getFhirPathType() {
      return fhirPathType;
    }
  }

  /**
   * Describes a ParseResult in terms of the FHIR type that it evaluates to. This may be different
   * to the FHIRPath type in some cases, e.g. the `count` function returns a FHIRPath Integer, but
   * an unsignedInt FHIR type.
   */
  public enum FhirType {
    DECIMAL(DecimalType.class, BigDecimal.class, "decimal"),
    MARKDOWN(MarkdownType.class, String.class, "markdown"),
    ID(IdType.class, String.class, "id"),
    DATE_TIME(DateTimeType.class, String.class, "datetime"),
    TIME(TimeType.class, String.class, "time"),
    DATE(DateType.class, String.class, "date"),
    CODE(CodeType.class, String.class, "code"),
    STRING(StringType.class, String.class, "string"),
    URI(UriType.class, String.class, "uri"),
    OID(OidType.class, String.class, "oid"),
    INTEGER(IntegerType.class, Integer.class, "integer"),
    UNSIGNED_INT(UnsignedIntType.class, Long.class, "unsignedInt"),
    POSITIVE_INT(PositiveIntType.class, Long.class, "positiveInt"),
    BOOLEAN(BooleanType.class, Boolean.class, "boolean"),
    INSTANT(InstantType.class, Date.class, "instant");

    private static final Map<FHIRDefinedType, FhirType> fhirTypeCodeToFhirType = new EnumMap<FHIRDefinedType, FhirType>(
        FHIRDefinedType.class) {{
      put(FHIRDefinedType.DECIMAL, DECIMAL);
      put(FHIRDefinedType.MARKDOWN, MARKDOWN);
      put(FHIRDefinedType.ID, ID);
      put(FHIRDefinedType.DATETIME, DATE_TIME);
      put(FHIRDefinedType.TIME, TIME);
      put(FHIRDefinedType.DATE, DATE);
      put(FHIRDefinedType.CODE, CODE);
      put(FHIRDefinedType.STRING, STRING);
      put(FHIRDefinedType.URI, URI);
      put(FHIRDefinedType.OID, OID);
      put(FHIRDefinedType.INTEGER, INTEGER);
      put(FHIRDefinedType.UNSIGNEDINT, UNSIGNED_INT);
      put(FHIRDefinedType.POSITIVEINT, POSITIVE_INT);
      put(FHIRDefinedType.BOOLEAN, BOOLEAN);
      put(FHIRDefinedType.INSTANT, INSTANT);
    }};

    // HAPI class that can be used for representing the value of this expression.
    @Nonnull
    private final Class hapiClass;

    // Java class that can be used for representing the value of this expression.
    @Nonnull
    private final Class javaClass;

    // One of the data types defined in the FHIRPath specification.
    @Nonnull
    private final String typeCode;

    FhirType(@Nonnull Class hapiClass, @Nonnull Class javaClass,
        @Nonnull String typeCode) {
      this.hapiClass = hapiClass;
      this.javaClass = javaClass;
      this.typeCode = typeCode;
    }

    // Maps a FHIR type code to a FHIR data type.
    public static FhirType forFhirTypeCode(FHIRDefinedType fhirTypeCode) {
      return fhirTypeCodeToFhirType.get(fhirTypeCode);
    }

    @Nonnull
    public Class getHapiClass() {
      return hapiClass;
    }

    @Nonnull
    public Class getJavaClass() {
      return javaClass;
    }

    @Nonnull
    public String getTypeCode() {
      return typeCode;
    }
  }

}
