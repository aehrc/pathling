/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.parsing;

import au.csiro.clinsight.fhir.definitions.PathTraversal;
import java.math.BigDecimal;
import java.util.*;
import javax.annotation.Nonnull;
import org.hl7.fhir.dstu3.model.*;

/**
 * Used to represent the results from the parsing of a FHIRPath expression, including information
 * gathered to assist in the creation of a query plan that can be executed using Spark SQL.
 *
 * @author John Grimes
 */
public class ParseResult {

  /**
   * The set of joins required to execute this expression.
   */
  private final SortedSet<Join> joins = new TreeSet<>();

  /**
   * The FHIRPath representation of this expression.
   */
  private String fhirPath;

  /**
   * The SQL representation of this expression
   */
  private String sql;

  /**
   * Information about the FHIRPath type returned by this expression.
   */
  private FhirPathType fhirPathType;

  /**
   * Information about the FHIR type returned by this expression.
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

  @Nonnull
  public SortedSet<Join> getJoins() {
    return joins;
  }

  public String getFhirPath() {
    return fhirPath;
  }

  public void setFhirPath(@Nonnull String fhirPath) {
    this.fhirPath = fhirPath;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(@Nonnull String sql) {
    this.sql = sql;
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

  /**
   * Describes a ParseResult in terms of the FHIRPath type that it evaluates to.
   */
  public enum FhirPathType {
    BOOLEAN(BooleanType.class, "Boolean"),
    STRING(StringType.class, "String"),
    INTEGER(IntegerType.class, "Integer"),
    // DECIMAL(DecimalType.class, "Decimal"),                               // Not currently supported
    // DATE(DateType.class, "Date"),                                        // Not currently supported
    DATE_TIME(DateTimeType.class, "DateTime"),
    // TIME(TimeType.class, "Time");                                        // Not currently supported
    CODING(Coding.class, "Coding");

    private static final Map<String, FhirPathType> fhirTypeCodeToFhirPathType = new HashMap<String, FhirPathType>() {{
      // put("decimal", DECIMAL);  // Decimal not yet supported
      put("markdown", STRING);
      put("id", STRING);
      put("dateTime", DATE_TIME);
      // put("time", TIME);  // Time not yet supported
      put("date", DATE_TIME);
      put("code", STRING);
      put("string", STRING);
      put("uri", STRING);
      put("oid", STRING);
      put("integer", INTEGER);
      put("unsignedInt", INTEGER);
      put("positiveInt", INTEGER);
      put("boolean", BOOLEAN);
      put("instant", DATE_TIME);
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
    public static FhirPathType forFhirTypeCode(String fhirTypeCode) {
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
    INTEGER(IntegerType.class, int.class, "integer"),
    UNSIGNED_INT(UnsignedIntType.class, Long.class, "unsignedInt"),
    POSITIVE_INT(PositiveIntType.class, Long.class, "positiveInt"),
    BOOLEAN(BooleanType.class, Boolean.class, "boolean"),
    INSTANT(InstantType.class, Date.class, "instant");

    private static final Map<String, FhirType> fhirTypeCodeToFhirType = new HashMap<String, FhirType>() {{
      put("decimal", DECIMAL);
      put("markdown", MARKDOWN);
      put("id", ID);
      put("dateTime", DATE_TIME);
      put("time", TIME);
      put("date", DATE);
      put("code", CODE);
      put("string", STRING);
      put("uri", URI);
      put("oid", OID);
      put("integer", INTEGER);
      put("unsignedInt", UNSIGNED_INT);
      put("positiveInt", POSITIVE_INT);
      put("boolean", BOOLEAN);
      put("instant", INSTANT);
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
    public static FhirType forFhirTypeCode(String fhirTypeCode) {
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
