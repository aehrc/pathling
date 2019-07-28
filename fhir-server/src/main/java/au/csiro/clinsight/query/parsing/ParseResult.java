/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.parsing;

import au.csiro.clinsight.fhir.definitions.PathTraversal;
import java.util.SortedSet;
import java.util.TreeSet;
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
   * Information about the FHIR and FHIRPath types returned by this expression.
   */
  private ParseResultType resultType;

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

  public ParseResultType getResultType() {
    return resultType;
  }

  public void setResultType(@Nonnull ParseResultType resultType) {
    this.resultType = resultType;
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
   * Describes a ParseResult in terms of the FHIR and FHIRPath types that it evaluates to.
   */
  public enum ParseResultType {
    BOOLEAN(BooleanType.class, "Boolean"),
    STRING(StringType.class, "String"),
    INTEGER(IntegerType.class, "Integer"),
    // DECIMAL(DecimalType.class, "Decimal"),                               // Not currently supported
    // DATE(DateType.class, "Date"),                                        // Not currently supported
    DATE_TIME(DateTimeType.class, "DateTime"),
    // TIME(TimeType.class, "Time");                                        // Not currently supported
    CODING(Coding.class, "Coding");

    // Java class that can be used for representing the value of this expression.
    @Nonnull
    private final Class fhirType;

    // One of the data types defined in the FHIRPath specification.
    @Nonnull
    private final String fhirPathType;

    ParseResultType(@Nonnull Class fhirType, @Nonnull String fhirPathType) {
      this.fhirType = fhirType;
      this.fhirPathType = fhirPathType;
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

}
