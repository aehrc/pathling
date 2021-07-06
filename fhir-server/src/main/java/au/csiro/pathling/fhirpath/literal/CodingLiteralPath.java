/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Strings.unSingleQuote;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.element.CodingPath;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Type;

/**
 * Represents a FHIRPath Coding literal.
 *
 * @author John Grimes
 */
@Getter
public class CodingLiteralPath extends LiteralPath implements Materializable<Coding>, Comparable {

  private static final Pattern CODING_PATTERN = Pattern
      .compile("('.*'|[^ '|\\r\\n\\t(),]+)(\\|('.*'|[^ '|\\r\\n\\t(),]+)){0,2}?");
  private static final Pattern NEEDS_QUOTING = Pattern.compile("[ '|\\r\\n\\t(),]");
  private static final Pattern NEEDS_UNQUOTING = Pattern.compile("^'(.*)'$");

  @SuppressWarnings("WeakerAccess")
  protected CodingLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Type literalValue) {
    super(dataset, idColumn, literalValue);
    check(literalValue instanceof Coding);
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param fhirPath The FHIRPath representation of the literal
   * @param context An input context that can be used to build a {@link Dataset} to represent the
   * literal
   * @return A new instance of {@link LiteralPath}
   * @throws IllegalArgumentException if the literal is malformed
   */
  @Nonnull
  public static CodingLiteralPath fromString(@Nonnull final CharSequence fhirPath,
      @Nonnull final FhirPath context) throws IllegalArgumentException {
    final Matcher matcher = CODING_PATTERN.matcher(fhirPath);
    final List<String> codingTokens = matcher.results()
        .map(MatchResult::group)
        .collect(Collectors.toList());
    final Coding coding;
    if (codingTokens.size() == 2) {
      coding = new Coding(decodeComponent(codingTokens.get(0)),
          decodeComponent(codingTokens.get(1)), null);
    } else if (codingTokens.size() == 3) {
      coding = new Coding(decodeComponent(codingTokens.get(0)),
          decodeComponent(codingTokens.get(2)), null);
      coding.setVersion(decodeComponent(codingTokens.get(1)));
    } else {
      throw new IllegalArgumentException(
          "Coding literal must be of form [system]|[code] or [system]|[version]|[code]");
    }
    return new CodingLiteralPath(context.getDataset(), context.getIdColumn(), coding);
  }

  @Nonnull
  private static String decodeComponent(@Nonnull final String component) {
    final Matcher matcher = NEEDS_UNQUOTING.matcher(component);
    if (matcher.matches()) {
      final String result = unSingleQuote(component);
      return StringLiteralPath.unescapeFhirPathString(result);
    } else {
      return component;
    }
  }

  @Nonnull
  private static String encodeComponent(@Nonnull final String component) {
    final Matcher matcher = NEEDS_QUOTING.matcher(component);
    if (matcher.find()) {
      final String result = StringLiteralPath.escapeFhirPathString(component);
      return "'" + result + "'";
    } else {
      return component;
    }
  }

  @Nonnull
  @Override
  public String getExpression() {
    final String system = getLiteralValue().getSystem();
    final String version = getLiteralValue().getVersion();
    final String code = getLiteralValue().getCode();
    return version == null
           ? encodeComponent(system) + "|" + encodeComponent(code)
           : encodeComponent(system) + "|" + encodeComponent(version) + "|" + encodeComponent(code);
  }

  @Override
  public Coding getLiteralValue() {
    return (Coding) literalValue;
  }

  @Nonnull
  @Override
  public Coding getJavaValue() {
    return getLiteralValue();
  }

  @Nonnull
  @Override
  public Column buildValueColumn() {
    final Coding value = getJavaValue();
    return struct(
        lit(value.getId()).as("id"),
        lit(value.getSystem()).as("system"),
        lit(value.getVersion()).as("version"),
        lit(value.getCode()).as("code"),
        lit(value.getDisplay()).as("display"),
        lit(value.getUserSelected()).as("userSelected"));
  }

  @Override
  @Nonnull
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return CodingPath.buildComparison(this, operation);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return CodingPath.getComparableTypes().contains(type);
  }

  @Nonnull
  @Override
  public Optional<Coding> getValueFromRow(@Nonnull final Row row, final int columnNumber) {
    return CodingPath.valueFromRow(row, columnNumber);
  }

  @Override
  public boolean canBeCombinedWith(@Nonnull final FhirPath target) {
    return super.canBeCombinedWith(target) || target instanceof CodingPath;
  }
 
}
