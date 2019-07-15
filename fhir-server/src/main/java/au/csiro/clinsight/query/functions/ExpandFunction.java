/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.COMPLEX;
import static au.csiro.clinsight.query.parsing.ParseResult.ParseResultType.COLLECTION;
import static au.csiro.clinsight.query.parsing.ParseResult.ParseResultType.STRING;

import au.csiro.clinsight.query.Code;
import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.ParseResult;
import au.csiro.clinsight.utilities.Strings;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.hl7.fhir.dstu3.model.UriType;
import org.hl7.fhir.dstu3.model.ValueSet;

/**
 * A function that takes a set of Codings as inputs and returns a set of boolean values, based upon
 * whether each Coding is present within the ValueSet identified by the supplied URL.
 *
 * @author John Grimes
 */
public class ExpandFunction implements ExpressionFunction {

  private ExpressionParserContext context;

  @Nonnull
  @Override
  public ParseResult invoke(@Nullable ParseResult input, @Nonnull List<ParseResult> arguments) {
    ParseResult argument = validateArgument(arguments);

    assert argument.getFhirPath() != null;
    String unquotedArgument = argument.getFhirPath()
        .substring(1, argument.getFhirPath().length() - 1);
    // Get a shortened hash of the ValueSet URL for use as part of the temporary view name, and also
    // within the alias in the query. This helps us ensure uniqueness of the names we use.
    String argumentHash = Strings.md5(unquotedArgument).substring(0, 7);
    String tableName = "valueSet_" + argumentHash;

    // Create a table containing the results of the expanded ValueSet, if it doesn't already exist.
    ensureExpansionTableExists(unquotedArgument, tableName);

    // Build a new parse result, representing the results of the ValueSet expansion.
    ParseResult result = new ParseResult();
    result.setFhirPath("expand(" + argument.getFhirPath() + ")");
    result.setSql(tableName + ".code");
    result.setResultType(COLLECTION);
    result.setElementType(COMPLEX);
    result.setElementTypeCode("Coding");
    return result;
  }

  @Nonnull
  private ParseResult validateArgument(@Nonnull List<ParseResult> arguments) {
    if (arguments.size() != 1) {
      throw new InvalidRequestException("Must pass URL argument to expand function");
    }
    ParseResult argument = arguments.get(0);
    if (argument.getResultType() != STRING) {
      throw new InvalidRequestException(
          "Argument to expand function must be a String: " + argument.getFhirPath());
    }
    return argument;
  }

  /**
   * Expands the specified ValueSet using the terminology server, and saves the result to a
   * temporary view identified by the specified table name.
   */
  private void ensureExpansionTableExists(String valueSetUrl, String tableName) {
    if (!spark.catalog().tableExists(databaseName, tableName)) {
      ValueSet expansion = terminologyClient.expandValueSet(new UriType(valueSetUrl));
      List<Code> expansionRows = expansion.getExpansion().getContains().stream()
          .map(contains -> new Code(contains.getSystem(), contains.getCode()))
          .collect(Collectors.toList());
      Dataset<Code> expansionDataset = spark
          .createDataset(expansionRows, Encoders.bean(Code.class));
      expansionDataset.createOrReplaceTempView(tableName);
    }
  }

  @Override
  public void setContext(@Nonnull ExpressionParserContext context) {
    this.context = context;
  }

}
