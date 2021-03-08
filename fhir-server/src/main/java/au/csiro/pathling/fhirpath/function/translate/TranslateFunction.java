/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.translate;

import static au.csiro.pathling.fhirpath.TerminologyUtils.isCodeableConcept;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TerminologyUtils;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.fhirpath.encoding.SimpleCodingsDecoders;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.literal.BooleanLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.sql.SqlExtensions;
import au.csiro.pathling.utilities.Strings;
import java.util.List;
import java.util.Optional;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.slf4j.MDC;

/**
 * A function that takes a set of Codings or CodeableConcepts as inputs and returns a set Codings
 * translated using provided concept map URL.
 * <p>
 * Sinature:
 * <pre>
 * collection<Coding|CodeableConcept> -> translate(conceptMapUrl: string, reverse = false,
 * equivalence = 'equivalent') : collection<Coding>
 * </pre>
 * <p>
 * Uses: <a href="https://www.hl7.org/fhir/operation-conceptmap-translate.html">Translate
 * Operation</a>
 *
 * @author Piotr Szul
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#translate">translate</a>
 */
public class TranslateFunction implements NamedFunction {

  private static final String NAME = "translate";

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    validateInput(input);

    final ElementPath inputPath = (ElementPath) input.getInput();
    final StringLiteralPath conceptMapUrlArg = (StringLiteralPath) input.getArguments().get(0);
    final BooleanLiteralPath reverseArg = (BooleanLiteralPath) input.getArguments().get(1);
    final StringLiteralPath equivalenceArg = (StringLiteralPath) input.getArguments().get(2);

    final ParserContext inputContext = input.getContext();
    final Column idColumn = inputPath.getIdColumn();
    final Column conceptColumn = inputPath.getValueColumn();

    final boolean isCodeableConcept = isCodeableConcept(inputPath);

    CheckReturnValue functions;
    final Column codingArrayCol = isCodeableConcept
                                  ? conceptColumn.getField("coding")
                                  : when(conceptColumn.isNotNull(), array(conceptColumn))
                                      .otherwise(lit(null));

    // the the definition of the result is always the Coding element.
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    final ElementDefinition resultDefinition = isCodeableConcept
                                               ? inputPath.getChildElement("coding").get()
                                               : inputPath.getDefinition().get();

    // Prepare the data which will be used within the map operation. All of these things must be
    // Serializable.
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    final TerminologyClientFactory terminologyClientFactory = inputContext
        .getTerminologyClientFactory().get();
    final String conceptMapUrl = conceptMapUrlArg.getJavaValue();
    final boolean reverse = reverseArg.getJavaValue();
    final String equivalence = equivalenceArg.getJavaValue();
    final Dataset<Row> dataset = inputPath.getDataset();

    final TranslatefMapperWithPreview mapper =
        new TranslatefMapperWithPreview(MDC.get("requestId"), terminologyClientFactory,
            conceptMapUrl, reverse, Strings.parseCsvList(equivalence,
            ConceptMapEquivalence::fromCode));

    final Dataset<Row> translatedDataset = SqlExtensions
        .mapWithPartitionPreview(dataset, codingArrayCol,
            SimpleCodingsDecoders::decodeList,
            mapper,
            StructField.apply("result", DataTypes.createArrayType(CodingEncoding.DATA_TYPE), true,
                Metadata.empty()));

    // the result is an array of translations per each input element, which we now
    // need to explode in the same way as for path traversal, creating unique element ids.
    final MutablePair<Column, Column> valueAndEidColumns = new MutablePair<>();
    final Dataset<Row> resultDataset = inputPath
        .explodeArray(translatedDataset, translatedDataset.col("result"), valueAndEidColumns);
    // Construct a new result expression.
    final String expression = expressionFromInput(input, NAME);

    return ElementPath
        .build(expression, resultDataset, idColumn, Optional.of(valueAndEidColumns.getRight()),
            valueAndEidColumns.getLeft(),
            false, inputPath.getForeignResource(), inputPath.getThisColumn(),
            resultDefinition);
  }

  private void validateInput(@Nonnull final NamedFunctionInput input) {
    final ParserContext context = input.getContext();
    checkUserInput(
        context.getTerminologyClientFactory()
            .isPresent(), "Attempt to call terminology function " + NAME
            + " when terminology service has not been configured");

    final FhirPath inputPath = input.getInput();
    checkUserInput(TerminologyUtils.isCodingOrCodeableConcept(inputPath),
        String.format("Input to %s function is of unsupported type: %s", NAME,
            inputPath.getExpression()));
    final List<FhirPath> arguments = input.getArguments();
    checkUserInput(arguments.size() == 3,
        NAME + " function accepts 3 arguments");
    checkUserInput(arguments.get(0) instanceof StringLiteralPath,
        String.format("Function `%s` expects `%s` as argument %s", NAME, "String literal", 1));
    checkUserInput(arguments.get(1) instanceof BooleanLiteralPath,
        String.format("Function `%s` expects `%s` as argument %s", NAME, "String literal", 2));
    checkUserInput(arguments.get(2) instanceof StringLiteralPath,
        String.format("Function `%s` expects `%s` as argument %s", NAME, "String literal", 3));
  }
}
