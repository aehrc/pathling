/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.subsumes;

import static au.csiro.pathling.utilities.Preconditions.check;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.encoding.BooleanResult;
import au.csiro.pathling.fhirpath.encoding.IdAndCodingSets;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import ca.uhn.fhir.rest.param.UriParam;
import com.google.common.collect.Streams;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.CodeSystem;
import org.slf4j.MDC;


/**
 * Takes a set of Rows with schema: STRING id, ARRAY(CODING) inputCoding, ARRAY(CODING) argCodings
 * to check for a subsumption relation with a terminology server.
 * <p>
 * Returns a set of {@link BooleanResult} objects, which contain the identified and the status of
 * subsumption relation for each of input elements.
 */
@Slf4j
public class SubsumptionMapper implements MapPartitionsFunction<Row, Row> {

  private static final long serialVersionUID = 1L;

  @Nonnull
  private final String requestId;

  @Nonnull
  private final TerminologyClientFactory terminologyClientFactory;
  private final boolean inverted;

  /**
   * Constructor
   *
   * @param requestId An identifier used alongside any logging that the mapper outputs
   * @param terminologyClientFactory the factory to use to create the {@link
   * au.csiro.pathling.fhir.TerminologyClient}
   * @param inverted if true checks for `subsumedBy` relation otherwise for `subsumes`
   */
  public SubsumptionMapper(@Nonnull final String requestId,
      @Nonnull final TerminologyClientFactory terminologyClientFactory,
      final boolean inverted) {
    this.requestId = requestId;
    this.terminologyClientFactory = terminologyClientFactory;
    this.inverted = inverted;
  }

  /**
   * Maps an object from a Row into a SimpleCoding.
   */
  @Nonnull
  private static SimpleCoding valueToSimpleCoding(@Nonnull final Object value) {
    final Row simpleCodingRow = (Row) value;
    final int systemIndex = simpleCodingRow.fieldIndex("system");
    final int versionIndex = simpleCodingRow.fieldIndex("version");
    final int codeIndex = simpleCodingRow.fieldIndex("code");
    final SimpleCoding simpleCoding = new SimpleCoding();
    simpleCoding.setSystem(simpleCodingRow.getString(systemIndex));
    simpleCoding.setVersion(simpleCodingRow.getString(versionIndex));
    simpleCoding.setCode(simpleCodingRow.getString(codeIndex));
    return simpleCoding;
  }

  /**
   * Takes a Row as input, and returns a new Row with an additional field containing the supplied
   * Boolean result.
   */
  @Nonnull
  private static Row addResultToRow(@Nonnull final Row row, @Nullable final Boolean result) {
    final StructType previousSchema = row.schema();
    final List<String> fieldNames = Arrays.stream(previousSchema.fields())
        .map(StructField::name)
        .collect(Collectors.toList());
    final List<Object> values = fieldNames.stream()
        .map(fieldName -> row.get(row.fieldIndex(fieldName)))
        .collect(Collectors.toList());
    values.add(result);
    final StructType newSchema = createResultSchema(previousSchema);
    return new GenericRowWithSchema(values.toArray(new Object[0]), newSchema);
  }

  /**
   * Take an existing {@link StructType} and return a new schema, adding a new Boolean value column
   * to it.
   *
   * @param previousSchema the StructType on which to base the new schema
   * @return a new StructType with an additional Boolean column
   */
  @Nonnull
  public static StructType createResultSchema(@Nonnull final StructType previousSchema) {
    check(previousSchema.fields().length > 0);

    final Metadata metadata = previousSchema.fields()[0].metadata();
    final StructField[] newFields = Arrays.copyOf(
        previousSchema.fields(), previousSchema.fields().length + 1);

    final StructField valueField = new StructField(SubsumesFunction.COL_VALUE,
        DataTypes.BooleanType, true,
        metadata);
    newFields[previousSchema.fields().length] = valueField;

    return new StructType(newFields);
  }

  @Override
  public Iterator<Row> call(@Nonnull final Iterator<Row> input) {
    final Iterable<Row> inputRowsIterable = () -> input;
    final Stream<Row> inputStream = StreamSupport
        .stream(inputRowsIterable.spliterator(), false);

    // Convert the input rows into a list of IdAndCodingSets objects.
    final List<Row> inputRows = inputStream
        .collect(Collectors.toList());
    final List<IdAndCodingSets> entries = inputRows.stream()
        .map(row -> {
          final int idIndex = row.fieldIndex(SubsumesFunction.COL_ID);
          final int inputCodingsIndex = row.fieldIndex(SubsumesFunction.COL_INPUT_CODINGS);
          final int argCodingsIndex = row.fieldIndex(SubsumesFunction.COL_ARG_CODINGS);

          final List<SimpleCoding> inputCodings = row.isNullAt(inputCodingsIndex)
                                                  ? null
                                                  : row.getList(inputCodingsIndex).stream()
                                                      .map(SubsumptionMapper::valueToSimpleCoding)
                                                      .collect(Collectors.toList());
          final List<SimpleCoding> argCodings = row.isNullAt(argCodingsIndex)
                                                ? null
                                                : row.getList(argCodingsIndex).stream()
                                                    .map(SubsumptionMapper::valueToSimpleCoding)
                                                    .collect(Collectors.toList());

          final IdAndCodingSets result = new IdAndCodingSets();
          result.setId(row.getString(idIndex));
          result.setInputCodings(inputCodings);
          result.setArgCodings(argCodings);
          return result;
        })
        .collect(Collectors.toList());

    // Add the request ID to the logging context, so that we can track the logging for this
    // request across all workers.
    MDC.put("requestId", requestId);

    // Collect all distinct tokens used on both in inputs and arguments in this partition
    // Rows in which either input or argument are NULL are excluded as they do not need
    // to be included in closure request.
    // Also we only include codings with both system and code defined as only they can
    // be expected to be meaningfully represented to the terminology server

    final Set<SimpleCoding> allCodings = entries.stream()
        .filter(r -> r.getInputCodings() != null && r.getArgCodings() != null)
        .flatMap(r -> Streams.concat(r.getInputCodings().stream(), r.getArgCodings().stream()))
        .filter(SimpleCoding::isDefined)
        .collect(Collectors.toSet());

    final TerminologyClient terminologyClient = terminologyClientFactory.build(log);

    // filter out codings with code systems unknown to the terminology server
    final Set<String> allCodeSystems = allCodings.stream()
        .map(SimpleCoding::getSystem)
        .collect(Collectors.toSet());

    final Set<String> knownCodeSystems = allCodeSystems.stream().filter(codeSystem -> {
      final UriParam uri = new UriParam(codeSystem);
      final List<CodeSystem> knownSystems = terminologyClient.searchCodeSystems(
          uri, new HashSet<>(Collections.singletonList("id")));
      return !knownSystems.isEmpty();
    }).collect(Collectors.toSet());

    if (!knownCodeSystems.equals(allCodeSystems)) {
      final Collection<String> unrecognizedCodeSystems = new HashSet<>(allCodeSystems);
      unrecognizedCodeSystems.removeAll(knownCodeSystems);
      log.warn("Terminology server does not recognize these coding systems: {}",
          unrecognizedCodeSystems);
    }

    final Set<SimpleCoding> knownCodings = allCodings.stream()
        .filter(coding -> knownCodeSystems.contains(coding.getSystem()))
        .collect(Collectors.toSet());

    final ClosureService closureService = new ClosureService(terminologyClient);
    final Closure subsumeClosure = closureService.getSubsumesRelation(knownCodings);

    // Create a new Row with all the fields of the input row, plus an additional Boolean field
    // containing the result.
    final Collection<Row> results = new ArrayList<>();
    for (int i = 0; i < inputRows.size(); i++) {
      final Row row = inputRows.get(i);
      final IdAndCodingSets entry = entries.get(i);

      if (entry.getInputCodings() == null) {
        results.add(addResultToRow(row, null));
      } else {
        // The result is determined by looking for relationships in the closure returned by the
        // terminology server.
        final boolean result = (!inverted
                                ? subsumeClosure
                                    .anyRelates(entry.safeGetInputCodings(),
                                        entry.safeGetArgCodings())
                                :
                                subsumeClosure
                                    .anyRelates(entry.safeGetArgCodings(),
                                        entry.safeGetInputCodings()));
        results.add(addResultToRow(row, result));
      }
    }
    return results.iterator();
  }

}