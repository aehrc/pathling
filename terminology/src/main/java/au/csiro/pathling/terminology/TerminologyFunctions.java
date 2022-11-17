/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.terminology;

import static au.csiro.pathling.utilities.Preconditions.wrapInUserInputError;

import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.fhirpath.encoding.SimpleCodingsDecoders;
import au.csiro.pathling.fhirpath.function.memberof.MemberOfMapper;
import au.csiro.pathling.fhirpath.function.subsumes.SubsumesMapper;
import au.csiro.pathling.fhirpath.function.translate.TranslateMapper;
import au.csiro.pathling.sql.MapperWithPreview;
import au.csiro.pathling.sql.SqlExtensions;
import au.csiro.pathling.sql.Terminology;
import au.csiro.pathling.utilities.Strings;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public interface TerminologyFunctions {

  Logger log = LoggerFactory.getLogger(TerminologyFunctions.class);

  @Nonnull
  Dataset<Row> memberOf(@Nonnull final Column codingArrayCol, @Nonnull final String valueSetUri,
      @Nonnull final Dataset<Row> dataset, @Nonnull final String outputColumnName);

  @Nonnull
  Dataset<Row> translate(@Nonnull final Column codingArrayCol, @Nonnull final String conceptMapUrl,
      final boolean reverse, @Nonnull final String equivalence, @Nonnull final Dataset<Row> dataset,
      @Nonnull final String outputColumnName);

  @Nonnull
  Dataset<Row> subsumes(@Nonnull final Dataset<Row> idAndCodingSet,
      @Nonnull final Column codingArrayA, @Nonnull final Column codingArrayB,
      @Nonnull final String outputColumnName, final boolean inverted);

  @Nonnull
  static TerminologyFunctions of(
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    // TODO: remove the legacy dependency at some point
    if ("legacy".equals(System.getenv("PATHLING_TERMINOLOGY"))) {
      log.warn("Using legacy implementation of terminology functions");
      return new TerminologyFunctionsLegacyImpl(terminologyServiceFactory);
    } else {
      return new TerminologyFunctionsImpl();
    }
  }
}

class TerminologyFunctionsLegacyImpl implements TerminologyFunctions {

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

  TerminologyFunctionsLegacyImpl(
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  @Override
  @Nonnull
  public Dataset<Row> memberOf(@Nonnull final Column codingArrayCol,
      @Nonnull final String valueSetUri, @Nonnull final Dataset<Row> dataset,
      @Nonnull final String outputColumnName) {

    final MapperWithPreview<List<SimpleCoding>, Boolean, Set<SimpleCoding>> mapper = new MemberOfMapper(
        getOrCreateRequestId(), terminologyServiceFactory, valueSetUri);

    return SqlExtensions.mapWithPartitionPreview(dataset, codingArrayCol,
        SimpleCodingsDecoders::decodeList, mapper,
        StructField.apply(outputColumnName, DataTypes.BooleanType, true, Metadata.empty()));
  }

  @Override
  @Nonnull
  public Dataset<Row> translate(@Nonnull final Column codingArrayCol,
      @Nonnull final String conceptMapUrl, final boolean reverse, @Nonnull final String equivalence,
      @Nonnull final Dataset<Row> dataset, @Nonnull final String outputColumnName) {

    final MapperWithPreview<List<SimpleCoding>, Row[], ConceptTranslator> mapper = new TranslateMapper(
        getOrCreateRequestId(), terminologyServiceFactory, conceptMapUrl, reverse,
        Strings.parseCsvList(equivalence, wrapInUserInputError(ConceptMapEquivalence::fromCode)));

    return SqlExtensions.mapWithPartitionPreview(dataset, codingArrayCol,
        SimpleCodingsDecoders::decodeList, mapper,
        StructField.apply(outputColumnName, DataTypes.createArrayType(CodingEncoding.DATA_TYPE),
            true, Metadata.empty()));
  }

  @Override
  @Nonnull
  public Dataset<Row> subsumes(@Nonnull final Dataset<Row> idAndCodingSet,
      @Nonnull final Column codingArrayA, @Nonnull final Column codingArrayB,
      @Nonnull final String outputColumnName, final boolean inverted) {

    final SubsumesMapper mapper = new SubsumesMapper(getOrCreateRequestId(),
        terminologyServiceFactory, inverted);

    return SqlExtensions.mapWithPartitionPreview(idAndCodingSet,
        functions.struct(codingArrayA, codingArrayB),
        SimpleCodingsDecoders::decodeListPair, mapper,
        StructField.apply(outputColumnName, DataTypes.BooleanType, true, Metadata.empty()));
  }

  @Nonnull
  static String getOrCreateRequestId() {
    String requestId = MDC.get("requestId");
    if (requestId == null) {
      requestId = UUID.randomUUID().toString();
      MDC.put("requestId", requestId);
    }
    return requestId;
  }
}


class TerminologyFunctionsImpl implements TerminologyFunctions {

  TerminologyFunctionsImpl() {
  }

  @Nonnull
  @Override
  public Dataset<Row> memberOf(@Nonnull final Column codingArrayCol,
      @Nonnull final String valueSetUri, @Nonnull final Dataset<Row> dataset,
      @Nonnull final String outputColumnName) {
    return dataset.withColumn(outputColumnName, Terminology.member_of(codingArrayCol, valueSetUri));
  }

  @Nonnull
  @Override
  public Dataset<Row> translate(@Nonnull final Column codingArrayCol,
      @Nonnull final String conceptMapUrl, final boolean reverse, @Nonnull final String equivalence,
      @Nonnull final Dataset<Row> dataset, @Nonnull final String outputColumnName) {
    return dataset.withColumn(outputColumnName,
        Terminology.translate(codingArrayCol, conceptMapUrl, reverse, equivalence));
  }

  @Override
  @Nonnull
  public Dataset<Row> subsumes(@Nonnull final Dataset<Row> idAndCodingSet,
      @Nonnull final Column codingArrayA, @Nonnull final Column codingArrayB
      , @Nonnull final String outputColumnName,
      final boolean inverted) {
    return idAndCodingSet.withColumn(outputColumnName,
        Terminology.subsumes(codingArrayA, codingArrayB, inverted));

  }
}
