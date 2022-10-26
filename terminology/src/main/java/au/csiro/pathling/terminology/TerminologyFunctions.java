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

import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.fhirpath.encoding.SimpleCodingsDecoders;
import au.csiro.pathling.fhirpath.function.memberof.MemberOfMapper;
import au.csiro.pathling.fhirpath.function.subsumes.SubsumesMapper;
import au.csiro.pathling.fhirpath.function.translate.TranslateMapper;
import au.csiro.pathling.sql.MapperWithPreview;
import au.csiro.pathling.sql.SqlExtensions;
import au.csiro.pathling.utilities.Strings;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;

public interface TerminologyFunctions {

  @Nonnull
  static Dataset<Row> memberOf(@Nonnull final Column codingArrayCol,
      @Nonnull final String valueSetUri, @Nonnull final Dataset<Row> dataset,
      @Nonnull final String outputColumnName,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory,
      @Nonnull final String requestId) {

    final MapperWithPreview<List<SimpleCoding>, Boolean, Set<SimpleCoding>> mapper =
        new MemberOfMapper(requestId, terminologyServiceFactory,
            valueSetUri);

    return SqlExtensions
        .mapWithPartitionPreview(dataset, codingArrayCol,
            SimpleCodingsDecoders::decodeList,
            mapper,
            StructField.apply(outputColumnName, DataTypes.BooleanType, true, Metadata.empty()));
  }

  @Nonnull
  static Dataset<Row> translate(@Nonnull final Column codingArrayCol,
      @Nonnull final String conceptMapUrl, final boolean reverse, @Nonnull final String equivalence,
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String outputColumnName,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory,
      @Nonnull final String requestId) {

    final MapperWithPreview<List<SimpleCoding>, Row[], ConceptTranslator> mapper =
        new TranslateMapper(requestId, terminologyServiceFactory,
            conceptMapUrl, reverse, Strings.parseCsvList(equivalence,
            wrapInUserInputError(ConceptMapEquivalence::fromCode)));

    return SqlExtensions
        .mapWithPartitionPreview(dataset, codingArrayCol,
            SimpleCodingsDecoders::decodeList,
            mapper,
            StructField.apply(outputColumnName, DataTypes.createArrayType(CodingEncoding.DATA_TYPE),
                true, Metadata.empty()));
  }

  @Nonnull
  static Dataset<Row> subsumes(@Nonnull final Dataset<Row> idAndCodingSet,
      @Nonnull final Column codingPairCol, @Nonnull final String outputColumnName,
      final boolean inverted, @Nonnull final TerminologyServiceFactory terminologyServiceFactory,
      @Nonnull final String requestId) {

    final SubsumesMapper mapper =
        new SubsumesMapper(requestId,
            terminologyServiceFactory,
            inverted);

    return SqlExtensions
        .mapWithPartitionPreview(idAndCodingSet, codingPairCol,
            SimpleCodingsDecoders::decodeListPair,
            mapper,
            StructField.apply(outputColumnName, DataTypes.BooleanType, true, Metadata.empty()));
  }

}
