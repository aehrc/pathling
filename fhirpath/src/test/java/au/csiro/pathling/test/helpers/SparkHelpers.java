/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test.helpers;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.spark.sql.functions.col;

import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import scala.collection.JavaConverters;
import scala.collection.mutable.Buffer;

/**
 * @author John Grimes
 */
@SuppressWarnings("WeakerAccess")
public abstract class SparkHelpers {

  @Nonnull
  public static IdAndValueColumns getIdAndValueColumns(@Nonnull final Dataset<Row> dataset) {
    return getIdAndValueColumns(dataset, false);
  }

  @Nonnull
  public static IdAndValueColumns getIdAndValueColumns(@Nonnull final Dataset<Row> dataset,
      final boolean hasEid) {
    int colIndex = 0;
    final Column idColumn = col(dataset.columns()[colIndex++]);
    final Optional<Column> eidColumn = hasEid
                                       ? Optional.of(col(dataset.columns()[colIndex++]))
                                       : Optional.empty();
    final Column valueColumn = col(dataset.columns()[colIndex]);
    return new IdAndValueColumns(idColumn, eidColumn, Collections.singletonList(valueColumn));
  }

  @Nonnull
  public static StructType codingStructType() {
    final Metadata metadata = new MetadataBuilder().build();
    final StructField id = new StructField("id", DataTypes.StringType, true, metadata);
    final StructField system = new StructField("system", DataTypes.StringType, true, metadata);
    final StructField version = new StructField("version", DataTypes.StringType, true, metadata);
    final StructField code = new StructField("code", DataTypes.StringType, true, metadata);
    final StructField display = new StructField("display", DataTypes.StringType, true, metadata);
    final StructField userSelected = new StructField("userSelected", DataTypes.BooleanType, true,
        metadata);
    final StructField fid = new StructField("_fid", DataTypes.IntegerType, true,
        metadata);
    return new StructType(new StructField[]{id, system, version, code, display, userSelected, fid});
  }

  @Nonnull
  public static StructType simpleCodingStructType() {
    final Metadata metadata = new MetadataBuilder().build();
    final StructField system = new StructField("system", DataTypes.StringType, true, metadata);
    final StructField version = new StructField("version", DataTypes.StringType, true, metadata);
    final StructField code = new StructField("code", DataTypes.StringType, true, metadata);
    return new StructType(new StructField[]{system, version, code});
  }

  @Nonnull
  public static StructType codeableConceptStructType() {
    final Metadata metadata = new MetadataBuilder().build();
    final StructField id = new StructField("id", DataTypes.StringType, true, metadata);
    final StructField coding = new StructField("coding",
        DataTypes.createArrayType(codingStructType()),
        true, metadata);
    final StructField text = new StructField("text", DataTypes.StringType, true, metadata);
    return new StructType(new StructField[]{id, coding, text});
  }

  @Nonnull
  public static StructType referenceStructType() {
    final Metadata metadata = new MetadataBuilder().build();
    final StructField id = new StructField("id", DataTypes.StringType, true, metadata);
    final StructField coding = new StructField("reference", DataTypes.StringType, true, metadata);
    final StructField text = new StructField("display", DataTypes.StringType, true, metadata);
    return new StructType(new StructField[]{id, coding, text});
  }

  @Nonnull
  public static Row rowFromCoding(@Nonnull final Coding coding) {
    return new GenericRowWithSchema(
        new Object[]{coding.getId(), coding.getSystem(), coding.getVersion(), coding.getCode(),
            coding.getDisplay(), coding.hasUserSelected()
                                 ? coding.getUserSelected()
                                 : null,
            null}, codingStructType());
  }


  @Nonnull
  public static Row rowFromCodeableConcept(@Nonnull final CodeableConcept codeableConcept) {
    final List<Coding> coding = codeableConcept.getCoding();
    requireNonNull(coding);

    final List<Row> codings = coding.stream().map(SparkHelpers::rowFromCoding)
        .collect(toList());
    final Buffer<Row> buffer = JavaConverters.asScalaBuffer(codings);
    requireNonNull(buffer);

    return new GenericRowWithSchema(
        new Object[]{codeableConcept.getId(), buffer.toList(), codeableConcept.getText()},
        codeableConceptStructType());
  }


  @Value
  public static class IdAndValueColumns {

    @Nonnull
    Column id;

    @Nonnull
    Optional<Column> eid;

    @Nonnull
    List<Column> values;

  }

}
