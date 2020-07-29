/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.helpers;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;

/**
 * @author John Grimes
 */
public abstract class SparkHelpers {

  @Nonnull
  public static SparkSession getSparkSession() {
    @Nullable final Option<SparkSession> activeSession = SparkSession.getActiveSession();
    checkNotNull(activeSession);

    if (activeSession.isEmpty()) {
      return SparkSession.builder()
          .appName("pathling-test")
          .config("spark.master", "local[*]")
          .config("spark.driver.host", "localhost")
          .config("spark.sql.shuffle.partitions", "1")
          .getOrCreate();
    }
    return activeSession.get();
  }

  @Nonnull
  public static IdAndValueColumns getIdAndValueColumns(@Nonnull final Dataset<Row> dataset) {
    final Column idColumn = dataset.col("id");
    final Column valueColumn = dataset.col("value");
    return new IdAndValueColumns(idColumn, valueColumn);
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
    return new StructType(new StructField[]{id, system, version, code, display, userSelected});
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
            coding.getDisplay(), coding.getUserSelected()}, codingStructType());
  }

  @Nonnull
  public static Row rowFromCodeableConcept(@Nonnull final CodeableConcept codeableConcept) {
    final List<Coding> coding = codeableConcept.getCoding();
    checkNotNull(coding);

    final List<Row> codings = coding.stream().map(SparkHelpers::rowFromCoding)
        .collect(Collectors.toList());
    final Buffer<Row> buffer = JavaConversions.asScalaBuffer(codings);
    checkNotNull(buffer);

    return new GenericRowWithSchema(
        new Object[]{codeableConcept.getId(), buffer.toList(), codeableConcept.getText()},
        codeableConceptStructType());
  }

  @Value
  public static class IdAndValueColumns {

    @Nonnull
    Column id;

    @Nonnull
    Column value;

  }

}
