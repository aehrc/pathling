/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.encoding;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.spark.sql.functions.lit;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Coding;

/** A scheme for representing {@link Coding} objects within Spark SQL. */
public interface CodingSchema {

  /** Field name for the id property. */
  String ID_FIELD = "id";

  /** Field name for the system property. */
  String SYSTEM_FIELD = "system";

  /** Field name for the version property. */
  String VERSION_FIELD = "version";

  /** Field name for the code property. */
  String CODE_FIELD = "code";

  /** Field name for the display property. */
  String DISPLAY_FIELD = "display";

  /** Field name for the userSelected property. */
  String USER_SELECTED_FIELD = "userSelected";

  /** Field name for the fid property. */
  String FID_FIELD = "_fid";

  /**
   * @return a {@link StructType} that can house a Coding
   */
  @Nonnull
  static StructType codingStructType() {
    final Metadata metadata = new MetadataBuilder().build();
    final StructField id = new StructField(ID_FIELD, DataTypes.StringType, true, metadata);
    final StructField system = new StructField(SYSTEM_FIELD, DataTypes.StringType, true, metadata);
    final StructField version =
        new StructField(VERSION_FIELD, DataTypes.StringType, true, metadata);
    final StructField code = new StructField(CODE_FIELD, DataTypes.StringType, true, metadata);
    final StructField display =
        new StructField(DISPLAY_FIELD, DataTypes.StringType, true, metadata);
    final StructField userSelected =
        new StructField(USER_SELECTED_FIELD, DataTypes.BooleanType, true, metadata);
    final StructField fid = new StructField(FID_FIELD, DataTypes.IntegerType, true, metadata);
    return new StructType(
        new StructField[] {id, system, version, code, display, userSelected, fid});
  }

  /** A {@link StructType} for a Coding. */
  StructType DATA_TYPE = codingStructType();

  /** Index of the system field in the Coding struct. */
  int SYSTEM_INDEX = DATA_TYPE.fieldIndex(SYSTEM_FIELD);

  /** Index of the version field in the Coding struct. */
  int VERSION_INDEX = DATA_TYPE.fieldIndex(VERSION_FIELD);

  /** Index of the code field in the Coding struct. */
  int CODE_INDEX = DATA_TYPE.fieldIndex(CODE_FIELD);

  /** Index of the display field in the Coding struct. */
  int DISPLAY_INDEX = DATA_TYPE.fieldIndex(DISPLAY_FIELD);

  /** Index of the userSelected field in the Coding struct. */
  int USER_SELECTED_INDEX = DATA_TYPE.fieldIndex(USER_SELECTED_FIELD);

  /**
   * Encodes a Coding to a Row (spark SQL compatible type)
   *
   * @param coding a coding to encode
   * @return the Row representation of the coding
   */
  @Nullable
  static Row encode(@Nullable final Coding coding) {
    if (coding == null) {
      return null;
    } else {
      final Boolean userSelected = coding.hasUserSelected() ? coding.getUserSelected() : null;
      return RowFactory.create(
          coding.getId(),
          coding.getSystem(),
          coding.getVersion(),
          coding.getCode(),
          coding.getDisplay(),
          userSelected,
          null /* _fid */);
    }
  }

  /**
   * Decodes a Coding from a Row.
   *
   * @param row the row to decode
   * @return the resulting Coding
   */
  @Nullable
  static Coding decode(@Nullable final Row row) {
    if (isNull(row)) {
      return null;
    }
    final Coding coding = new Coding();
    coding.setSystem(row.getString(SYSTEM_INDEX));
    coding.setVersion(row.getString(VERSION_INDEX));
    coding.setCode(row.getString(CODE_INDEX));
    coding.setDisplay(row.getString(DISPLAY_INDEX));
    if (!row.isNullAt(USER_SELECTED_INDEX)) {
      coding.setUserSelected(row.getBoolean(USER_SELECTED_INDEX));
    }
    return coding;
  }

  /**
   * Encodes a list of Codings to a list of {@link Row} objects
   *
   * @param codings a list of codings to encode
   * @return a list of the Row representations of the coding
   */
  @Nullable
  static List<Row> encodeList(@Nullable final List<Coding> codings) {
    return codings == null ? null : codings.stream().map(CodingSchema::encode).toList();
  }

  /**
   * Encodes a list of Codings to an array of {@link Row} objects
   *
   * @param codings a list of codings to encode
   * @return an array of the Row representations of the coding
   */
  @Nullable
  static Row[] encodeListToArray(@Nullable final List<Coding> codings) {
    final List<Row> encoded = encodeList(codings);
    return encoded == null ? null : encoded.toArray(new Row[0]);
  }

  /**
   * Creates a Coding struct column from individual field columns.
   *
   * @param id the id column
   * @param system the system column
   * @param version the version column
   * @param code the code column
   * @param display the display column
   * @param userSelected the userSelected column
   * @return a struct column representing a Coding
   */
  @Nonnull
  static Column toStruct(
      @Nonnull final Column id,
      @Nonnull final Column system,
      @Nonnull final Column version,
      @Nonnull final Column code,
      @Nonnull final Column display,
      @Nonnull final Column userSelected) {
    return functions.struct(
        id.as(ID_FIELD),
        system.as(SYSTEM_FIELD),
        version.as(VERSION_FIELD),
        code.as(CODE_FIELD),
        display.as(DISPLAY_FIELD),
        userSelected.as(USER_SELECTED_FIELD),
        lit(null).as(FID_FIELD));
  }

  @Nonnull
  private static Column userSelectedToLiteral(@Nonnull final Coding coding) {
    return coding.hasUserSelected() ? lit(coding.getUserSelected()) : lit(null);
  }

  /**
   * Encodes the coding as a literal column.
   *
   * @param coding the Coding to encode.
   * @return the column with the literal representation of the coding.
   */
  @Nonnull
  static Column toLiteralColumn(@Nullable final Coding coding) {
    return nonNull(coding)
        ? toStruct(
            lit(coding.getId()),
            lit(coding.getSystem()),
            lit(coding.getVersion()),
            lit(coding.getCode()),
            lit(coding.getDisplay()),
            userSelectedToLiteral(coding))
        : lit(null);
  }
}
