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


/**
 * Object decoders/encoders for {@link Coding} and collections of it.
 */
public interface CodingEncoding {

  /**
   * @return a {@link StructType} that can house a Coding
   */
  @Nonnull
  static StructType codingStructType() {
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

  /**
   * A {@link StructType} for a Coding.
   */
  StructType DATA_TYPE = codingStructType();

  int SYSTEM_INDEX = DATA_TYPE.fieldIndex("system");
  int VERSION_INDEX = DATA_TYPE.fieldIndex("version");
  int CODE_INDEX = DATA_TYPE.fieldIndex("code");
  int DISPLAY_INDEX = DATA_TYPE.fieldIndex("display");
  int USER_SELECTED_INDEX = DATA_TYPE.fieldIndex("userSelected");


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
      final Boolean userSelected = coding.hasUserSelected()
                                   ? coding.getUserSelected()
                                   : null;
      return RowFactory.create(coding.getId(), coding.getSystem(), coding.getVersion(),
          coding.getCode(), coding.getDisplay(), userSelected, null /* _fid */);
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
    return codings == null
           ? null
           : codings.stream().map(CodingEncoding::encode).toList();
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
    return encoded == null
           ? null
           : encoded.toArray(new Row[0]);
  }

  @Nonnull
  static Column toStruct(@Nonnull final Column id, @Nonnull final Column system,
      @Nonnull final Column version, @Nonnull final Column code, @Nonnull final Column display,
      @Nonnull final Column userSelected) {
    return functions.struct(
        id.as("id"),
        system.as("system"),
        version.as("version"),
        code.as("code"),
        display.as("display"),
        userSelected.as("userSelected"),
        lit(null).as("_fid")
    );
  }


  @Nonnull
  private static Column userSelectedToLiteral(@Nonnull final Coding coding) {
    return coding.hasUserSelected()
           ? lit(coding.getUserSelected())
           : lit(null);
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
           ? toStruct(lit(coding.getId()), lit(coding.getSystem()), lit(coding.getVersion()),
        lit(coding.getCode()), lit(coding.getDisplay()), userSelectedToLiteral(coding))
           : lit(null);
  }
}
