/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.encoding;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
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
    return coding == null
           ? null
           : RowFactory
               .create(coding.getId(), coding.getSystem(), coding.getVersion(), coding.getCode(),
                   coding.getDisplay(),
                   coding.hasUserSelected()
                   ? coding.getUserSelected()
                   : null,
                   null); // _fid

  }

  /**
   * Decodes a Coding from a Row.
   *
   * @param row the row to decode
   * @return the resulting Coding
   */
  static Coding decode(@Nonnull final Row row) {
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
   * Encodes a  list of Codings to a Row[] (spark SQL compatible type)
   *
   * @param codings a  list of codings to encode
   * @return the Row[] representation of the coding
   */
  @Nullable
  static Row[] encodeList(@Nullable final List<Coding> codings) {
    return codings == null
           ? null
           : codings.stream().map(CodingEncoding::encode).toArray(Row[]::new);
  }
}
