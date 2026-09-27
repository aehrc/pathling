/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Coding;

/**
 * Decodes a Coding from a row by field name, against the schema of the structure it is stored in
 * (T091, FR-031).
 *
 * <p>A stored Coding need not have the canonical structure of {@link CodingSchema#DATA_TYPE}. A
 * pruned new-layout schema carries only the fields the data populates, the new layout orders them
 * as the definition does and may add an inline {@code extension}, and the previous layout adds a
 * {@code _fid}. So the position of each Coding field is resolved from the structure's field names,
 * and an absent field decodes as null.
 *
 * <p>The positions are resolved once per schema, not per row. Every row that Spark hands a function
 * from one column shares one schema instance, so the decoder last used is kept and reused while the
 * schema is the same instance; any other schema is looked up by equality.
 *
 * <p>A row without a schema, as {@link org.apache.spark.sql.RowFactory} builds it, can only be read
 * by position, so it is read as the canonical structure.
 *
 * @author Piotr Szul
 */
public final class CodingDecoder {

  /**
   * The fields a Coding structure may carry: the elements of a Coding, its inline extensions on the
   * new layout, and its field identifier on the previous layout.
   */
  @Nonnull
  private static final List<String> CODING_FIELDS =
      List.of(
          CodingSchema.ID_FIELD,
          CodingSchema.SYSTEM_FIELD,
          CodingSchema.VERSION_FIELD,
          CodingSchema.CODE_FIELD,
          CodingSchema.DISPLAY_FIELD,
          CodingSchema.USER_SELECTED_FIELD,
          "extension",
          CodingSchema.FID_FIELD);

  /** The position that stands for a field the structure does not carry. */
  private static final int ABSENT = -1;

  @Nonnull private static final CodingDecoder CANONICAL = new CodingDecoder(CodingSchema.DATA_TYPE);

  @Nonnull
  private static final Map<StructType, CodingDecoder> BY_SCHEMA = new ConcurrentHashMap<>();

  @Nonnull
  private static final AtomicReference<CodingDecoder> LAST_USED = new AtomicReference<>(CANONICAL);

  @Nonnull private final StructType schema;

  private final int systemIndex;

  private final int versionIndex;

  private final int codeIndex;

  private final int displayIndex;

  private final int userSelectedIndex;

  private CodingDecoder(@Nonnull final StructType schema) {
    final List<String> fieldNames = Arrays.asList(schema.fieldNames());
    if (fieldNames.stream().noneMatch(CODING_FIELDS::contains)) {
      throw new IllegalArgumentException(
          "Expected a Coding structure with at least one of the fields "
              + CODING_FIELDS
              + ", but the structure has the fields "
              + fieldNames);
    }
    this.schema = schema;
    this.systemIndex = fieldNames.indexOf(CodingSchema.SYSTEM_FIELD);
    this.versionIndex = fieldNames.indexOf(CodingSchema.VERSION_FIELD);
    this.codeIndex = fieldNames.indexOf(CodingSchema.CODE_FIELD);
    this.displayIndex = fieldNames.indexOf(CodingSchema.DISPLAY_FIELD);
    this.userSelectedIndex = fieldNames.indexOf(CodingSchema.USER_SELECTED_FIELD);
  }

  /**
   * Returns the decoder for a Coding structure, resolving the positions of its fields if they have
   * not been resolved for that schema already.
   *
   * @param schema the schema of the structure, or null for a row without a schema
   * @return the decoder for that schema
   * @throws IllegalArgumentException if the structure carries no field a Coding may have
   */
  @Nonnull
  public static CodingDecoder forSchema(@Nullable final StructType schema) {
    if (schema == null) {
      return CANONICAL;
    }
    final CodingDecoder last = LAST_USED.get();
    // Rows from one column share one schema instance, so an identity check suffices here, and it
    // avoids hashing the schema for every row.
    if (last.schema == schema) {
      return last;
    }
    final CodingDecoder decoder = BY_SCHEMA.computeIfAbsent(schema, CodingDecoder::new);
    LAST_USED.set(decoder);
    return decoder;
  }

  /**
   * Decodes a Coding from a row, by the names of the fields in the row's schema.
   *
   * @param row the row to decode
   * @return the Coding, or null if the row is null
   * @throws IllegalArgumentException if the row's structure carries no field a Coding may have
   */
  @Nullable
  public static Coding decodeRow(@Nullable final Row row) {
    return row == null ? null : forSchema(row.schema()).decode(row);
  }

  /**
   * Decodes a Coding from a row of the schema this decoder was resolved for.
   *
   * @param row the row to decode
   * @return the Coding, or null if the row is null
   */
  @Nullable
  public Coding decode(@Nullable final Row row) {
    if (row == null) {
      return null;
    }
    final Coding coding = new Coding();
    coding.setSystem(stringAt(row, systemIndex));
    coding.setVersion(stringAt(row, versionIndex));
    coding.setCode(stringAt(row, codeIndex));
    coding.setDisplay(stringAt(row, displayIndex));
    if (userSelectedIndex != ABSENT && !row.isNullAt(userSelectedIndex)) {
      coding.setUserSelected(row.getBoolean(userSelectedIndex));
    }
    return coding;
  }

  @Nullable
  private static String stringAt(@Nonnull final Row row, final int index) {
    return index == ABSENT ? null : row.getString(index);
  }
}
