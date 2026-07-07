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

package au.csiro.pathling.terminology.store;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Defines the on-disk layout of the terminology store: the store format version, the names of the
 * Delta tables, and the schema of the manifest. Each table is a Delta table in a subdirectory of
 * the store's root path.
 *
 * @author John Grimes
 */
public final class TerminologyStoreSchema {

  private TerminologyStoreSchema() {
    // Utility class.
  }

  /**
   * The format version of the store. A reader refuses to open a store whose manifest reports a
   * greater version, since it may not understand the layout.
   */
  public static final int STORE_FORMAT_VERSION = 1;

  /** The manifest table, recording what content is loaded and the store format version. */
  public static final String MANIFEST = "manifest";

  /** The code system table, one row per imported code system version. */
  public static final String CODE_SYSTEM = "code_system";

  /** The concept table, one row per code within a code system version. */
  public static final String CONCEPT = "concept";

  /** The description table, holding SNOMED descriptions and FHIR designations. */
  public static final String DESCRIPTION = "description";

  /** The relationship table, holding SNOMED attributes and FHIR Coding-valued properties. */
  public static final String RELATIONSHIP = "relationship";

  /** The property table, holding scalar concept properties from FHIR code systems. */
  public static final String PROPERTY = "property";

  /** The transitive closure table of the concept hierarchy. */
  public static final String CLOSURE = "closure";

  /** The reference set membership table. */
  public static final String REFSET_MEMBER = "refset_member";

  /** The imported FHIR ValueSet resources. */
  public static final String VALUE_SET = "value_set";

  /** The imported FHIR ConceptMap resources. */
  public static final String CONCEPT_MAP = "concept_map";

  /** The manifest column holding the store format version. */
  public static final String COLUMN_STORE_FORMAT_VERSION = "store_format_version";

  /** The manifest column recording the kind of entry (code system, value set, concept map). */
  public static final String COLUMN_ENTRY_TYPE = "entry_type";

  /** The manifest column holding the canonical URL of the entry. */
  public static final String COLUMN_CANONICAL_URL = "canonical_url";

  /** The manifest column holding the version of the entry. */
  public static final String COLUMN_VERSION = "version";

  /** The manifest column recording the provenance (source file or package). */
  public static final String COLUMN_SOURCE = "source";

  /** The manifest column recording when the entry was imported. */
  public static final String COLUMN_IMPORTED_AT = "imported_at";

  /**
   * Returns the path to a table within a store.
   *
   * @param storagePath the root path of the store
   * @param tableName the table name (one of the constants in this class)
   * @return the fully qualified path to the table
   */
  @Nonnull
  public static String tablePath(
      @Nonnull final String storagePath, @Nonnull final String tableName) {
    final String base = storagePath.endsWith("/") ? storagePath : storagePath + "/";
    return base + tableName;
  }

  /**
   * Returns the Spark schema of the manifest table.
   *
   * @return the manifest schema
   */
  @Nonnull
  public static StructType manifestSchema() {
    return new StructType()
        .add(COLUMN_STORE_FORMAT_VERSION, DataTypes.IntegerType, false)
        .add(COLUMN_ENTRY_TYPE, DataTypes.StringType, false)
        .add(COLUMN_CANONICAL_URL, DataTypes.StringType, false)
        .add(COLUMN_VERSION, DataTypes.StringType, true)
        .add(COLUMN_SOURCE, DataTypes.StringType, true)
        .add(COLUMN_IMPORTED_AT, DataTypes.TimestampType, true);
  }
}
