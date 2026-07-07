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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
   * The stable identifier of a code system version, a hash of its URL and version. This is the
   * partition column of every content table so that versions coexist and replace atomically.
   */
  public static final String COLUMN_SYSTEM_VERSION_ID = "system_version_id";

  /** The canonical URL of a code system (e.g. {@code http://snomed.info/sct}). */
  public static final String COLUMN_URL = "url";

  /** The SNOMED edition module identifier parsed from a version URI; null for non-SNOMED. */
  public static final String COLUMN_SNOMED_EDITION = "snomed_edition";

  /** The SNOMED effectiveTime parsed from a version URI; null for non-SNOMED. */
  public static final String COLUMN_SNOMED_EFFECTIVE_TIME = "snomed_effective_time";

  /** The number of concepts in a code system version. */
  public static final String COLUMN_CONCEPT_COUNT = "concept_count";

  /** The FHIR CodeSystem hierarchy meaning ({@code is-a} for SNOMED). */
  public static final String COLUMN_HIERARCHY_MEANING = "hierarchy_meaning";

  /** A concept code (SCTID or CodeSystem concept code). */
  public static final String COLUMN_CODE = "code";

  /** The contiguous per-system-version integer that addresses a concept in the runtime bitmaps. */
  public static final String COLUMN_DENSE_ID = "dense_id";

  /** Whether a concept, description, or relationship is active. */
  public static final String COLUMN_ACTIVE = "active";

  /** An RF2 effectiveTime ({@code YYYYMMDD}). */
  public static final String COLUMN_EFFECTIVE_TIME = "effective_time";

  /** The SNOMED module identifier of a concept. */
  public static final String COLUMN_MODULE_ID = "module_id";

  /** Whether a SNOMED concept is sufficiently defined. */
  public static final String COLUMN_DEFINED = "defined";

  /** The default display term of a concept. */
  public static final String COLUMN_DISPLAY = "display";

  /** The dense identifier of the concept a description or property belongs to. */
  public static final String COLUMN_CONCEPT_DENSE_ID = "concept_dense_id";

  /** A description or designation term. */
  public static final String COLUMN_TERM = "term";

  /** The BCP-47 language of a description. */
  public static final String COLUMN_LANGUAGE = "language";

  /** The description type SCTID or designation use code. */
  public static final String COLUMN_TYPE_CODE = "type_code";

  /** The code system of a description type or designation use. */
  public static final String COLUMN_TYPE_SYSTEM = "type_system";

  /** A map from language reference set identifier to acceptability (SNOMED only). */
  public static final String COLUMN_ACCEPTABILITY = "acceptability";

  /** The dense identifier of a relationship's source concept. */
  public static final String COLUMN_SOURCE_DENSE_ID = "source_dense_id";

  /** The dense identifier of a relationship's target concept. */
  public static final String COLUMN_TARGET_DENSE_ID = "target_dense_id";

  /** The relationship group of a SNOMED relationship. */
  public static final String COLUMN_ROLE_GROUP = "role_group";

  /** A scalar property code (FHIR CodeSystem property). */
  public static final String COLUMN_PROPERTY_CODE = "property_code";

  /** The declared type of a scalar property value. */
  public static final String COLUMN_VALUE_TYPE = "value_type";

  /** The canonical string encoding of a scalar property value. */
  public static final String COLUMN_VALUE = "value";

  /** The dense identifier of a closure ancestor. */
  public static final String COLUMN_ANCESTOR_DENSE_ID = "ancestor_dense_id";

  /** The dense identifier of a closure descendant. */
  public static final String COLUMN_DESCENDANT_DENSE_ID = "descendant_dense_id";

  /** Whether a closure edge is a direct parent-child edge. */
  public static final String COLUMN_DIRECT = "direct";

  /** The reference set identifier of a membership row. */
  public static final String COLUMN_REFSET_CODE = "refset_code";

  /** The dense identifier of a reference set member's referenced concept. */
  public static final String COLUMN_REFERENCED_DENSE_ID = "referenced_dense_id";

  /** The association target code of a reference set member (drives {@code ?fhir_cm}). */
  public static final String COLUMN_TARGET_CODE = "target_code";

  /** The full R4 resource JSON of an imported ValueSet or ConceptMap. */
  public static final String COLUMN_RESOURCE_JSON = "resource_json";

  /**
   * Derives the stable identifier of a code system version from its URL and version. This is the
   * partition key of every content table, so that versions coexist and replace atomically.
   *
   * @param url the code system canonical URL
   * @param version the code system version
   * @return a short hexadecimal hash of the URL and version
   */
  @Nonnull
  public static String systemVersionId(@Nonnull final String url, @Nonnull final String version) {
    try {
      final MessageDigest digest = MessageDigest.getInstance("SHA-256");
      final byte[] hash = digest.digest((url + "|" + version).getBytes(StandardCharsets.UTF_8));
      final StringBuilder builder = new StringBuilder();
      for (int i = 0; i < 8; i++) {
        builder.append(String.format("%02x", hash[i]));
      }
      return builder.toString();
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is not available", e);
    }
  }

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
   * Returns the Spark schema of a resource table ({@code value_set} or {@code concept_map}), which
   * stores each imported FHIR resource as JSON keyed by its canonical URL and version.
   *
   * @return the resource table schema
   */
  @Nonnull
  public static StructType resourceTableSchema() {
    return new StructType()
        .add(COLUMN_CANONICAL_URL, DataTypes.StringType, false)
        .add(COLUMN_VERSION, DataTypes.StringType, true)
        .add(COLUMN_RESOURCE_JSON, DataTypes.StringType, false);
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
