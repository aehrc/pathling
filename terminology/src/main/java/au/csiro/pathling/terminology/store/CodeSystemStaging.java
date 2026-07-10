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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ACTIVE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CONCEPT_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DEFINED;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DISPLAY;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_LANGUAGE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_PROPERTY_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TERM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TYPE_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TYPE_SYSTEM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VALUE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VALUE_TYPE;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.SerializedString;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Manages the transient NDJSON staging files produced while streaming one CodeSystem to disk. Each
 * of the six staging files (concepts, descriptions, scalar properties, Coding-valued properties,
 * nesting-derived edges, and property-derived edges) is written line by line by the flattener, then
 * read back by the stage loader through an explicit Spark schema. All files live in a single
 * driver-local temporary directory that is deleted when the staging is closed, on both the success
 * and failure paths.
 *
 * <p>The columns of each staging file are a subset of the store schema (the store's SNOMED-only
 * columns and the resolved dense identifiers of code-keyed references are supplied later by the
 * loader), so the staging never holds content proportional to the source size on the driver.
 *
 * @author John Grimes
 */
@Slf4j
public class CodeSystemStaging implements AutoCloseable {

  /** The staging column naming the concept that carries a property-derived edge. */
  static final String COLUMN_KNOWN_DENSE_ID = "known_dense_id";

  /** The staging column recording whether the known side of an edge is the child or the parent. */
  static final String COLUMN_KNOWN_ROLE = "known_role";

  /** The staging column holding the still-unresolved referenced code of a property-derived edge. */
  static final String COLUMN_OTHER_CODE = "other_code";

  /** The {@code known_role} value marking the known side of an edge as the child. */
  static final String ROLE_CHILD = "child";

  /** The {@code known_role} value marking the known side of an edge as the parent. */
  static final String ROLE_PARENT = "parent";

  private static final JsonFactory FACTORY = new JsonFactory();

  private static final String FILE_CONCEPT = "concept.ndjson";
  private static final String FILE_DESCRIPTION = "description.ndjson";
  private static final String FILE_PROPERTY = "property.ndjson";
  private static final String FILE_CODING_PROPERTY = "coding_property.ndjson";
  private static final String FILE_EDGE_DENSE = "edge_dense.ndjson";
  private static final String FILE_EDGE_BY_CODE = "edge_by_code.ndjson";

  private static final String[] ALL_FILES = {
    FILE_CONCEPT,
    FILE_DESCRIPTION,
    FILE_PROPERTY,
    FILE_CODING_PROPERTY,
    FILE_EDGE_DENSE,
    FILE_EDGE_BY_CODE
  };

  @Nonnull private final Path directory;
  @Nonnull private final Map<String, JsonGenerator> generators = new LinkedHashMap<>();
  private boolean sealed;

  private CodeSystemStaging(@Nonnull final Path directory) {
    this.directory = directory;
  }

  /**
   * Creates a fresh staging directory with an empty file per staging table ready to append to.
   *
   * @return a new staging instance
   * @throws TerminologyImportException if the temporary directory cannot be created
   */
  @Nonnull
  public static CodeSystemStaging create() {
    try {
      final Path directory = SecureTempDirectory.create("pathling-fhir-import-");
      final CodeSystemStaging staging = new CodeSystemStaging(directory);
      // Open every file eagerly so an empty staging table still reads back as zero rows.
      for (final String file : ALL_FILES) {
        staging.generators.put(file, staging.openGenerator(file));
      }
      return staging;
    } catch (final IOException e) {
      throw new TerminologyImportException("Unable to create a temporary staging directory", e);
    }
  }

  /**
   * Returns the staging directory.
   *
   * @return the temporary directory holding the staging files
   */
  @Nonnull
  public Path getDirectory() {
    return directory;
  }

  @Nonnull
  private JsonGenerator openGenerator(@Nonnull final String file) throws IOException {
    final OutputStream out = Files.newOutputStream(directory.resolve(file));
    final JsonGenerator generator = FACTORY.createGenerator(out, JsonEncoding.UTF8);
    // Emit one JSON object per line so Spark reads the file as newline-delimited JSON.
    generator.setRootValueSeparator(new SerializedString("\n"));
    return generator;
  }

  @Nonnull
  private JsonGenerator generator(@Nonnull final String file) {
    if (sealed) {
      throw new IllegalStateException("Staging has been sealed for reading and cannot be appended");
    }
    return generators.get(file);
  }

  /**
   * Appends a concept row.
   *
   * @param code the concept code
   * @param denseId the document-order dense identifier
   * @param active whether the concept is active
   * @param defined whether the concept is sufficiently defined (always false for FHIR)
   * @param display the display term, falling back to the code when absent
   */
  public void appendConcept(
      @Nonnull final String code,
      final int denseId,
      final boolean active,
      final boolean defined,
      @Nonnull final String display) {
    try {
      final JsonGenerator generator = generator(FILE_CONCEPT);
      generator.writeStartObject();
      generator.writeStringField(COLUMN_CODE, code);
      generator.writeNumberField(COLUMN_DENSE_ID, denseId);
      generator.writeBooleanField(COLUMN_ACTIVE, active);
      generator.writeBooleanField(COLUMN_DEFINED, defined);
      generator.writeStringField(COLUMN_DISPLAY, display);
      generator.writeEndObject();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Appends a description (designation) row.
   *
   * @param conceptDenseId the dense identifier of the owning concept
   * @param term the designation value
   * @param language the designation language, or null
   * @param typeCode the designation use code, or null
   * @param typeSystem the designation use system
   */
  public void appendDescription(
      final int conceptDenseId,
      @Nullable final String term,
      @Nullable final String language,
      @Nullable final String typeCode,
      @Nullable final String typeSystem) {
    try {
      final JsonGenerator generator = generator(FILE_DESCRIPTION);
      generator.writeStartObject();
      generator.writeNumberField(COLUMN_CONCEPT_DENSE_ID, conceptDenseId);
      writeNullableString(generator, COLUMN_TERM, term);
      writeNullableString(generator, COLUMN_LANGUAGE, language);
      writeNullableString(generator, COLUMN_TYPE_CODE, typeCode);
      writeNullableString(generator, COLUMN_TYPE_SYSTEM, typeSystem);
      generator.writeEndObject();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Appends a scalar property row.
   *
   * @param conceptDenseId the dense identifier of the owning concept
   * @param propertyCode the property code
   * @param valueType the FHIR type name of the value
   * @param value the primitive value as a string
   */
  public void appendProperty(
      final int conceptDenseId,
      @Nonnull final String propertyCode,
      @Nonnull final String valueType,
      @Nonnull final String value) {
    try {
      final JsonGenerator generator = generator(FILE_PROPERTY);
      generator.writeStartObject();
      generator.writeNumberField(COLUMN_CONCEPT_DENSE_ID, conceptDenseId);
      generator.writeStringField(COLUMN_PROPERTY_CODE, propertyCode);
      generator.writeStringField(COLUMN_VALUE_TYPE, valueType);
      generator.writeStringField(COLUMN_VALUE, value);
      generator.writeEndObject();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Appends an unresolved Coding-valued property row.
   *
   * @param sourceDenseId the dense identifier of the concept carrying the property
   * @param propertyCode the property code
   * @param targetCode the referenced Coding code, resolved to a dense identifier by the loader
   */
  public void appendCodingProperty(
      final int sourceDenseId,
      @Nonnull final String propertyCode,
      @Nonnull final String targetCode) {
    try {
      final JsonGenerator generator = generator(FILE_CODING_PROPERTY);
      generator.writeStartObject();
      generator.writeNumberField(COLUMN_SOURCE_DENSE_ID, sourceDenseId);
      generator.writeStringField(COLUMN_PROPERTY_CODE, propertyCode);
      generator.writeStringField(COLUMN_TARGET_CODE, targetCode);
      generator.writeEndObject();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Appends a fully resolved nesting-derived is-a edge.
   *
   * @param sourceDenseId the dense identifier of the child (nested) concept
   * @param targetDenseId the dense identifier of the parent (enclosing) concept
   */
  public void appendDenseEdge(final int sourceDenseId, final int targetDenseId) {
    try {
      final JsonGenerator generator = generator(FILE_EDGE_DENSE);
      generator.writeStartObject();
      generator.writeNumberField(COLUMN_SOURCE_DENSE_ID, sourceDenseId);
      generator.writeNumberField(COLUMN_TARGET_DENSE_ID, targetDenseId);
      generator.writeEndObject();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Appends a property-derived is-a edge with the referenced side still unresolved.
   *
   * @param knownDenseId the dense identifier of the concept carrying the property
   * @param knownRole {@code child} when derived from a parent property, {@code parent} when derived
   *     from a child property
   * @param otherCode the referenced code, resolved to a dense identifier by the loader
   */
  public void appendCodeEdge(
      final int knownDenseId, @Nonnull final String knownRole, @Nonnull final String otherCode) {
    try {
      final JsonGenerator generator = generator(FILE_EDGE_BY_CODE);
      generator.writeStartObject();
      generator.writeNumberField(COLUMN_KNOWN_DENSE_ID, knownDenseId);
      generator.writeStringField(COLUMN_KNOWN_ROLE, knownRole);
      generator.writeStringField(COLUMN_OTHER_CODE, otherCode);
      generator.writeEndObject();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Flushes and closes every appender so the staging files can be read back by Spark. No further
   * rows may be appended after sealing.
   */
  public void sealForReading() {
    if (sealed) {
      return;
    }
    for (final JsonGenerator generator : generators.values()) {
      try {
        generator.close();
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    sealed = true;
  }

  // --- Staging file paths. ---

  @Nonnull
  private String path(@Nonnull final String file) {
    return directory.resolve(file).toUri().toString();
  }

  /**
   * Returns the path of the concept staging file.
   *
   * @return the concept file path
   */
  @Nonnull
  public String conceptPath() {
    return path(FILE_CONCEPT);
  }

  /**
   * Returns the path of the description staging file.
   *
   * @return the description file path
   */
  @Nonnull
  public String descriptionPath() {
    return path(FILE_DESCRIPTION);
  }

  /**
   * Returns the path of the scalar property staging file.
   *
   * @return the property file path
   */
  @Nonnull
  public String propertyPath() {
    return path(FILE_PROPERTY);
  }

  /**
   * Returns the path of the Coding-property staging file.
   *
   * @return the Coding-property file path
   */
  @Nonnull
  public String codingPropertyPath() {
    return path(FILE_CODING_PROPERTY);
  }

  /**
   * Returns the path of the nesting-derived edge staging file.
   *
   * @return the dense-edge file path
   */
  @Nonnull
  public String denseEdgePath() {
    return path(FILE_EDGE_DENSE);
  }

  /**
   * Returns the path of the property-derived edge staging file.
   *
   * @return the code-edge file path
   */
  @Nonnull
  public String codeEdgePath() {
    return path(FILE_EDGE_BY_CODE);
  }

  // --- Explicit Spark read schemas. ---

  /**
   * Returns the read schema of the concept staging file.
   *
   * @return the concept schema
   */
  @Nonnull
  public static StructType conceptSchema() {
    return new StructType()
        .add(COLUMN_CODE, DataTypes.StringType, false)
        .add(COLUMN_DENSE_ID, DataTypes.IntegerType, false)
        .add(COLUMN_ACTIVE, DataTypes.BooleanType, false)
        .add(COLUMN_DEFINED, DataTypes.BooleanType, false)
        .add(COLUMN_DISPLAY, DataTypes.StringType, true);
  }

  /**
   * Returns the read schema of the description staging file.
   *
   * @return the description schema
   */
  @Nonnull
  public static StructType descriptionSchema() {
    return new StructType()
        .add(COLUMN_CONCEPT_DENSE_ID, DataTypes.IntegerType, false)
        .add(COLUMN_TERM, DataTypes.StringType, true)
        .add(COLUMN_LANGUAGE, DataTypes.StringType, true)
        .add(COLUMN_TYPE_CODE, DataTypes.StringType, true)
        .add(COLUMN_TYPE_SYSTEM, DataTypes.StringType, true);
  }

  /**
   * Returns the read schema of the scalar property staging file.
   *
   * @return the property schema
   */
  @Nonnull
  public static StructType propertySchema() {
    return new StructType()
        .add(COLUMN_CONCEPT_DENSE_ID, DataTypes.IntegerType, false)
        .add(COLUMN_PROPERTY_CODE, DataTypes.StringType, false)
        .add(COLUMN_VALUE_TYPE, DataTypes.StringType, false)
        .add(COLUMN_VALUE, DataTypes.StringType, false);
  }

  /**
   * Returns the read schema of the Coding-property staging file.
   *
   * @return the Coding-property schema
   */
  @Nonnull
  public static StructType codingPropertySchema() {
    return new StructType()
        .add(COLUMN_SOURCE_DENSE_ID, DataTypes.IntegerType, false)
        .add(COLUMN_PROPERTY_CODE, DataTypes.StringType, false)
        .add(COLUMN_TARGET_CODE, DataTypes.StringType, false);
  }

  /**
   * Returns the read schema of the nesting-derived edge staging file.
   *
   * @return the dense-edge schema
   */
  @Nonnull
  public static StructType denseEdgeSchema() {
    return new StructType()
        .add(COLUMN_SOURCE_DENSE_ID, DataTypes.IntegerType, false)
        .add(COLUMN_TARGET_DENSE_ID, DataTypes.IntegerType, false);
  }

  /**
   * Returns the read schema of the property-derived edge staging file.
   *
   * @return the code-edge schema
   */
  @Nonnull
  public static StructType codeEdgeSchema() {
    return new StructType()
        .add(COLUMN_KNOWN_DENSE_ID, DataTypes.IntegerType, false)
        .add(COLUMN_KNOWN_ROLE, DataTypes.StringType, false)
        .add(COLUMN_OTHER_CODE, DataTypes.StringType, false);
  }

  @Override
  public void close() {
    if (!sealed) {
      for (final JsonGenerator generator : generators.values()) {
        try {
          generator.close();
        } catch (final IOException e) {
          log.debug("Failed to close a staging appender during cleanup", e);
        }
      }
      sealed = true;
    }
    deleteRecursively(directory);
  }

  private static void writeNullableString(
      @Nonnull final JsonGenerator generator,
      @Nonnull final String field,
      @Nullable final String value)
      throws IOException {
    if (value == null) {
      generator.writeNullField(field);
    } else {
      generator.writeStringField(field, value);
    }
  }

  private static void deleteRecursively(@Nonnull final Path directory) {
    if (!Files.exists(directory)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(directory)) {
      paths
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (final IOException e) {
                  log.debug("Failed to delete temporary staging file {}", path, e);
                }
              });
    } catch (final IOException e) {
      log.warn("Failed to clean up temporary staging directory {}", directory, e);
    }
  }
}
