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

package au.csiro.pathling.operations.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * In-process regression tests for the {@code schemaAutoMerge} workaround in {@link UpdateExecutor}.
 *
 * <p>Reproduces the {@code DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION} failure that occurs when a
 * Delta table's persisted schema lags the current FHIR encoder schema in the specific way that
 * Delta's MERGE cannot reconcile: missing fields inside nested struct types. A target struct with
 * fewer fields than the corresponding source struct cannot be implicitly cast by Delta's MERGE
 * planner, even with {@code spark.databricks.delta.schema.autoMerge.enabled=true}.
 *
 * <p>Setup: a Delta table is created via {@link UpdateExecutor} so it has the full encoder schema,
 * then the {@code schemaString} in the initial Delta log commit is rewritten in place to remove the
 * {@code forEach} and {@code forEachOrNull} fields from every nested struct. The CRC side-cars are
 * deleted so checksum validation does not reject the modified commit. The result looks like a Delta
 * table that was written by an older encoder version that lacked those fields.
 *
 * <p>Replaces the container-based regression coverage previously provided by {@code
 * ViewDefinitionInstallContainerIT}, runs in-process without Docker, and validates the
 * locally-built {@code UpdateExecutor} directly rather than the published Pathling image.
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class UpdateExecutorSchemaAutoMergeTest {

  private static final String VIEW_ID = "schema-compat-test";

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  @Autowired private CacheableDatabase cacheableDatabase;

  private Path tempDatabasePath;

  @BeforeEach
  void setUp() throws IOException {
    tempDatabasePath = Files.createTempDirectory("schema-automerge-test-");
  }

  @AfterEach
  void tearDown() throws IOException {
    if (tempDatabasePath != null && Files.exists(tempDatabasePath)) {
      Files.walk(tempDatabasePath)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    }
  }

  /**
   * With {@code schemaAutoMerge=false} a merge into a table whose nested-struct schema has been
   * downgraded must fail with {@code DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION} because Delta cannot
   * cast the richer source struct into the narrower target struct on the {@code
   * whenMatched().updateAll()} path.
   */
  @Test
  void mergeIntoOldSchemaTable_withoutAutoMerge_throwsSchemaMismatch() throws Exception {
    seedTableAndDowngradeSchema();

    final UpdateExecutor executor = newExecutor(false);
    final ViewDefinitionResource update = createViewDefinition(VIEW_ID, "updated_view", "Patient");

    assertThatThrownBy(() -> executor.merge("ViewDefinition", update))
        .hasMessageContaining("DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION");
  }

  /**
   * With {@code schemaAutoMerge=true} the warmup write evolves the table schema to match the
   * encoder's current shape before the MERGE runs, so the merge succeeds and the matched row is
   * updated.
   */
  @Test
  void mergeIntoOldSchemaTable_withAutoMerge_succeeds() throws Exception {
    seedTableAndDowngradeSchema();

    final UpdateExecutor executor = newExecutor(true);
    final ViewDefinitionResource update = createViewDefinition(VIEW_ID, "updated_view", "Patient");

    assertThatNoException().isThrownBy(() -> executor.merge("ViewDefinition", update));
  }

  // ---- helpers ----

  /**
   * Two-stage setup: write a ViewDefinition through {@link UpdateExecutor} so the Delta log is
   * created in the correct format, then rewrite the {@code schemaString} on disk to simulate an
   * older encoder version.
   */
  private void seedTableAndDowngradeSchema() throws IOException {
    final UpdateExecutor seed = newExecutor(true);
    final ViewDefinitionResource initial = createViewDefinition(VIEW_ID, "initial_view", "Patient");
    seed.merge("ViewDefinition", initial);

    final Path tablePath = tempDatabasePath.resolve("ViewDefinition.parquet");
    final Path deltaLogDir = tablePath.resolve("_delta_log");
    downgradeDeltaSchema(deltaLogDir);
    invalidateDeltaMetadataCache(tablePath);
  }

  private void invalidateDeltaMetadataCache(@Nonnull final Path tablePath) {
    pathlingContext.getSpark().catalog().clearCache();
    pathlingContext.getSpark().catalog().refreshByPath(tablePath.toAbsolutePath().toString());
  }
  @Nonnull
  private UpdateExecutor newExecutor(final boolean schemaAutoMerge) {
    return new UpdateExecutor(
        pathlingContext,
        fhirEncoders,
        tempDatabasePath.toAbsolutePath().toString(),
        cacheableDatabase,
        schemaAutoMerge);
  }

  /**
   * Rewrites the {@code metaData} action in the first Delta log commit to remove the {@code
   * forEach} and {@code forEachOrNull} fields from all nested struct definitions in the table's
   * {@code schemaString}. Also removes the Delta and Hadoop CRC side-cars so checksum validation
   * does not reject the modified commit.
   *
   * <p>Why struct-level removal rather than a wholesale schema replacement: Delta 4.x {@code
   * updateAll()} silently ignores extra top-level columns in the source, so a target with only
   * {@code {id}} does not trigger {@code DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION}. The error only
   * fires when a struct-typed column in the target has fewer fields than the corresponding struct
   * in the source.
   */
  private static void downgradeDeltaSchema(@Nonnull final Path deltaLogDir) throws IOException {
    final Path logFile = deltaLogDir.resolve("00000000000000000000.json");
    assertThat(logFile).exists();

    final ObjectMapper mapper = new ObjectMapper();
    final List<String> original = Files.readAllLines(logFile);
    final List<String> patched = new ArrayList<>(original.size());

    for (final String line : original) {
      final JsonNode node = mapper.readTree(line);
      if (node.has("metaData")) {
        final String schemaString = node.get("metaData").get("schemaString").asText();
        final JsonNode schema = mapper.readTree(schemaString);
        removeStructFields(schema, Set.of("forEach", "forEachOrNull"));
        ((ObjectNode) node.get("metaData")).put("schemaString", mapper.writeValueAsString(schema));
        patched.add(mapper.writeValueAsString(node));
      } else {
        patched.add(line);
      }
    }

    Files.write(logFile, patched);

    Files.deleteIfExists(deltaLogDir.resolve("00000000000000000000.crc"));
    Files.deleteIfExists(deltaLogDir.resolve(".00000000000000000000.json.crc"));
  }

  /**
   * Recursively removes fields with the given names from all struct-type definitions within a Spark
   * schema JSON node (as serialised by Delta Lake). Struct nodes are identified by having a {@code
   * fields} array; array-type nodes by having an {@code elementType} object.
   */
  private static void removeStructFields(
      @Nonnull final JsonNode node, @Nonnull final Set<String> fieldNames) {
    if (!node.isObject()) {
      return;
    }
    if (node.has("fields")) {
      final ArrayNode fields = (ArrayNode) node.get("fields");
      for (int i = fields.size() - 1; i >= 0; i--) {
        if (fieldNames.contains(fields.get(i).get("name").asText())) {
          fields.remove(i);
        }
      }
      for (final JsonNode field : fields) {
        removeStructFields(field.get("type"), fieldNames);
      }
    } else if (node.has("elementType")) {
      removeStructFields(node.get("elementType"), fieldNames);
    }
  }

  @Nonnull
  private ViewDefinitionResource createViewDefinition(
      @Nonnull final String id, @Nonnull final String name, @Nonnull final String resource) {
    final ViewDefinitionResource viewDef = new ViewDefinitionResource();
    viewDef.setId(id);
    viewDef.setName(new StringType(name));
    viewDef.setResource(new CodeType(resource));
    viewDef.setStatus(new CodeType("active"));

    final SelectComponent select = new SelectComponent();
    final ColumnComponent column = new ColumnComponent();
    column.setName(new StringType("id"));
    column.setPath(new StringType("id"));
    select.getColumn().add(column);
    viewDef.getSelect().add(select);

    return viewDef;
  }
}
