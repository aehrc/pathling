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

package au.csiro.pathling.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Test fixtures that simulate Delta tables written by an older server version. The schema of an
 * existing table is downgraded by appending a new commit to the Delta log whose {@code metaData}
 * action carries a {@code schemaString} with named fields removed from every struct definition.
 * Delta applies the most recent {@code metaData} action, so the table presents the downgraded
 * schema without any checkpoint or data file being touched.
 *
 * @author John Grimes
 */
public final class DeltaSchemaFixtures {

  private DeltaSchemaFixtures() {}

  /**
   * Downgrades the table's schema by removing the named fields from every struct definition. The
   * most recent {@code metaData} action in the JSON commit log is copied, its {@code schemaString}
   * rewritten, and the result appended as a new commit. The result looks like a table written by an
   * older encoder version that lacked those fields.
   *
   * @param tablePath the path to the Delta table directory
   * @param fieldNames the names of the fields to remove wherever they occur
   * @throws IOException if the Delta log cannot be read or written
   */
  public static void removeFieldsFromTableSchema(
      @Nonnull final Path tablePath, @Nonnull final Set<String> fieldNames) throws IOException {
    final Path deltaLogDir = tablePath.resolve("_delta_log");
    final ObjectMapper mapper = new ObjectMapper();

    final List<Path> commits = listJsonCommits(deltaLogDir);
    if (commits.isEmpty()) {
      throw new IllegalStateException("No JSON commits found in Delta log: " + deltaLogDir);
    }

    // Find the most recent metaData action in the JSON commit log. The metaData action is written
    // in the first commit and only rewritten on schema or configuration changes, so scanning the
    // JSON commits is sufficient for tables produced by test data generation.
    final JsonNode metaData = findLatestMetaData(mapper, commits);
    if (metaData == null) {
      throw new IllegalStateException("No metaData action found in Delta log: " + deltaLogDir);
    }

    // Rewrite the schemaString with the named fields removed.
    final JsonNode schema = mapper.readTree(metaData.get("schemaString").asText());
    removeStructFields(schema, fieldNames);
    ((ObjectNode) metaData).put("schemaString", mapper.writeValueAsString(schema));

    // Append the downgraded metaData as a new commit, so it becomes the table's current schema.
    final long lastVersion = commitVersion(commits.get(commits.size() - 1));
    final long newVersion = lastVersion + 1;
    final ObjectNode commitInfo = mapper.createObjectNode();
    final ObjectNode commitInfoBody = commitInfo.putObject("commitInfo");
    commitInfoBody.put("timestamp", System.currentTimeMillis());
    commitInfoBody.put("operation", "CHANGE SCHEMA");
    commitInfoBody.putObject("operationParameters");
    commitInfoBody.put("readVersion", lastVersion);
    commitInfoBody.put("isBlindAppend", false);
    final ObjectNode metaDataAction = mapper.createObjectNode();
    metaDataAction.set("metaData", metaData);

    final Path newCommit = deltaLogDir.resolve(String.format("%020d.json", newVersion));
    Files.write(
        newCommit,
        List.of(mapper.writeValueAsString(commitInfo), mapper.writeValueAsString(metaDataAction)));
  }

  /** Lists the JSON commit files of a Delta log in version order. */
  @Nonnull
  private static List<Path> listJsonCommits(@Nonnull final Path deltaLogDir) throws IOException {
    try (final Stream<Path> entries = Files.list(deltaLogDir)) {
      return entries
          .filter(path -> path.getFileName().toString().matches("\\d{20}\\.json"))
          .sorted()
          .toList();
    }
  }

  /** Extracts the numeric version from a commit file name. */
  private static long commitVersion(@Nonnull final Path commit) {
    final String name = commit.getFileName().toString();
    return Long.parseLong(name.substring(0, name.length() - ".json".length()));
  }

  /** Returns the most recent {@code metaData} action found in the given commits, if any. */
  @Nullable
  private static JsonNode findLatestMetaData(
      @Nonnull final ObjectMapper mapper, @Nonnull final List<Path> commits) throws IOException {
    JsonNode latest = null;
    for (final Path commit : commits) {
      for (final String line : Files.readAllLines(commit)) {
        final JsonNode node = mapper.readTree(line);
        if (node.has("metaData")) {
          latest = node.get("metaData");
        }
      }
    }
    return latest;
  }

  /**
   * Recursively removes fields with the given names from all struct-type definitions within a Spark
   * schema JSON node (as serialised by Delta Lake). Struct nodes are identified by having a {@code
   * fields} array; array-type nodes by having an {@code elementType} object; map-type nodes by
   * having a {@code valueType} object.
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
    } else if (node.has("valueType")) {
      removeStructFields(node.get("valueType"), fieldNames);
    }
  }
}
