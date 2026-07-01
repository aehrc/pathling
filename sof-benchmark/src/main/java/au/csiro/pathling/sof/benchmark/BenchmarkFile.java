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

package au.csiro.pathling.sof.benchmark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A parsed SQL-on-FHIR benchmark file (see {@code benchmark.schema.json}).
 *
 * <p>The file is parsed with Jackson as a tree; the {@code view} node of each case is re-serialized
 * to a JSON string so it can be handed to {@code FhirViewQuery.json(String)}, which deserializes it
 * with Gson internally. A Jackson node is never handed directly to Pathling.
 */
public class BenchmarkFile {

  /** Default warmup iterations when the benchmark file omits an {@code iterations} block. */
  private static final int DEFAULT_WARMUP = 1;

  /** Default measured iterations when the benchmark file omits an {@code iterations} block. */
  private static final int DEFAULT_MEASUREMENT = 5;

  @Nonnull private static final ObjectMapper MAPPER = new ObjectMapper();

  @Nonnull private final String title;

  @Nonnull private final String fhirVersion;

  @Nonnull private final BenchmarkDataset dataset;

  @Nonnull private final List<BenchmarkCase> cases;

  private final int warmup;

  private final int measurement;

  private BenchmarkFile(
      @Nonnull final String title,
      @Nonnull final String fhirVersion,
      @Nonnull final BenchmarkDataset dataset,
      @Nonnull final List<BenchmarkCase> cases,
      final int warmup,
      final int measurement) {
    this.title = title;
    this.fhirVersion = fhirVersion;
    this.dataset = dataset;
    this.cases = cases;
    this.warmup = warmup;
    this.measurement = measurement;
  }

  /**
   * Parses a benchmark file from disk.
   *
   * @param file the path to the benchmark {@code *.json} file
   * @return the parsed benchmark file
   * @throws UncheckedIOException if the file cannot be read or parsed
   */
  @Nonnull
  public static BenchmarkFile parse(@Nonnull final Path file) {
    final JsonNode root;
    try {
      root = MAPPER.readTree(Files.readAllBytes(file));
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to read benchmark file: " + file, e);
    }

    final String title = requiredText(root, "title");
    final String fhirVersion = requiredText(root, "fhirVersion");
    final BenchmarkDataset dataset = parseDataset(root.path("dataset"));
    final List<BenchmarkCase> cases = parseCases(root.path("cases"));

    final JsonNode iterations = root.path("iterations");
    final int warmup = iterations.path("warmup").asInt(DEFAULT_WARMUP);
    final int measurement = iterations.path("measurement").asInt(DEFAULT_MEASUREMENT);

    return new BenchmarkFile(title, fhirVersion, dataset, cases, warmup, measurement);
  }

  @Nonnull
  private static BenchmarkDataset parseDataset(@Nonnull final JsonNode dataset) {
    final String name = requiredText(dataset, "name");

    final Map<String, Integer> sizePopulations = new LinkedHashMap<>();
    final JsonNode sizes = dataset.path("sizes");
    final Iterator<Map.Entry<String, JsonNode>> sizeFields = sizes.fields();
    while (sizeFields.hasNext()) {
      final Map.Entry<String, JsonNode> entry = sizeFields.next();
      sizePopulations.put(entry.getKey(), entry.getValue().path("population").asInt());
    }

    final List<String> resources = new ArrayList<>();
    dataset.path("resources").forEach(node -> resources.add(node.asText()));

    return new BenchmarkDataset(name, sizePopulations, resources);
  }

  @Nonnull
  private static List<BenchmarkCase> parseCases(@Nonnull final JsonNode casesNode) {
    final List<BenchmarkCase> cases = new ArrayList<>();
    for (final JsonNode caseNode : casesNode) {
      final String title = requiredText(caseNode, "title");
      final JsonNode view = caseNode.path("view");
      final String resource = requiredText(view, "resource");
      final String viewJson = serialize(view);

      final Map<String, Integer> expectCount = new LinkedHashMap<>();
      final JsonNode expect = caseNode.path("expectCount");
      final Iterator<Map.Entry<String, JsonNode>> expectFields = expect.fields();
      while (expectFields.hasNext()) {
        final Map.Entry<String, JsonNode> entry = expectFields.next();
        expectCount.put(entry.getKey(), entry.getValue().asInt());
      }

      cases.add(new BenchmarkCase(title, resource, viewJson, expectCount));
    }
    return cases;
  }

  @Nonnull
  private static String serialize(@Nonnull final JsonNode node) {
    try {
      return MAPPER.writeValueAsString(node);
    } catch (final JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize view node", e);
    }
  }

  @Nonnull
  private static String requiredText(@Nonnull final JsonNode parent, @Nonnull final String field) {
    final JsonNode node = parent.get(field);
    if (node == null || node.isNull()) {
      throw new IllegalArgumentException("Benchmark file is missing required field: " + field);
    }
    return node.asText();
  }

  /**
   * Returns the benchmark title (used as the {@code results} key in the report).
   *
   * @return the benchmark title
   */
  @Nonnull
  public String getTitle() {
    return title;
  }

  /**
   * Returns the declared FHIR version.
   *
   * @return the FHIR version
   */
  @Nonnull
  public String getFhirVersion() {
    return fhirVersion;
  }

  /**
   * Returns the dataset recipe descriptor.
   *
   * @return the dataset descriptor
   */
  @Nonnull
  public BenchmarkDataset getDataset() {
    return dataset;
  }

  /**
   * Returns the benchmark cases in file order.
   *
   * @return the cases
   */
  @Nonnull
  public List<BenchmarkCase> getCases() {
    return cases;
  }

  /**
   * Returns the number of warmup iterations to discard.
   *
   * @return the warmup iteration count
   */
  public int getWarmup() {
    return warmup;
  }

  /**
   * Returns the number of measured iterations to record.
   *
   * @return the measured iteration count
   */
  public int getMeasurement() {
    return measurement;
  }
}
