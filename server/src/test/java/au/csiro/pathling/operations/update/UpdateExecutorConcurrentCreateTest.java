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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Regression tests for the concurrent first-create race in {@link UpdateExecutor}.
 *
 * <p>When two or more threads call {@code merge} for a resource type that has no Delta table in the
 * warehouse yet, all of them can pass the table-existence check before any of them has written, so
 * every loser of the race fails its {@code ErrorIfExists} write with {@code DELTA_PATH_EXISTS}. The
 * fix serialises only the create-new-table branch with a per-resource-code lock and rechecks
 * existence inside it, so the loser recovers by merging into the freshly created table.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class UpdateExecutorConcurrentCreateTest {

  private static final String RESOURCE_CODE = "ViewDefinition";

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  @Autowired private CacheableDatabase cacheableDatabase;

  private Path tempDatabasePath;

  @BeforeEach
  void setUp() throws IOException {
    tempDatabasePath = Files.createTempDirectory("concurrent-create-test-");
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
   * Two threads released simultaneously each create the first ever resource of a type. Both calls
   * must complete without error and both resources must be persisted (FR-001, acceptance scenario
   * 1).
   */
  @Test
  void twoConcurrentCreatesBothSucceed() throws Exception {
    final List<Throwable> failures = raceCreates(2);

    assertThat(failures).isEmpty();
    assertThat(readPersistedIds()).containsExactlyInAnyOrder("vd-0", "vd-1");
  }

  /**
   * More than two threads racing the same first create must all succeed and persist their resources
   * (edge case: more than two concurrent creates).
   */
  @Test
  void fourConcurrentCreatesAllSucceed() throws Exception {
    final List<Throwable> failures = raceCreates(4);

    assertThat(failures).isEmpty();
    assertThat(readPersistedIds()).containsExactlyInAnyOrder("vd-0", "vd-1", "vd-2", "vd-3");
  }

  /**
   * A single uncontended create must behave exactly as before the fix: the table is created and the
   * resource is persisted (acceptance scenario 2, regression guard).
   */
  @Test
  void uncontendedCreateSucceeds() {
    final UpdateExecutor executor = newExecutor(tempDatabasePath.toAbsolutePath().toString());

    executor.merge(RESOURCE_CODE, createViewDefinition("vd-solo"));

    assertThat(readPersistedIds()).containsExactly("vd-solo");
  }

  /**
   * A create that fails for a reason unrelated to concurrent table creation - here a database path
   * whose parent is a regular file, so the table directory cannot be created - must still surface
   * its original error rather than being masked by the race handling (FR-002, acceptance scenario
   * 3).
   */
  @Test
  void unrelatedCreateFailureStillSurfaces() throws IOException {
    final Path blockingFile = tempDatabasePath.resolve("not-a-directory");
    Files.createFile(blockingFile);
    final UpdateExecutor executor = newExecutor(blockingFile.toAbsolutePath().toString());

    assertThatThrownBy(() -> executor.merge(RESOURCE_CODE, createViewDefinition("vd-fail")))
        .isInstanceOf(Exception.class);
  }

  // ---- helpers ----

  /**
   * Releases {@code threadCount} threads from a {@link CyclicBarrier}, each calling {@code merge}
   * on a shared executor for the same previously absent resource type with a distinct resource ID
   * ({@code vd-0} to {@code vd-n}), and returns any exceptions the calls threw.
   */
  @Nonnull
  private List<Throwable> raceCreates(final int threadCount) throws Exception {
    final UpdateExecutor executor = newExecutor(tempDatabasePath.toAbsolutePath().toString());
    final CyclicBarrier barrier = new CyclicBarrier(threadCount);
    final List<Throwable> failures = new CopyOnWriteArrayList<>();
    final ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    try {
      final List<Future<?>> tasks =
          IntStream.range(0, threadCount)
              .mapToObj(
                  i ->
                      pool.submit(
                          () -> {
                            try {
                              final ViewDefinitionResource resource =
                                  createViewDefinition("vd-" + i);
                              barrier.await(30, TimeUnit.SECONDS);
                              executor.merge(RESOURCE_CODE, resource);
                            } catch (final Throwable t) {
                              failures.add(t);
                            }
                          }))
              .collect(Collectors.toList());
      for (final Future<?> task : tasks) {
        task.get(120, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }
    return new ArrayList<>(failures);
  }

  /** Reads the IDs of all resources persisted in the test table. */
  @Nonnull
  private List<String> readPersistedIds() {
    final Path tablePath = tempDatabasePath.resolve(RESOURCE_CODE + ".parquet");
    return pathlingContext
        .getSpark()
        .read()
        .format("delta")
        .load(tablePath.toAbsolutePath().toString())
        .select("id")
        .collectAsList()
        .stream()
        .map(row -> row.getString(0))
        .collect(Collectors.toList());
  }

  @Nonnull
  private UpdateExecutor newExecutor(@Nonnull final String databasePath) {
    final StorageConfiguration storageConfiguration = new StorageConfiguration();
    return new UpdateExecutor(
        pathlingContext,
        fhirEncoders,
        databasePath,
        cacheableDatabase,
        storageConfiguration,
        mock(QueryableDataSource.class));
  }

  @Nonnull
  private ViewDefinitionResource createViewDefinition(@Nonnull final String id) {
    final ViewDefinitionResource viewDef = new ViewDefinitionResource();
    viewDef.setId(id);
    viewDef.setName(new StringType("view_" + id.replace('-', '_')));
    viewDef.setResource(new CodeType("Patient"));
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
