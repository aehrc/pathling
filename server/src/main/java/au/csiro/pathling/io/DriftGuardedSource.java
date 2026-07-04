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

package au.csiro.pathling.io;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.library.query.FhirViewQuery;
import au.csiro.pathling.views.FhirView;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A {@link QueryableDataSource} wrapper that carries the schema drift guard of a {@link
 * DynamicDeltaSource} into sources derived from it through {@code map} or {@code
 * filterByResourceType}. Reads of a drifted type fail with {@link SchemaDriftError} instead of an
 * opaque Spark analysis failure. View queries are guarded on their subject resource type when the
 * query is constructed, because the executed query resolves datasets through the wrapped source's
 * own dispatcher rather than the guarded {@code read}.
 *
 * <p>The drifted types are snapshotted at construction; derived sources are created per request, so
 * they reflect the drift state at the time of derivation.
 *
 * @author John Grimes
 */
public class DriftGuardedSource implements QueryableDataSource {

  @Nonnull private final QueryableDataSource delegate;

  @Nonnull private final Set<String> driftedTypes;

  /**
   * Constructs a new DriftGuardedSource.
   *
   * @param delegate the underlying QueryableDataSource to delegate to
   * @param driftedTypes the resource types whose tables are drifted and unmigrated
   */
  public DriftGuardedSource(
      @Nonnull final QueryableDataSource delegate, @Nonnull final Set<String> driftedTypes) {
    this.delegate = delegate;
    this.driftedTypes = Set.copyOf(driftedTypes);
  }

  @Override
  @Nonnull
  public Dataset<Row> read(@Nullable final String resourceCode) {
    checkNotDrifted(resourceCode);
    return delegate.read(resourceCode);
  }

  @Override
  @Nonnull
  public Set<String> getResourceTypes() {
    return delegate.getResourceTypes();
  }

  @Override
  @Nonnull
  public DataSinkBuilder write() {
    return delegate.write();
  }

  @Override
  @Nonnull
  public FhirViewQuery view(@Nullable final String subjectResource) {
    checkNotDrifted(subjectResource);
    return delegate.view(subjectResource);
  }

  @Override
  @Nonnull
  public FhirViewQuery view(@Nullable final FhirView view) {
    if (view != null) {
      checkNotDrifted(view.getResource());
    }
    return delegate.view(view);
  }

  @Override
  @Nonnull
  public QueryableDataSource map(
      @Nonnull final BiFunction<String, Dataset<Row>, Dataset<Row>> operator) {
    return new DriftGuardedSource(delegate.map(operator), driftedTypes);
  }

  @Override
  @Nonnull
  public QueryableDataSource filterByResourceType(
      @Nonnull final Predicate<String> resourceTypePredicate) {
    return new DriftGuardedSource(
        delegate.filterByResourceType(resourceTypePredicate), driftedTypes);
  }

  @Override
  @Nonnull
  public DataSource cache() {
    final DataSource cached = delegate.cache();
    return cached instanceof final QueryableDataSource queryable
        ? new DriftGuardedSource(queryable, driftedTypes)
        : cached;
  }

  private void checkNotDrifted(@Nullable final String resourceCode) {
    if (resourceCode != null && driftedTypes.contains(resourceCode)) {
      throw new SchemaDriftError(resourceCode);
    }
  }
}
