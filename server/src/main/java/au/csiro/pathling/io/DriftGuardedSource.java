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
import au.csiro.pathling.library.PathlingContext;
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
 * A {@link QueryableDataSource} wrapper that guards reads against schema drift. Reads of a drifted
 * type fail with {@link SchemaDriftError} instead of an opaque Spark analysis failure. View queries
 * are guarded on their subject resource type when the query is constructed, because the executed
 * query resolves datasets through the wrapped source's own dispatcher rather than the guarded
 * {@code read}.
 *
 * <p>Deriving through {@code map}, {@code filterByResourceType} or {@code cache} produces a {@link
 * DerivedSource} over this source rather than over the delegate. The derived source therefore reads
 * through this class's guarded {@code read}, so the guard propagates without being re-applied, and
 * so does whatever read behaviour a subclass adds - dynamic discovery, an empty-dataset fallback, a
 * pinned version.
 *
 * <p>The drifted types set is held by reference, so guard decisions reflect its current contents.
 * This allows a mutable set shared with a refreshing source to clear the guard when a type is
 * successfully migrated.
 *
 * @author John Grimes
 */
public class DriftGuardedSource implements QueryableDataSource {

  /** The Pathling context, used to construct sources derived from this one. */
  @Nonnull protected final PathlingContext context;

  /** The underlying QueryableDataSource that guarded operations delegate to. */
  @Nonnull protected final QueryableDataSource delegate;

  /** The resource types whose tables are drifted and unmigrated. */
  @Nonnull protected final Set<String> driftedTypes;

  /**
   * Constructs a new DriftGuardedSource.
   *
   * @param context the Pathling context, used to construct derived sources
   * @param delegate the underlying QueryableDataSource to delegate to
   * @param driftedTypes the resource types whose tables are drifted and unmigrated; held by
   *     reference so that mutations are reflected in guard decisions
   */
  public DriftGuardedSource(
      @Nonnull final PathlingContext context,
      @Nonnull final QueryableDataSource delegate,
      @Nonnull final Set<String> driftedTypes) {
    this.context = context;
    this.delegate = delegate;
    this.driftedTypes = driftedTypes;
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
    return new DerivedSource(context, this, operator, DerivedSource.RETAIN_ALL_TYPES, driftedTypes);
  }

  @Override
  @Nonnull
  public QueryableDataSource filterByResourceType(
      @Nonnull final Predicate<String> resourceTypePredicate) {
    return new DerivedSource(
        context, this, DerivedSource.IDENTITY_OPERATOR, resourceTypePredicate, driftedTypes);
  }

  @Override
  @Nonnull
  public DataSource cache() {
    return new DerivedSource(
        context,
        this,
        (resourceType, dataset) -> dataset.cache(),
        DerivedSource.RETAIN_ALL_TYPES,
        driftedTypes);
  }

  /**
   * Fails with a {@link SchemaDriftError} if the given resource type is marked as drifted.
   *
   * @param resourceCode the resource type code to check, or null to skip the check
   */
  protected final void checkNotDrifted(@Nullable final String resourceCode) {
    if (resourceCode != null && driftedTypes.contains(resourceCode)) {
      throw new SchemaDriftError(resourceCode);
    }
  }
}
