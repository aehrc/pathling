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
 * A {@link QueryableDataSource} that delegates to another source, and whose derived sources read
 * through it rather than through the delegate.
 *
 * <p>Deriving through {@code map}, {@code filterByResourceType} or {@code cache} produces a {@link
 * DerivedSource} over this source rather than over the delegate. The derived source therefore reads
 * through this class's {@code read}, so whatever read behaviour a subclass adds - dynamic
 * discovery, an empty-dataset fallback, a pinned version - propagates to it.
 *
 * @author John Grimes
 */
public class DelegatingSource implements QueryableDataSource {

  /** The Pathling context, used to construct sources derived from this one. */
  @Nonnull protected final PathlingContext context;

  /** The underlying QueryableDataSource that operations delegate to. */
  @Nonnull protected final QueryableDataSource delegate;

  /**
   * Constructs a new DelegatingSource.
   *
   * @param context the Pathling context, used to construct derived sources
   * @param delegate the underlying QueryableDataSource to delegate to
   */
  public DelegatingSource(
      @Nonnull final PathlingContext context, @Nonnull final QueryableDataSource delegate) {
    this.context = context;
    this.delegate = delegate;
  }

  @Override
  @Nonnull
  public Dataset<Row> read(@Nullable final String resourceCode) {
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
    return delegate.view(subjectResource);
  }

  @Override
  @Nonnull
  public FhirViewQuery view(@Nullable final FhirView view) {
    return delegate.view(view);
  }

  @Override
  @Nonnull
  public QueryableDataSource map(
      @Nonnull final BiFunction<String, Dataset<Row>, Dataset<Row>> operator) {
    return new DerivedSource(context, this, operator, DerivedSource.RETAIN_ALL_TYPES);
  }

  @Override
  @Nonnull
  public QueryableDataSource filterByResourceType(
      @Nonnull final Predicate<String> resourceTypePredicate) {
    return new DerivedSource(context, this, DerivedSource.IDENTITY_OPERATOR, resourceTypePredicate);
  }

  @Override
  @Nonnull
  public DataSource cache() {
    return new DerivedSource(
        context, this, (resourceType, dataset) -> dataset.cache(), DerivedSource.RETAIN_ALL_TYPES);
  }
}
