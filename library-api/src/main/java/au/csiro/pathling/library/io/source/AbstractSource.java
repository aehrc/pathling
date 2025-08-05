/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.library.io.source;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.query.FhirViewQuery;
import au.csiro.pathling.library.query.QueryDispatcher;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * Provides common functionality for all queryable data sources.
 *
 * @author John Grimes
 */
public abstract class AbstractSource implements QueryableDataSource {

  @Nonnull
  protected final PathlingContext context;

  @Nonnull
  protected final QueryDispatcher dispatcher;

  protected AbstractSource(@Nonnull final PathlingContext context) {
    this.context = context;
    dispatcher = buildDispatcher(context, this);
  }

  @Nonnull
  private QueryDispatcher buildDispatcher(final @Nonnull PathlingContext context,
      final DataSource dataSource) {
    final FhirViewExecutor viewExecutor = new FhirViewExecutor(context.getFhirContext(),
        context.getSpark(), dataSource
    );

    // Build the dispatcher using the executors.
    return new QueryDispatcher(viewExecutor);
  }

  @Nonnull
  @Override
  public DataSinkBuilder write() {
    return new DataSinkBuilder(context, this);
  }

  @Nonnull
  @Override
  public FhirViewQuery view(@Nullable final String subjectResource) {
    requireNonNull(subjectResource);
    return new FhirViewQuery(dispatcher, subjectResource, context.getGson());
  }

  @Nonnull
  @Override
  public FhirViewQuery view(@Nullable final FhirView view) {
    requireNonNull(view);
    return new FhirViewQuery(dispatcher, view.getResource(), context.getGson()).view(view);
  }
}
