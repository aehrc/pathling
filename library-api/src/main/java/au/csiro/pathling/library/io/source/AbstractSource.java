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

import static au.csiro.pathling.fhir.FhirUtils.getResourceType;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.aggregate.AggregateQueryExecutor;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.extract.ExtractQueryExecutor;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.query.AggregateQuery;
import au.csiro.pathling.library.query.ExtractQuery;
import au.csiro.pathling.library.query.FhirViewQuery;
import au.csiro.pathling.library.query.QueryDispatcher;
import au.csiro.pathling.views.FhirViewExecutor;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

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
    // Use the default query configuration.
    final QueryConfiguration queryConfiguration = QueryConfiguration.builder().build();

    // Build executors for the aggregate and extract queries using the context, configuration and 
    // this data source.
    final AggregateQueryExecutor aggregateExecutor = new AggregateQueryExecutor(queryConfiguration,
        context.getFhirContext(), context.getSpark(), dataSource,
        Optional.of(context.getTerminologyServiceFactory()));
    final ExtractQueryExecutor extractExecutor = new ExtractQueryExecutor(queryConfiguration,
        context.getFhirContext(), context.getSpark(), dataSource,
        Optional.of(context.getTerminologyServiceFactory()));
    final FhirViewExecutor viewExecutor = new FhirViewExecutor(context.getFhirContext(),
        context.getSpark(), dataSource
    );

    // Build the dispatcher using the executors.
    return new QueryDispatcher(aggregateExecutor, extractExecutor, viewExecutor);
  }

  @Nonnull
  @Override
  public DataSinkBuilder write() {
    return new DataSinkBuilder(context, this);
  }

  @Nonnull
  @Override
  public AggregateQuery aggregate(@Nullable final ResourceType subjectResource) {
    return new AggregateQuery(dispatcher, requireNonNull(subjectResource));
  }

  @Nonnull
  @Override
  public ExtractQuery extract(@Nullable final ResourceType subjectResource) {
    return new ExtractQuery(dispatcher, requireNonNull(subjectResource));
  }

  @Nonnull
  @Override
  public AggregateQuery aggregate(@Nullable final String subjectResource) {
    return aggregate(getResourceType(subjectResource));
  }

  @Nonnull
  @Override
  public ExtractQuery extract(@Nullable final String subjectResource) {
    return extract(getResourceType(subjectResource));
  }

  @Nonnull
  @Override
  public FhirViewQuery view(@Nullable final ResourceType subjectResource) {
    return new FhirViewQuery(dispatcher, requireNonNull(subjectResource), context.getGson());
  }

  @Nonnull
  @Override
  public FhirViewQuery view(@Nullable final String subjectResource) {
    return view(getResourceType(subjectResource));
  }

}
