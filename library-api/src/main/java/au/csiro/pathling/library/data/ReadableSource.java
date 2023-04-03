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

package au.csiro.pathling.library.data;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.aggregate.AggregateQueryExecutor;
import au.csiro.pathling.aggregate.AggregateRequest;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.extract.ExtractQueryExecutor;
import au.csiro.pathling.extract.ExtractRequest;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.query.AggregateQuery;
import au.csiro.pathling.library.query.ExtractQuery;
import au.csiro.pathling.library.query.QueryExecutor;
import au.csiro.pathling.query.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents a client that can execute complex queries such as extract or aggregate on a
 * {@link DataSource}.
 */
@Value
@EqualsAndHashCode(callSuper = false)
@NonFinal
public class ReadableSource extends QueryExecutor {

  @Nonnull
  FhirContext fhirContext;

  @Nonnull
  SparkSession sparkSession;

  @Nonnull
  DataSource dataSource;

  @Nonnull
  QueryConfiguration configuration;

  @Nonnull
  Optional<TerminologyServiceFactory> terminologyClientFactory;

  /**
   * A builder of {@link ReadableSource} that allows for explicit assignment of the underlying
   * {@link DataSource}.
   */
  public static class Builder extends AbstractSourceBuilder<Builder> {

    @Nullable
    private DataSource dataSource;

    protected Builder(@Nonnull final PathlingContext pathlingContext) {
      super(pathlingContext);
    }

    @Nonnull
    @Override
    protected DataSource buildDataSource() {
      return requireNonNull(dataSource);
    }

    /**
     * Sets the {@link DataSource} to be used by the client.
     *
     * @param dataSource the data source.
     * @return this builder.
     */
    @Nonnull
    public Builder withDataSource(@Nonnull final DataSource dataSource) {
      this.dataSource = dataSource;
      return this;
    }
  }

  /**
   * Creates a new extract query bound to this client with given subject resource type.
   *
   * @param subjectResourceType the type of subject resource for the query.
   * @return the new instance of extract query.
   */
  @Nonnull
  public ExtractQuery extract(@Nonnull final ResourceType subjectResourceType) {
    return ExtractQuery.of(subjectResourceType).withClient(this);
  }

  /**
   * Creates a new extract query bound to this client with given subject resource.
   *
   * @param subjectResourceCode the code of subject resource for the query.
   * @return the new instance of extract query.
   */
  @Nonnull
  public ExtractQuery extract(@Nonnull final String subjectResourceCode) {
    return extract(ResourceType.fromCode(subjectResourceCode));
  }

  /**
   * Creates a new aggregate query bound to this client with given subject resource type.
   *
   * @param subjectResourceType the type of subject resource for the query.
   * @return the new instance of aggregate query.
   */
  @Nonnull
  public AggregateQuery aggregate(@Nonnull final ResourceType subjectResourceType) {
    return AggregateQuery.of(subjectResourceType).withClient(this);
  }

  /**
   * Creates a new aggregate query bound to this client with given subject resource.
   *
   * @param subjectResourceCode the code of subject resource for the query.
   * @return the new instance of aggregate query.
   */
  @Nonnull
  public AggregateQuery aggregate(@Nonnull final String subjectResourceCode) {
    return aggregate(ResourceType.fromCode(subjectResourceCode));
  }

  /**
   * Creates a builder for pathling client.
   *
   * @param pathlingContext the context to build the client for.
   * @return the new instance of the pathling client builder.
   */
  @Nonnull
  public static Builder builder(@Nonnull final PathlingContext pathlingContext) {
    return new Builder(pathlingContext);
  }

  @Override
  @Nonnull
  protected Dataset<Row> execute(@Nonnull final ExtractRequest extractRequest) {
    return new ExtractQueryExecutor(
        configuration,
        fhirContext,
        sparkSession,
        dataSource,
        terminologyClientFactory
    ).buildQuery(extractRequest);
  }

  @Override
  @Nonnull
  protected Dataset<Row> execute(@Nonnull final AggregateRequest aggregateRequest) {
    return new AggregateQueryExecutor(
        configuration,
        fhirContext,
        sparkSession,
        dataSource,
        terminologyClientFactory
    ).buildQuery(aggregateRequest).getDataset();
  }
  
}
