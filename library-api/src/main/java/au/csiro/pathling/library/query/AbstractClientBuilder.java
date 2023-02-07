package au.csiro.pathling.library.query;

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.query.DataSource;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Abstract base class for builders of {@link PathlingClient}.
 *
 * @param <T> the actual class of the builder.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class AbstractClientBuilder<T extends AbstractClientBuilder> {

  @Nonnull
  protected final PathlingContext pathlingContext;

  @Nonnull
  protected QueryConfiguration queryConfiguration = QueryConfiguration.builder().build();

  protected AbstractClientBuilder(@Nonnull final PathlingContext pathlingContext) {
    this.pathlingContext = pathlingContext;
  }

  /**
   * Sets the query configuration to use with the client.
   *
   * @param queryConfiguration the query configuration.
   * @return this builder.
   */
  @Nonnull
  public T withQueryConfiguration(@Nonnull final QueryConfiguration queryConfiguration) {
    this.queryConfiguration = queryConfiguration;
    return (T) this;
  }

  /**
   * Builds a new instance of {@link PathlingClient} with the configuration defined by this
   * builder.
   *
   * @return the configured instance of pathling client.
   */
  @Nonnull
  public PathlingClient build() {
    return new PathlingClient(pathlingContext.getFhirContext(), pathlingContext.getSpark(),
        buildDataSource(), queryConfiguration,
        Optional.of(pathlingContext.getTerminologyServiceFactory()));
  }

  /**
   * This is a shortcut to directly build an {@link  ExtractQuery} bound to the pathling client
   * created by this builder.
   *
   * @param subjectResourceType the type of the subject resource for the extract query.
   * @return the new instance of extract query.
   */
  @Nonnull
  public ExtractQuery buildExtractQuery(@Nonnull final ResourceType subjectResourceType) {
    return this.build().newExtractQuery(subjectResourceType);
  }

  @Nonnull
  protected abstract DataSource buildDataSource();
}
