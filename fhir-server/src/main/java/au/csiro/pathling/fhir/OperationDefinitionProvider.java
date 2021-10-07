/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static au.csiro.pathling.utilities.Versioning.getMajorVersion;

import au.csiro.pathling.PathlingVersion;
import au.csiro.pathling.errors.ResourceNotFoundError;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.server.IResourceProvider;
import com.google.common.collect.ImmutableMap;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.r4.model.OperationDefinition;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * This class is used for serving up the OperationDefinition resources which describe this server's
 * FHIR API.
 *
 * @author John Grimes
 */
@Component
@Profile("server")
public class OperationDefinitionProvider implements IResourceProvider {

  /**
   * All system-level operations available within Pathling.
   */
  public static final List<String> SYSTEM_LEVEL_OPERATIONS = Arrays.asList("import", "extract",
      "result", "job");

  /**
   * All resource-level operations available within Pathling.
   */
  private static final List<String> RESOURCE_LEVEL_OPERATIONS = Arrays.asList("aggregate",
      "search");

  /**
   * All operations available within Pathling.
   */
  private static final List<String> OPERATIONS;

  static {
    OPERATIONS = new ArrayList<>();
    OPERATIONS.addAll(SYSTEM_LEVEL_OPERATIONS);
    OPERATIONS.addAll(RESOURCE_LEVEL_OPERATIONS);
  }

  private static final String UNKNOWN_VERSION = "UNKNOWN";

  @Nonnull
  private final IParser jsonParser;

  @Nonnull
  private final Map<String, OperationDefinition> resources;

  @Nonnull
  private final PathlingVersion version;

  private OperationDefinitionProvider(@Nonnull final IParser jsonParser,
      @Nonnull final PathlingVersion version) {
    this.jsonParser = jsonParser;
    this.version = version;

    final ImmutableMap.Builder<String, OperationDefinition> mapBuilder = new ImmutableMap.Builder<>();
    for (final String operation : OPERATIONS) {
      final String id =
          "OperationDefinition/" + operation + "-" + getMajorVersion(
              version.getMajorVersion().orElse("UNKNOWN"));
      final String path = "fhir/" + operation + ".OperationDefinition.json";
      mapBuilder.put(id, load(path));
    }
    resources = mapBuilder.build();
  }

  @Override
  public Class<? extends IBaseResource> getResourceType() {
    return OperationDefinition.class;
  }

  /**
   * Handles all read requests to the OperationDefinition resource.
   *
   * @param id the ID of the desired OperationDefinition
   * @return an {@link OperationDefinition} resource
   */
  @Read
  @SuppressWarnings("unused")
  public OperationDefinition getOperationDefinitionById(@Nullable @IdParam final IIdType id) {
    checkUserInput(id != null, "Missing ID parameter");

    final String idString = id.getValue();
    final OperationDefinition resource = resources.get(idString);
    if (resource == null) {
      throw new ResourceNotFoundError("OperationDefinition not found: " + idString);
    }
    return resource;
  }

  @Nonnull
  private OperationDefinition load(@Nonnull final String resourcePath) {
    @Nullable final InputStream resourceStream = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream(resourcePath);
    checkNotNull(resourceStream);

    final OperationDefinition operationDefinition = (OperationDefinition) jsonParser
        .parseResource(resourceStream);
    final String id = String
        .format("%1$s%2$s", operationDefinition.getName(),
            version.getMajorVersion().map(v -> String.format("-%1$s", v)).orElse(""));
    operationDefinition.setId(id);
    final String url = String
        .format("%1$s/OperationDefinition/%2$s", ConformanceProvider.URI_BASE, id);
    operationDefinition.setUrl(url);
    operationDefinition.setVersion(version.getBuildVersion().orElse(UNKNOWN_VERSION));
    return operationDefinition;
  }

}
