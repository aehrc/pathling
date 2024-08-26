package au.csiro.pathling.io.source;

import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.jetbrains.annotations.NotNull;

/**
 * A resource type and an associated path containing the resource data.
 *
 * @param type the {@link ResourceType} of the resource
 * @param path the path to the resource data
 * @author John Grimes
 */
public record ResourceTypeAndPath(@NotNull ResourceType type, @NotNull String path) {

}
