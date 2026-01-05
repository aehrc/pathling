package au.csiro.pathling.library.io.sink;

import jakarta.validation.constraints.NotNull;

/**
 * Encapsulate information about a file that is the result of a write operation.
 *
 * @param fhirResourceType The FHIR resource type code (as a string) this file belongs to.
 * @param absoluteUrl The download url. It may be required to have a controller in between to
 *     resolve it to an actual file.
 * @author Felix Naumann
 */
public record FileInformation(@NotNull String fhirResourceType, @NotNull String absoluteUrl) {}
