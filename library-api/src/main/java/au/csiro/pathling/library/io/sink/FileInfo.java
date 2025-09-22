package au.csiro.pathling.library.io.sink;

import au.csiro.pathling.io.source.DataSource;
import jakarta.validation.constraints.NotNull;

/**
 * Encapsulate information about a file that is the result of
 * {@link NdjsonSink#write(DataSource)}.
 * 
 * @param fhirResourceType The FHIR resource type code (as a string) this file belongs to.
 * @param absoluteUrl The download url. It may be required to have a controller in between to resolve it to an actual file.
 * @param count Convenience information about the amount of resources in this file. 
 * 
 * @author Felix Naumann
 */
public record FileInfo(@NotNull String fhirResourceType, @NotNull String absoluteUrl, long count) {

}
