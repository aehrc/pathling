package au.csiro.pathling.operations.import_;

import au.csiro.pathling.library.io.SaveMode;
import jakarta.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import java.util.Collection;
import java.util.Map;

/**
 * 
 * @param originalRequest The original request URL
 * @param input a map of resource type and associated directory/file to load
 * @param saveMode the save mode to use throughout the entire import operation
 * @param importFormat the expected input format
 * @param lenient lenient handling enabled
 * 
 * @author Felix Naumann
 */
public record ImportRequest(
    @Nonnull String originalRequest,
    @Nonnull Map<String, Collection<String>> input,
    @Nonnull SaveMode saveMode,
    @Nonnull ImportFormat importFormat,
    boolean lenient
) {

}
