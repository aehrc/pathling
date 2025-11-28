package au.csiro.pathling.operations.bulkimport;

import au.csiro.pathling.library.io.SaveMode;
import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;

/**
 * Represents a bulk data import request aligned with the SMART Bulk Data Import specification.
 *
 * @param originalRequest The original request URL.
 * @param inputSource URI for tracking the imported data throughout its lifecycle (required by SMART
 * specification).
 * @param input A map of resource type to associated directories/files to load.
 * @param saveMode The save mode to use throughout the entire import operation.
 * @param importFormat The expected input format (NDJSON, Parquet, or Delta).
 * @author Felix Naumann
 */
public record ImportRequest(
    @Nonnull String originalRequest,
    @Nonnull String inputSource,
    @Nonnull Map<String, Collection<String>> input,
    @Nonnull SaveMode saveMode,
    @Nonnull ImportFormat importFormat
) {

}
