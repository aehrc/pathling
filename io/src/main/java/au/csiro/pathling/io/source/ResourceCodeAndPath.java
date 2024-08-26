package au.csiro.pathling.io.source;

import org.jetbrains.annotations.NotNull;

/**
 * A resource code and an associated path containing the resource data.
 *
 * @param code the resource code
 * @param path the path to the resource data
 * @author John Grimes
 */
public record ResourceCodeAndPath(@NotNull String code, @NotNull String path) {

}
