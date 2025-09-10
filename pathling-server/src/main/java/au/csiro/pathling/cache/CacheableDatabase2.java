package au.csiro.pathling.cache;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.io.FileSystemPersistence;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.Set;

/**
 * @author Felix Naumann
 */
@Component
public class CacheableDatabase2 implements DataSource, Cacheable {

    // TODO - I need access to the CacheableDatabase in fhirpath, but that's not accessible
    // Try to implement it manually (again) but there is too much stuff going on
    // Surely there's a better way than just


    @Override
    public Optional<String> getCacheKey() {
        return Optional.empty();
    }

    @Override
    public boolean cacheKeyMatches(String otherKey) {
        return false;
    }

    @Override
    public @NotNull Dataset<Row> read(@Nullable String resourceCode) {
        return null;
    }

    @Override
    public @NotNull Set<String> getResourceTypes() {
        return Set.of();
    }
}
