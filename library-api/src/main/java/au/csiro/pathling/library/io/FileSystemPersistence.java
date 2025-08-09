package au.csiro.pathling.library.io;

import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

/**
 * Utility methods relating to persisting data to the filesystem.
 *
 * @author John Grimes
 */
@Slf4j
public class FileSystemPersistence {

  private FileSystemPersistence() {
  }

  /**
   * Joins two paths together, taking care that URLs, Unix-style paths and Windows-style paths are
   * catered for.
   *
   * @param first the first path
   * @param second the second path
   * @return the joined path
   */
  @Nonnull
  public static String safelyJoinPaths(@Nonnull final String first, @Nonnull final String second) {
    try {
      final URI uri = URI.create(first);
      return uri.toString().replaceFirst("/$", "") + "/" + second;
    } catch (final IllegalArgumentException e) {
      return java.nio.file.Path.of(first, second).toString();
    }
  }

  /**
   * Get a Hadoop {@link FileSystem} for the given location.
   *
   * @param spark the Spark session
   * @param location the location URL to be accessed
   * @return the {@link FileSystem} for the given location
   * @throws PersistenceError if there is a problem accessing the location
   * @throws IllegalArgumentException if the location URL is malformed
   */
  @Nonnull
  public static FileSystem getFileSystem(@Nonnull final SparkSession spark,
      @Nonnull final String location) {
    @Nullable final org.apache.hadoop.conf.Configuration hadoopConfiguration = spark.sparkContext()
        .hadoopConfiguration();
    requireNonNull(hadoopConfiguration);
    @Nullable final FileSystem warehouseLocation;
    try {
      warehouseLocation = FileSystem.get(new URI(location), hadoopConfiguration);
    } catch (final IOException e) {
      throw new PersistenceError("Problem accessing location: " + location, e);
    } catch (final URISyntaxException e) {
      throw new IllegalArgumentException("Problem parsing URL: " + location, e);
    }
    requireNonNull(warehouseLocation);
    return warehouseLocation;
  }

  /**
   * Convert a directory containing a single file partition into a single file.
   *
   * @param spark the Spark session
   * @param partitionedLocation the location URL containing the partitioned file
   * @param departitionedLocation the desired URL of the resulting file
   * @param extension the file extension used within the partitioned directory
   * @return the location of the resulting file
   */
  @Nonnull
  public static String departitionResult(@Nonnull final SparkSession spark,
      @Nonnull final String partitionedLocation,
      @Nonnull final String departitionedLocation, @Nonnull final String extension) {
    return departitionResult(getFileSystem(spark, partitionedLocation), partitionedLocation,
        departitionedLocation, extension);
  }

  /**
   * Convert a directory containing a single file partition into a single file.
   *
   * @param partitionedLocation a Hadoop {@link FileSystem} representing the location that both the
   * partitioned and departitioned files are located in
   * @param partitionedUrl the URL of the partitioned file
   * @param departitionedUrl the desired URL of the resulting file
   * @param extension the file extension used within the partitioned directory
   * @return the location of the resulting file
   * @throws PersistenceError if there is a problem copying the partition file
   */
  @Nonnull
  public static String departitionResult(@Nonnull final FileSystem partitionedLocation,
      @Nonnull final String partitionedUrl, @Nonnull final String departitionedUrl,
      @Nonnull final String extension) {
    try {
      final Path partitionedPath = new Path(partitionedUrl);
      final FileStatus[] partitionFiles = partitionedLocation.listStatus(partitionedPath);
      final String targetFile = Arrays.stream(partitionFiles)
          .map(f -> f.getPath().toString())
          .filter(f -> f.endsWith("." + extension))
          .findFirst()
          .orElseThrow(() -> new IOException("Partition file not found"));
      log.info("Renaming result to: {}", departitionedUrl);
      partitionedLocation.rename(new Path(targetFile), new Path(departitionedUrl));
      log.info("Cleaning up: {}", partitionedUrl);
      partitionedLocation.delete(partitionedPath, true);
    } catch (final IOException e) {
      throw new PersistenceError("Problem copying partition file", e);
    }
    return departitionedUrl;
  }

}
