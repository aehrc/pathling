package au.csiro.pathling.io;

import static java.util.Objects.requireNonNull;

import io.delta.tables.DeltaMergeBuilder;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A persistence scheme that stores Delta tables at a nominated file system location.
 *
 * @author John Grimes
 */
public class FileSystemPersistence implements PersistenceScheme {

  @Nonnull
  protected final SparkSession spark;

  @Nonnull
  protected final String path;

  public FileSystemPersistence(@Nonnull final SparkSession spark, @Nonnull final String path) {
    this.spark = spark;
    this.path = convertS3ToS3aUrl(path);
  }

  @Nonnull
  @Override
  public DeltaTable read(@Nonnull final ResourceType resourceType) {
    return DeltaTable.forPath(spark, getTableUrl(path, resourceType));
  }

  @Override
  public void write(@Nonnull final ResourceType resourceType,
      @Nonnull final DataFrameWriter<Row> writer) {
    writer.save(getTableUrl(path, resourceType));
  }

  @Override
  public void merge(@Nonnull final ResourceType resourceType,
      @Nonnull final DeltaMergeBuilder merge) {
    merge.execute();
  }

  @Override
  public boolean exists(@Nonnull final ResourceType resourceType) {
    return DeltaTable.isDeltaTable(spark, getTableUrl(path, resourceType));
  }

  @Override
  public void invalidate(@Nonnull final ResourceType resourceType) {
  }

  @Nonnull
  @Override
  public Set<ResourceType> list() {
    try {
      final Stream<FileStatus> files = Stream.of(
          getFileSystem(spark, path).listStatus(new Path(path)));
      return files
          .map(FileStatus::getPath)
          .map(Path::getName)
          .map(fileName -> fileName.replace(".parquet", ""))
          .map(FileSystemPersistence::resourceTypeFromCode)
          .filter(Optional::isPresent)
          .map(Optional::get)
          .collect(Collectors.toSet());
    } catch (final IOException e) {
      throw new RuntimeException("Problem listing resources", e);
    }
  }

  @Nonnull
  private static Optional<ResourceType> resourceTypeFromCode(@Nullable final String code) {
    try {
      return Optional.ofNullable(ResourceType.fromCode(code));
    } catch (final FHIRException e) {
      return Optional.empty();
    }
  }

  /**
   * @param path the URL of the warehouse location
   * @param resourceType the resource type to be read or written to
   * @return the URL of the resource within the warehouse
   */
  @Nonnull
  protected static String getTableUrl(@Nonnull final String path,
      @Nonnull final ResourceType resourceType) {
    return safelyJoinPaths(path, fileNameForResource(resourceType));
  }

  /**
   * @param resourceType A HAPI {@link ResourceType} describing the type of resource
   * @return The filename that should be used
   */
  @Nonnull
  private static String fileNameForResource(@Nonnull final ResourceType resourceType) {
    return resourceType.toCode() + ".parquet";
  }

  /**
   * Joins two paths together, taking care that URLs, Unix-style paths and Windows-style paths are
   * catered for.
   *
   * @param first the first path
   * @param second the second path
   * @return the joined path
   */
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
      throw new RuntimeException("Problem accessing location: " + location, e);
    } catch (final URISyntaxException e) {
      throw new RuntimeException("Problem parsing URL: " + location, e);
    }
    requireNonNull(warehouseLocation);
    return warehouseLocation;
  }

  /**
   * @param s3Url The S3 URL that should be converted
   * @return A S3A URL
   */
  @Nonnull
  public static String convertS3ToS3aUrl(@Nonnull final String s3Url) {
    return s3Url.replaceFirst("s3:", "s3a:");
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
      log.info("Renaming result to: " + departitionedUrl);
      partitionedLocation.rename(new Path(targetFile), new Path(departitionedUrl));
      log.info("Cleaning up: " + partitionedUrl);
      partitionedLocation.delete(partitionedPath, true);
    } catch (final IOException e) {
      throw new RuntimeException("Problem copying partition file", e);
    }
    return departitionedUrl;
  }

}
