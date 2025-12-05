package au.csiro.pathling.library.io;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * @author Felix Naumann
 */
class FileSystemPersistenceTest {

  @TempDir
  private File tempDir;
  
  private FileSystem fs;
  private Path hadoopPath;
  
  @BeforeEach
  void setup() throws IOException {
    fs = FileSystem.get(new Configuration());
    hadoopPath = new Path(tempDir.toURI());
    copyTestFiles(fs, "src/test/resources/test-data/spark-partitioned-data/", hadoopPath.toString());
    TestDataFileLogger.logDirectoryContents(hadoopPath);
  }
  
  @Test
  void test_files_are_renamed_following_pathling_pattern() throws IOException {
    Path partitionPath = new Path(hadoopPath, "Patient.ndjson");
    assumeTrue(fs.exists(partitionPath), "Expected partition path %s to exist.".formatted(partitionPath.toString()));
    
    FileSystemPersistence.renamePartitionedFiles(fs, partitionPath.toString(), partitionPath.toString(), "txt");
    TestDataFileLogger.logDirectoryContents(hadoopPath);
    
    // assert that the files have been renamed
    assertTrue(fs.exists(new Path(hadoopPath, "Patient.00000.ndjson")));
    assertTrue(fs.exists(new Path(hadoopPath, "Patient.00001.ndjson")));
    assertTrue(fs.exists(new Path(hadoopPath, "Patient.00002.ndjson")));
    assertTrue(fs.exists(new Path(hadoopPath, "Patient.00003.ndjson")));
    
    // assert that the partitioned files have been deleted
    assertFalse(fs.exists(new Path(hadoopPath, "Patient.ndjson")));
  }

  @Test
  void test_throws_exception_when_partition_files_not_found() {
    String partitionPath = new Path(hadoopPath, "NonExistent.ndjson").toString();

    PersistenceError exception = assertThrows(PersistenceError.class, () ->
        FileSystemPersistence.renamePartitionedFiles(fs, partitionPath, partitionPath, "txt")
    );

    assertInstanceOf(IOException.class, exception.getCause());
  }

  @Test
  void test_throws_exception_when_no_files_with_expected_extension() throws IOException {
    // Create a directory with files that don't have the expected extension
    Path testDir = new Path(hadoopPath, "NoMatchingExtension.ndjson");
    fs.mkdirs(testDir);

    // Create a file with wrong extension
    Path wrongExtFile = new Path(testDir, "part-00000-hash.json"); // .json instead of .txt
    fs.create(wrongExtFile).close();

    String testDirString = testDir.toString();
    
    PersistenceError exception = assertThrows(PersistenceError.class, () ->
        FileSystemPersistence.renamePartitionedFiles(fs, testDirString, testDirString, "txt")
    );

    assertInstanceOf(IOException.class, exception.getCause());
    assertTrue(exception.getCause().getMessage().contains("Partition file not found"));
  }

  @Test
  void test_throws_exception_for_invalid_departitioned_filename_no_dot() {
    String partitionPath = new Path(hadoopPath, "Patient.ndjson").toString();
    String invalidDepartitionedUrl = new Path(hadoopPath, "InvalidFilename").toString(); // No dot

    PersistenceError exception = assertThrows(PersistenceError.class, () ->
        FileSystemPersistence.renamePartitionedFiles(fs, partitionPath, invalidDepartitionedUrl, "txt")
    );

    assertTrue(exception.getMessage().contains("Unexpected departitioning filename structure"));
    assertTrue(exception.getMessage().contains("exactly one FHIR resource type and the ndjson extension"));
  }

  @Test
  void test_throws_exception_for_invalid_departitioned_filename_multiple_dots() {
    String partitionPath = new Path(hadoopPath, "Patient.ndjson").toString();
    String invalidDepartitionedUrl = new Path(hadoopPath, "Patient.backup.ndjson").toString(); // Multiple dots

    PersistenceError exception = assertThrows(PersistenceError.class, () ->
        FileSystemPersistence.renamePartitionedFiles(fs, partitionPath, invalidDepartitionedUrl, "txt")
    );

    assertTrue(exception.getMessage().contains("Unexpected departitioning filename structure"));
  }

  @Test
  void test_throws_exception_for_partition_file_without_dash() throws IOException {
    // Create a partition directory with a file that doesn't have a dash in the name
    Path testDir = new Path(hadoopPath, "InvalidPartition.ndjson");
    fs.mkdirs(testDir);

    // Create a file without dash in name
    Path invalidFile = new Path(testDir, "part00000hash.txt"); // No dash
    fs.create(invalidFile).close();

    String testDirString = testDir.toString();
    
    PersistenceError exception = assertThrows(PersistenceError.class, () ->
        FileSystemPersistence.renamePartitionedFiles(fs, testDirString, testDirString, "txt")
    );

    assertTrue(exception.getMessage().contains("Unexpected spark partitioning structure"));
    assertTrue(exception.getMessage().contains("partitioned id after the first '-'"));
  }

  @Test
  void test_throws_exception_for_partition_file_with_only_prefix_before_dash() throws IOException {
    // Create a partition directory with a file that has only one part before the dash
    Path testDir = new Path(hadoopPath, "InvalidPartition2.ndjson");
    fs.mkdirs(testDir);

    // Create a file with only prefix before dash (no partition id)
    Path invalidFile = new Path(testDir, "part-.txt"); // Dash but no partition id
    fs.create(invalidFile).close();

    String testDirString = testDir.toString();

    PersistenceError exception = assertThrows(PersistenceError.class, () ->
        FileSystemPersistence.renamePartitionedFiles(fs, testDirString, testDirString, "txt")
    );

    assertTrue(exception.getMessage().contains("Unexpected spark partitioning structure"));
  }
  
  private static void copyTestFiles(FileSystem fs, String from, String to) throws IOException {
    Path fromPath = new Path(from);
    Path toPath = new Path(to);

    // Create destination directory
    fs.mkdirs(toPath);

    // Copy directory contents
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    FileStatus[] files = localFs.listStatus(fromPath);

    for (FileStatus file : files) {
      Path destFile = new Path(toPath, file.getPath().getName());
      fs.copyFromLocalFile(file.getPath(), destFile);
    }
  }
}
