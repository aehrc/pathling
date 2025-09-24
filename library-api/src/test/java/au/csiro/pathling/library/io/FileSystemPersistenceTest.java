package au.csiro.pathling.library.io;

import static org.junit.jupiter.api.Assertions.assertFalse;
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
