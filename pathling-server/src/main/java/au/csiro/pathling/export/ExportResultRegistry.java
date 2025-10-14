package au.csiro.pathling.export;

import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Felix Naumann
 */
@Slf4j
@Component
@Profile("core")
public class ExportResultRegistry extends ConcurrentHashMap<String, ExportResult> {

  private static final long serialVersionUID = -3960163244304628646L;
  
  private final SparkSession sparkSession;
  private final String jobsDir;

  public ExportResultRegistry(SparkSession sparkSession, @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}/jobs") String jobsDir) {
    this.sparkSession = sparkSession;
    this.jobsDir = jobsDir;
  }

  public void initFromStart() {
    try {
      Configuration configuration = sparkSession.sparkContext().hadoopConfiguration();
      FileSystem fs = FileSystem.get(configuration);
      Path jobsPath = new Path(jobsDir);
      if(!fs.exists(jobsPath)) {
        log.debug("No existing jobs found at {}", jobsDir);
        return;
      }
      
      for(FileStatus status : fs.listStatus(jobsPath)) {
        Path jobPath = status.getPath();
        if(!status.isDirectory()) {
          continue;
        }
        log.debug("Found existing job {}", jobPath.toString());
        Path jobMetadataFile = new Path(jobPath, "job_metadata.txt");
        if(!fs.exists(jobMetadataFile)) {
          log.debug("Did not find {}, skipping loading.", jobMetadataFile);
          continue;
        }
        try (FSDataInputStream in = fs.open(jobMetadataFile);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {

          String content = reader.lines().collect(Collectors.joining("\n"));
          String jobId = status.getPath().getName();
          String ownerId = !content.isBlank() ? content : null;
          log.debug("Loading jobId={}, ownerId={}", jobId, ownerId);
          put(jobId, new ExportResult(Optional.ofNullable(ownerId)));
        }
      }
    } catch (IOException e) {
      log.error("Failed loading job result registry.", e);
    }

  }
}
