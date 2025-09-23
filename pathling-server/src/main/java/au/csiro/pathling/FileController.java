package au.csiro.pathling;

import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Felix Naumann
 * 
 */
@RestController
public class FileController {

  private final String databasePath;

  public FileController(@Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}") String databasePath) {
    this.databasePath = databasePath;
  }

  @GetMapping("/jobs/{jobId}/{filename}")
  public ResponseEntity<Resource> serveFile(
      @PathVariable("jobId") String jobId,
      @PathVariable("filename") String filename
      ) {

    Path requestedFilePath = new Path(URI.create(databasePath).getPath() + Path.SEPARATOR + "jobs" + Path.SEPARATOR + jobId + Path.SEPARATOR + filename);
    Resource resource = new FileSystemResource(requestedFilePath.toString());
    
    if(!resource.exists() || !resource.isFile()) {
      return ResponseEntity.notFound().build();
    }
    return ResponseEntity.ok()
        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
        .body(resource);
  }
}
