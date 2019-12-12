/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.commons.io.IOUtils;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads the definitions archive from the FHIR specification, extracts the specified bundles and
 * copies them into the output directory.
 *
 * @author John Grimes
 */
@Mojo(name = "bundle", defaultPhase = LifecyclePhase.GENERATE_RESOURCES)
public class FhirDefinitionsMavenPlugin extends AbstractMojo {

  private static final Logger logger = LoggerFactory.getLogger(FhirDefinitionsMavenPlugin.class);

  @Parameter(defaultValue = "${project.build.directory}/fhir-definitions-zip")
  private String downloadDirectory;

  @Parameter(defaultValue = "${project.build.directory}/fhir-definitions")
  private String outputDirectory;

  @Parameter(defaultValue = "http://www.hl7.org/fhir/R4/definitions.json.zip")
  private String downloadUrl;

  @Parameter(required = true)
  private String[] sourceFiles;

  public void execute() throws MojoExecutionException {
    try {
      FileSystem fileSystem = FileSystems.getDefault();

      // Ensure that the download and output directories exist.
      new File(downloadDirectory).mkdirs();
      new File(outputDirectory).mkdirs();

      // Create a file for the source ZIP file in the download directory.
      Path downloadedFilePath = fileSystem.getPath(downloadDirectory, "fhir-definitions.zip");
      File downloadedFile = downloadedFilePath.toFile();

      // Check if the source ZIP file has already been downloaded - if so, skip the download.
      if (downloadedFile.exists()) {
        logger.info("Skipping download, file already exists: " + downloadedFilePath.toString());
      } else {
        // Download the definitions ZIP file and save it to the download directory.
        logger.info("Downloading: " + downloadUrl);
        HttpGet httpGet = new HttpGet(downloadUrl);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
          StatusLine statusLine = response.getStatusLine();
          if (statusLine.getStatusCode() != 200) {
            throw new RuntimeException(
                "Download was unsuccessful: " + statusLine.getStatusCode() + " " + statusLine
                    .getReasonPhrase());
          }

          try (FileOutputStream downloadedFileStream = new FileOutputStream(downloadedFile)) {
            IOUtils.copy(response.getEntity().getContent(), downloadedFileStream);
          }
        }
      }

      // Extract each specified file from the ZIP file and write it to the output directory.
      Bundle resultBundle = new Bundle();
      resultBundle.setType(BundleType.COLLECTION);
      for (String sourceFile : sourceFiles) {
        ZipFile zipFile = new ZipFile(downloadedFile);
        ZipEntry entry = zipFile.getEntry(sourceFile);
        Path targetPath = FileSystems.getDefault().getPath(outputDirectory, entry.getName());
        logger.info("Copying \"" + sourceFile + "\" from ZIP file to: " + targetPath.toString());

        Files.copy(zipFile.getInputStream(entry), targetPath, StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (Exception e) {
      throw new MojoExecutionException("Error occurred while executing plugin", e);
    }
  }

}
