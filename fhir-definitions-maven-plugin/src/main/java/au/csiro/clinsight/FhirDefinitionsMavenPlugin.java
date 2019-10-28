/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.Optional;
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
 * Downloads the definitions archive from the FHIR specification, extracts the relevant bundles and
 * combines them into a single bundle.
 *
 * @author John Grimes
 */
@Mojo(name = "bundle", defaultPhase = LifecyclePhase.GENERATE_RESOURCES)
public class FhirDefinitionsMavenPlugin extends AbstractMojo {

  private static final Logger logger = LoggerFactory.getLogger(FhirDefinitionsMavenPlugin.class);

  @Parameter(defaultValue = "${project.build.directory}/fhir-definitions")
  private String outputDirectory;

  @Parameter(defaultValue = "http://www.hl7.org/fhir/definitions.json.zip")
  private String downloadUrl;

  @Parameter(required = true)
  private String[] sourceFiles;

  public void execute() throws MojoExecutionException {
    try {
      // Ensure that the output directory exists.
      new File(outputDirectory).mkdirs();

      // Check if the definitions bundle has already been created.
      String resultFileName = outputDirectory + "/fhir-definitions.Bundle.json";
      File buildDirectoryFile = new File(outputDirectory);
      File resultFile = new File(resultFileName);
      if (resultFile.exists()) {
        logger.info("Skipping execution, file already exists: " + resultFileName);
        return;
      }

      // This is required to force the use of the Woodstox StAX implementation. If you don't use
      // Woodstox, parsing falls over when reading in resources (e.g. StructureDefinitions) that
      // contain HTML entities.
      //
      // See: http://hapifhir.io/download.html#_toc_stax__woodstox
      System.setProperty("javax.xml.stream.XMLInputFactory", "com.ctc.wstx.stax.WstxInputFactory");
      System
          .setProperty("javax.xml.stream.XMLOutputFactory", "com.ctc.wstx.stax.WstxOutputFactory");
      System.setProperty("javax.xml.stream.XMLEventFactory", "com.ctc.wstx.stax.WstxEventFactory");

      // Initialise the FHIR context.
      FhirContext fhirContext = FhirContext.forR4();
      IParser jsonParser = fhirContext.newJsonParser();

      // Create a temporary file.
      File downloadedFile = File.createTempFile("fhir-definitions-", ".zip");

      // Download the definitions ZIP file and save it to the temporary file.
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

      // Extract each specified file from the ZIP file and add its entries into a combined bundle.
      Bundle resultBundle = new Bundle();
      resultBundle.setType(BundleType.COLLECTION);
      for (String sourceFile : sourceFiles) {
        logger
            .info("Extracting \"" + sourceFile + "\" from ZIP file");
        ZipFile zipFile = new ZipFile(downloadedFile);
        ZipEntry entry = zipFile.getEntry(sourceFile);

        // Get an XML or JSON parser, based upon the file extension.
        Optional<String> extension = getExtensionFromFileName(sourceFile);
        if (!extension.isPresent()) {
          throw new RuntimeException(
              "Could not determine type of source file from extension: " + sourceFile);
        }
        IParser parser = extension.get().equals("xml")
            ? fhirContext.newXmlParser()
            : jsonParser;

        Bundle bundle = (Bundle) parser.parseResource(zipFile.getInputStream(entry));
        resultBundle.getEntry().addAll(bundle.getEntry());
      }

      // Write the resulting bundle to the build directory.
      logger.info("Writing combined bundle: " + resultFileName);
      buildDirectoryFile.mkdirs();
      if (!resultFile.createNewFile()) {
        throw new RuntimeException("Unable to create output file: " + resultFileName);
      }
      try (FileWriter resultFileWriter = new FileWriter(resultFile)) {
        jsonParser.encodeResourceToWriter(resultBundle, resultFileWriter);
      }

      // Clean up temporary download file.
      if (!downloadedFile.delete()) {
        throw new RuntimeException(
            "Unable to delete temporary download file: " + downloadedFile.getAbsolutePath());
      }
    } catch (Exception e) {
      throw new MojoExecutionException("Error occurred while executing plugin", e);
    }
  }

  private static Optional<String> getExtensionFromFileName(String filename) {
    return Optional.ofNullable(filename)
        .filter(f -> f.contains("."))
        .map(f -> f.substring(filename.lastIndexOf(".") + 1));
  }
}
