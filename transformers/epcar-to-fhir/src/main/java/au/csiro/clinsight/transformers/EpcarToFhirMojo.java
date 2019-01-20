/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.transformers;

import au.csiro.clinsight.TerminologyClient;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HapiContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.hl7.fhir.dstu3.model.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author John Grimes
 */
@Mojo(name = "transform")
public class EpcarToFhirMojo extends AbstractMojo {

  private static final Logger logger = LoggerFactory.getLogger(EpcarToFhirMojo.class);
  private FhirContext fhirContext;

  @Parameter(property = "inputPath", required = true)
  private String inputPath;

  @Parameter(property = "outputPath", required = true)
  private String outputPath;

  @Parameter(property = "terminologyServerUrl", required = true)
  private String terminologyServerUrl;

  @Override
  public void execute() throws MojoExecutionException {
    if (Files.exists(Paths.get(outputPath))) {
      logger.info("Output file already exists, skipping execution: " + outputPath);
      return;
    }
    try {
      long start = System.nanoTime();
      fhirContext = FhirContext.forDstu3();
      HapiContext v2Context = new DefaultHapiContext();
      TerminologyClient terminologyClient = new TerminologyClient(fhirContext,
          terminologyServerUrl);
      EpcarToFhirTransformer transformer = new EpcarToFhirTransformer(v2Context, terminologyClient);

      FileWriter fileWriter = new FileWriter(outputPath);
      try {
        fileWriter = new FileWriter(outputPath);
        File[] files = new File(inputPath).listFiles();
        assert files != null;
        transformFiles(transformer, fileWriter, files);
        double elapsedSecs = TimeUnit.MILLISECONDS
            .convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        logger.info(
            "Wrote transactions to " + outputPath + " in " + String.format("%.1f", elapsedSecs)
                + " ms");
      } finally {
        fileWriter.close();
      }
    } catch (Exception e) {
      throw new MojoExecutionException("Error occurred during execution", e);
    }
  }

  private void transformFiles(EpcarToFhirTransformer transformer, FileWriter fileWriter,
      File[] files)
      throws Exception {
    for (int i = 0; i < files.length; i++) {
      logger.info("Processing file " + (i + 1) + " of " + files.length + ": " + files[i].getPath());
      String message = IOUtils.toString(new FileInputStream(files[i]), StandardCharsets.UTF_8);
      Bundle result = transformer.transform(message);
      String resultJson = fhirContext.newJsonParser().encodeResourceToString(result);
      fileWriter.write(resultJson + "\n");
    }
  }

}
