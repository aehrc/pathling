/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.net.URL;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * @author John Grimes
 */
@Mojo(name = "load")
public class FhirLoaderMojo extends AbstractMojo {

  @Parameter(property = "sparkMasterUrl", required = true)
  private String sparkMasterUrl;

  @Parameter(property = "jsonBundlesDirectory")
  private String jsonBundlesDirectory;

  @Parameter(property = "ndjsonUrl")
  private String ndjsonUrl;

  @Override
  public void execute() throws MojoExecutionException {
    try {
      checkArgument(jsonBundlesDirectory != null || ndjsonUrl != null,
          "Must supply either jsonBundlesDirectory or ndjsonUrl");
      FhirLoader fhirLoader = new FhirLoader(sparkMasterUrl);
      if (jsonBundlesDirectory != null) {
        fhirLoader.processJsonBundles(new File(jsonBundlesDirectory));
      } else {
        fhirLoader.processNdjsonFile(new URL(ndjsonUrl));
      }
    } catch (Exception e) {
      throw new MojoExecutionException("Error occurred during execution: ", e);
    }
  }

}
