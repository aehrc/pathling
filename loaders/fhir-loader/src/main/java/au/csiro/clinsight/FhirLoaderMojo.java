/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * @author John Grimes
 */
@Mojo(name = "load")
public class FhirLoaderMojo extends AbstractMojo {

  @Parameter(property = "jdbcUrl", required = true)
  private String jdbcUrl;

  @Parameter(property = "jdbcDriver", required = true)
  private String jdbcDriver;

  @Parameter(property = "terminologyServerUrl", required = true)
  private String terminologyServerUrl;

  @Parameter(property = "transactionUrl")
  private String transactionUrl;

  @Parameter(property = "ndjsonUrl")
  private String ndjsonUrl;

  @Parameter(property = "autoDdl", defaultValue = "update")
  private String autoDdl;

  @Override
  public void execute() throws MojoExecutionException {
    try {
      FhirLoader fhirLoader = new FhirLoader(jdbcUrl, terminologyServerUrl, jdbcDriver, autoDdl);
      if (transactionUrl != null) {
        fhirLoader.processTransaction(transactionUrl);
      } else if (ndjsonUrl != null) {
        fhirLoader.processNdjsonFile(ndjsonUrl);
      } else {
        throw new MojoExecutionException("Must provide either transactionUrl or ndjsonUrl");
      }
    } catch (Exception e) {
      throw new MojoExecutionException("Error occurred during execution: ", e);
    }
  }

}
