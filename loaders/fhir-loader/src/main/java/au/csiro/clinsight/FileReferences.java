/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class FileReferences {

  public static InputStream urlToStream(URL url) throws IOException {
    switch (url.getProtocol()) {
      case "file":
        return fileUrlToStream(url);
      default:
        throw new RuntimeException("Not implemented");
    }
  }

  private static InputStream fileUrlToStream(URL url) throws FileNotFoundException {
    String path = url.getPath();
    return new FileInputStream(path);
  }

}
