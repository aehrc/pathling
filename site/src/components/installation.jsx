import React from "react";

export const PythonInstallation = () => (
  <p>
    To use the Pathling encoders from Python, install the{" "}
    <a href="https://pypi.org/project/pathling/">pathling</a> package using{" "}
    <a href="https://pip.pypa.io/">pip</a>. Note that Java 11 is required, with your{" "}
    <code>JAVA_HOME</code> properly set.
  </p>
);

export const ScalaInstallation = () => (
  <p>
    To use the Pathling encoders from Scala, install the <code>au.csiro.pathling:encoders</code>{" "}
    <a href="https://maven.apache.org/">Maven</a> package.
  </p>
);

export const JavaInstallation = () => (
  <p>
    To use the Pathling encoders from Java, install the <code>au.csiro.pathling:encoders</code>{" "}
    <a href="https://maven.apache.org/">Maven</a> package.
  </p>
);
