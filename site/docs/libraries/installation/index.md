---
sidebar_position: 1
title: Installation
sidebar_label: Installation
---

import {
JavaInstallation,
PythonInstallation,
ScalaInstallation
} from "../../../src/components/installation";

### Python

<PythonInstallation/>

Once you have Python and pip installed, the command to install the Pathling
package is:

```
pip install pathling
```

### Scala

<ScalaInstallation/>

To add the Pathling library to your project, add the following to
your [SBT](https://www.scala-sbt.org/) configuration:

```scala
libraryDependencies += "au.csiro.pathling" % "library-api" % "[version]"
```

### Java

<JavaInstallation/>

To add the Pathling library to your project, add the following to
your `pom.xml`:

```xml

<dependency>
    <groupId>au.csiro.pathling</groupId>
    <artifactId>library-api</artifactId>
    <version>[version]</version>
</dependency>
```

### Java Virtual Machine

All variants of the Pathling library require version 11 of a Java Virtual
Machine (JVM) to be installed. We recommend
using Azul OpenJDK, you can download installers for all major operating systems
at the [Azul OpenJDK](https://www.azul.com/downloads/?version=java-11-lts) web
site.

Ensure that the `JAVA_HOME` environment variable is set to the location of the
installation of Java 11.
