---
sidebar_position: 1
title: Installation
sidebar_label: Installation
---

### Python

Prerequisites:

- Python 3.8+ with pip

To install, run this command:

```
pip install pathling  
```

### R

Prerequisites: 

- R >= 3.5.0 (tested with 4.3.1)

To install, run these commands:

```r
# Install the `remotes` package.
install.packages('remotes')

# Install the `pathling` package.
remotes::install_url('https://pathling.csiro.au/R/pathling_6.4.0.tar.gz', upgrade = FALSE)

# Install the Spark version required by Pathling.
pathling::pathling_install_spark()
```

### Scala

To add the Pathling library to your project, add the following to
your [SBT](https://www.scala-sbt.org/) configuration:

```scala
libraryDependencies += "au.csiro.pathling" % "library-api" % "[version]"
```

### Java

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
