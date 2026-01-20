# Installation

### Python[​](#python "Direct link to Python")

Prerequisites:

* Python 3.9+ with pip

To install, run this command:

```
pip install pathling  
```

### R[​](#r "Direct link to R")

Prerequisites:

* R >= 3.5.0 (tested with 4.3.1)

To install, run these commands:

```
# Install the `pathling` package.
install.packages('pathling')

# Install the Spark version required by Pathling.
pathling::pathling_install_spark()
```

### Scala[​](#scala "Direct link to Scala")

To add the Pathling library to your project, add the following to your [SBT](https://www.scala-sbt.org/) configuration:

```
libraryDependencies += "au.csiro.pathling" % "library-runtime" % "[version]"
```

### Java[​](#java "Direct link to Java")

To add the Pathling library to your project, add the following to your `pom.xml`:

```

<dependency>
    <groupId>au.csiro.pathling</groupId>
    <artifactId>library-runtime</artifactId>
    <version>[version]</version>
</dependency>
```

### Java Virtual Machine[​](#java-virtual-machine "Direct link to Java Virtual Machine")

All variants of the Pathling library require version 21 of a Java Virtual Machine (JVM) to be installed. We recommend using Azul OpenJDK, you can download installers for all major operating systems at the [Azul OpenJDK](https://www.azul.com/downloads/?version=java-21-lts#zulu) website.

Ensure that the `JAVA_HOME` environment variable is set to the location of the installation of Java 21.
