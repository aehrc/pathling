---
sidebar_position: 2
---

# Windows installation

Installing Pathling (or Apache Spark) on Windows requires a few extra steps.
This is because the Hadoop libraries that Spark depends upon require special
binaries to run on Windows.

To solve this problem, you can do the following:

1. Go to the [winutils](https://github.com/steveloughran/winutils) repository,
   click the "Code" button and download the latest release as a ZIP file.
2. Extract the downloaded archive to a directory of your choice. For example,
   you can extract it to `C:\hadoop`.
3. Set the `HADOOP_HOME` environment variable to the `hadoop-3.0.0` subdirectory
   within the installation directory, e.g. `c:\hadoop\hadoop-3.0.0`.

You can set an environment variable in Windows by right-clicking on "This PC"
and selecting "Properties", then clicking on "Advanced system settings", then
clicking on "Environment Variables". You will need administrator privileges on
the computer to be able to do this.

As with other operating systems, you also need to have Java 11 installed and
the `JAVA_HOME` environment variable set to its installation location. We
recommend
the [Azul Zulu installer for Windows](https://www.azul.com/downloads/?version=java-11-lts&os=windows&package=jdk#zulu).
