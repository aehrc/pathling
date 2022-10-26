# Overview

Pathling is a set of tools that make it easier to
use [FHIR&reg;](https://hl7.org/fhir) within data analytics. It is built
on [Apache Spark](https://spark.apache.org), and includes both language
libraries and a server implementation.

It is primarily aimed at the following use cases:

1. **Exploratory data analysis** – Exploration of hypotheses, assessment of
   assumptions, and selection of appropriate statistical tools and techniques.
2. **Patient cohort selection** – Selection and retrieval of patient records
   based
   upon complex inclusion and exclusion criteria.
3. **Data preparation** – Processing and re-shaping data in preparation for use
   with statistical and machine learning tools.

## Components

There are three main components that are provided as part of Pathling:

1. **Encoders** - a library that can turn FHIR data into Spark data sets, ready
   for SQL query or use within Spark applications;
2. **Language libraries** - libraries that help you use FHIR data within data
   analytics workflows and applications;
4. **Server** - a FHIR server implementation that can provide query services for
   analytics applications.

![Components](../src/images/components.svg#light-mode-only "Components")
![Components](../src/images/components-dark.svg#dark-mode-only "Components")

## FHIRPath

Pathling implements a language called [FHIRPath](https://hl7.org/fhirpath/) as a
way of referring to FHIR data within your queries. It helps to reduce the
complexity of navigating FHIR data structures, as compared to more general query
languages such as SQL.

You can get further information about supported syntax and functions within
FHIRPath [here](/docs/fhirpath).

## Licensing and attribution

Pathling is a product of the
[Australian e-Health Research Centre, CSIRO](https://aehrc.csiro.au), licensed
under the
[Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
This means that you are free to use, modify and redistribute the software as
you wish, even for commercial purposes.

If you use Pathling within your research, please consider citing it using the
instructions on our [GitHub page](https://github.com/aehrc/pathling).

**Pathling is experimental software, use it at your own risk!** You can get a
full description of the current set of known issues over on our
[GitHub page](https://github.com/aehrc/pathling/issues).
