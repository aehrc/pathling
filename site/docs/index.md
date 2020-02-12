---
layout: page
title: Documentation
has_children: true
---

# Documentation

Pathling is a server based on the
[HL7&reg; FHIR&reg; standard](https://hl7.org/fhir/R4/), implementing special
functionality designed to ease the delivery of analytics-enabled apps and
augment tasks related to health data analytics.

You can find some examples of how to interact with Pathling in our
[Postman](https://www.getpostman.com/) collection:

<a class="postman-link"
   href="https://documenter.getpostman.com/view/634774/S17rx9Af?version=latest">
<img src="https://run.pstmn.io/button.svg" alt="Run in Postman"/></a>

## Functionality

The diagram below shows the functionality that we envision to be comprised
within a "FHIR Analytics Server". Pathling has currently only implemented the
[import](./import.html) and [aggregate](./aggregate.html) operations - but this
serves to give you an idea of the bigger picture that we are working towards.

See [Roadmap](./roadmap) for more information about new features that are
currently under development.

<img src="/images/analytics-api.png" 
     srcset="/images/analytics-api@2x.png 2x, /images/analytics-api.png 1x"
     alt="FHIR Analytics API" />

## FHIRPath

Pathling uses a language called
[FHIRPath](https://hl7.org/fhirpath/2018Sep/index.html) to facilitate the
description of expressions within requests to these operations. FHIRPath is a
language that is capable of navigating and extracting data from within the graph
of resources and data types that FHIR uses as its data model. It provides a
convenient way for us to abstract away the complexity of navigating FHIR data
structures that we ecounter when using more general query languages such as SQL.

You can get further information about supported syntax and functions within
FHIRPath [here](./fhirpath).

## Cluster execution

Pathling has the ability to integrate with
[Apache Spark](https://spark.apache.org/) in order to enable the execution of
queries and other operations with the help of a distributed computing cluster.
This is useful when the volume of data is large enough to warrant the use of
compute and memory resources from more than a single server.

You can get further information about this functionality
[here](./deployment.html).
