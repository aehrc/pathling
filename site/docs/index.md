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

Pathling is licensed under the
[CSIRO Open Source Software Licence Agreement](https://github.com/aehrc/pathling/blob/master/LICENSE),
which is a permissive licence based upon the BSD and MIT licences. This means
that you are free to use, modify and redistribute the software as you wish, even
for commercial purposes.

**Pathling is experimental software, use it at your own risk!** You can get a
full description of the current set of known issues over on our
[GitHub page](https://github.com/aehrc/pathling/issues).

## Motivation

Health care is a complex domain, and health care information models reflect this
complexity. Linking data with clinical terminology adds a further degree of
complexity, often requiring expert knowledge of coding systems and skill in the
correct use of them within the analytic context.

Generalized tools for data analytics and business intelligence do little to
insulate their users from this complexity, as it is often impractical for them
to cater for requirements unique to specialist areas such as health care. Users
often deal with this complexity by writing large volumes of code to cover the
domain-specific functionality missing from their tools. This code is sometimes
shared in the form of libraries and packages, but is more often written and
maintained in isolation from other users.

Pathling is an attempt to encapsulate a set of functionality useful for health
data analytics application development and workflow into a server implementation
that natively understands FHIR on the way in, and on the way out. This
functionality is designed to service the following use cases:

1. _Exploratory data analysis_ – Exploration of hypotheses, assessment of
   assumptions, and selection of appropriate statistical tools and techniques.
2. _Patient cohort selection_ – Selection and retrieval of patient records based
   upon complex inclusion and exclusion criteria.
3. _Data preparation_ – Processing and re-shaping data in preparation for use
   with statistical and machine learning tools.

## Functionality

The diagram below shows the functionality that we are working towards within
Pathling.

<img src="/images/analytics-api.png" 
     srcset="/images/analytics-api@2x.png 2x, /images/analytics-api.png 1x"
     alt="FHIR Analytics API" />  
In summary, the operations available within Pathling are:

1. [import](./import.html) - An operation for importing FHIR data in bulk into
   the server, making it availbel for query using the other operations.
2. [search](./search.html) - A way of retrieving a set of individual FHIR
   resources that match a set of criteria, described using expressions.
3. [aggregate](./aggregate.html) - A "pivot table as an API", able to take in a
   set of expressions that describe aggregations, groupings and filters and
   return grouped aggregate data.
4. [summarize](./roadmap#summarize-operation) _(currently under development)_

- An operation that allows a client to describe a custom tabular extract of FHIR
  data, and retrieve it in bulk.

You can find some examples of how to interact with Pathling in our
[Postman](https://www.getpostman.com/) collection:

<a class="postman-link"
   href="https://documenter.getpostman.com/view/634774/S17rx9Af?version=latest">
<img src="https://run.pstmn.io/button.svg" alt="Run in Postman"/></a>

See the [Roadmap](./roadmap.html) for more information about the features that
are currently under development.

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
[here](./deployment.html#apache-spark).
