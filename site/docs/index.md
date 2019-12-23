---
layout: page
title: Documentation
has_children: true
---

# Documentation

Pathling is a [HL7 FHIR&reg;](https://hl7.org/fhir/) server that implements
additional functionality designed to ease the delivery of analytics-enabled apps
and augment tasks related to health data analytics.

You can find some examples of how to interact with Pathling in our
[Postman](https://www.getpostman.com/) collection:

<div class="postman-run-button" data-postman-action="collection/import" data-postman-var-1="9fc2fce275013bd782c3"></div>
<script type="text/javascript">
  (function (p,o,s,t,m,a,n) {
    !p[s] && (p[s] = function () { (p[t] || (p[t] = [])).push(arguments); });
    !o.getElementById(s+t) && o.getElementsByTagName("head")[0].appendChild((
      (n = o.createElement("script")),
      (n.id = s+t), (n.async = 1), (n.src = m), n
    ));
  }(window, document, "_pm", "PostmanRunObject", "https://run.pstmn.io/button.js"));
</script>

## Functionality

The diagram below shows the functionality that we envision to be comprised
within a "FHIR Analytics Server". Pathling has currently only implemented the
[import](./import.html) and [aggregate](./aggregate.html) operations - but this
serves to give you an idea of the bigger picture that we are working towards.

See [Roadmap](./roadmap.html) for more information about new features that are
currently under development.

<img src="/images/analytics-api.png" 
     srcset="/images/analytics-api@2x.png 2x, /images/analytics-api.png 1x"/>

## FHIRPath

Pathling uses a language called
[FHIRPath](https://hl7.org/fhirpath/2018Sep/index.html) to facilitate the
description of expressions in requests to these operations. FHIRPath is a
language that is capable of navigating and extracting data from within the graph
of resources and complex data types that FHIR uses as its data model. It
provides a convenient way for us to abstract away the complexity of navigating
FHIR data structures that is required when using more general query languages
such as SQL.

You can get further information about supported syntax and functions within
FHIRPath [here](./fhirpath.html).

## Cluster execution

Pathling has the ability to integrate with
[Apache Spark](https://spark.apache.org/) in order to enable the execution of
queries and other operations with the help of a distributed computing cluster.

You can get further information about this functionality
[here](./deployment.html).
