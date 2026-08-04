Search - FHIR v6.0.0-ballot3                  

[![logo fhir](./assets/images/fhir-logo-www.png)](http://hl7.org/fhir)

**FHIR CI-Build**

[![visit the hl7 website](./assets/images/hl7-logo.png)](http://www.hl7.org)

[![Search CI-Build FHIR](./assets/images/search.png)](http://build.fhir.org/search-build.html)

[FHIR](index.html)

*   [Home](./index.html)
*   [Getting Started](./modules.html)
*   [Documentation](./documentation.html)
*   [Data Types](./datatypes.html)
*   [**_Resource Types_**](./resourcelist.html)
*   [Terminologies](./terminologies-systems.html)
*   [Artifacts](index.html)
    *   [Extensions](https://build.fhir.org/ig/HL7/fhir-extensions/extension-registry.html)
    *   [Operations](./operationslist.html)
    *   [Patterns](./patterns.html)
    *   [Profiles](./profilelist.html)
    *   [Search Parameters](./searchparameter-registry.html)
    
    *   [Artifact Registry ![icon](external.png)](http://registry.fhir.org) 
*   [Implementation Guides ![icon](external.png)](http://fhir.org/guides/registry) 

*    [![link](exchange.png) Exchange](exchange-module.html)
*   [RESTful API](http.html)
*   **Search**

This is the Continuous Integration Build of FHIR (will be incorrect/inconsistent at times).  
See the [Directory of published versions ![icon](external.png)](http://hl7.org/fhir/directory.html) 

3.2.1 Search[](search.html#3.2.1 "link to here")
------------------------------------------------

Responsible Owner: [FHIR Infrastructure ![icon](external.png)](http://www.hl7.org/Special/committees/fiwg/index.cfm) Work Group

[Standards Status](versions.html#std-process): [Normative](versions.html#std-process)

Searching for resources is fundamental to the mechanics of FHIR. Search operations traverse through an existing set of resources filtering by parameters supplied to the search operation. The text below describes the FHIR search framework, starting with simple cases moving to the more complex. Implementers need only implement the amount of complexity that they require for their implementations. Servers SHALL declare what features of search they require through their [CapabilityStatement search declarations](capabilitystatement.html#search-decl)[§0](#fcss0), and clients are encouraged to do this too.

### 3.2.1.1 Contents[](search.html#Contents "link to here")

*   [Introduction](#introduction)
    *   [Notes on URL-Encoding](#encoding-note)
    *   [Search Overview](#overview)
    *   [Search Inputs](#searchinputs)
    *   [Search Contexts](#searchcontexts)
    *   [Search Responses](#return)
        *   [Bundle Type](#returnbundletype)
        *   [Self Link - Understanding a Performed Search](#selflink)
        *   [Other Links - Paging](#paginglinks)
        *   [Matches, Inclusions, and Outcomes - Bundle Entries](#entries)
        *   [Handling Errors](#errors)
*   [Transport Protocols](#transportprotocols)
    *   [Multi-datastore Considerations](#multi-datastore)
    *   [Batching Searches](#batching)
    *   [Search in Messaging](#messaging)
*   [Search Parameters](#ptypes)
    *   [Search Test Basics](#matching-basics)
    *   [Matching and Cardinality](#matching-cardinality)
    *   [Matching and Sub-Elements](#matching-backbones)
    *   [Searching Multiple Values (joining with AND or OR)](#multivalue)
    *   [Modifiers](#modifiers)
    *   [Prefixes](#prefix)
    *   [Escaping Search Parameters](#escaping)
    *   [Search Types and FHIR Types](#type-mapping)
    *   Basic Parameter Types: [date](#date), [number](#number), [quantity](#quantity), [reference](#reference), [string](#string), [token](#token), [uri](#uri)
    *   Extended Parameter Types: [composite](#composite), [resource](#resource), [special](#special)
*   [Special Search Conditions](#specialconditions)
    *   [Searching Ranges](#specialranges)
    *   [Searching Identifiers](#specialidentifiers)
        *   [Logical Identifiers](#identifierslogical)
        *   [Searching by Membership](#membership)
        *   [Identifiers and References](#identifierreferences)
        *   [Canonical Identifiers](#identifiercanonical)
    *   [References and Versions](#versions)
    *   [Searching Hierarchies](#recursive)
    *   [Searching MIME Types](#mimetype)
    *   [Chaining (chained parameters)](#chaining)
    *   [Reverse Chaining](#has)
    *   [Handling Missing Data](#missing)
    *   [Advanced Filtering](#filter)
    *   [Searching Multiple Resource Types](#type)
    *   [Implicit Resources](#implicitresources)
    *   [Named Queries](#advanced)
    *   [Searching for Additional Resources](#additional)
*   [Modifying Search Results](#modifyingresults)
    *   [Sorting (\_sort)](#sort)
    *   [Total number of matching resources (\_total)](#total)
    *   [Limiting Page Size (\_count)](#count)
    *   [Limiting Total Result Size (\_maxresults)](#_maxresults)
    *   [Summary (\_summary)](#summary)
    *   [Selecting Elements (\_elements)](#elements)
    *   [Relevance (\_score)](#score)
    *   [Including Referenced Resources](#include)
        *   [Inline Requests (\_include, \_revinclude)](#_include)
        *   [External References](#external)
        *   [Graph Definitions (\_graph)](#_graph)
        *   [Contained Resources (\_contained)](#contained)
        *   [Paging and Other Resources](#paging)
*   [Standard Parameters](#standard)
    *   [Parameters for All Resources](#all)
    *   [Parameters for Each Resource](#parameters)
    *   [Text Search Parameters](#text)
*   [Server Conformance](#conformance)
*   [Search Result Currency](#currency)
*   [Conformance Summary](#conformancesummary)
*   [Summary Tables](#table)

### 3.2.1.2 Introduction[](search.html#introduction "link to here")

FHIR Search is the primary mechanism used to find and list resource instances. The search mechanism is designed to be flexible enough to meet the needs of a wide variety of use cases, and yet be simple enough to be commonly useful.

In a typical RESTful interface, collections of instances are returned as arrays of a type. In order to include related information (e.g., number of total results), support extended functionality (e.g., paging), and allow multiple resource types in results (e.g., returning Patient and Encounter resources), FHIR Search instead returns a [Bundle](bundle.html) resource, with a type of [searchset](bundle.html#searchset). Resources included in the results appear as individual entries in such a bundle. Note that the `_format` parameter works for search [like for other interactions](http.html#mime-type). More information about returned contents and elements can be found in [Managing Returned Resources](#return)

There are safety issues associated with the implementation of searching that implementers need to always keep in mind. Implementers SHOULD review the [safety checklist](safety.html#search).[§1](#fcss1)

#### 3.2.1.2.1 Notes on URL-Encoding[](search.html#encoding-note "link to here")

Note that for the readability of this document, syntaxes and examples are shown _without_ URL-Encoding escaping applied. For example, a syntax of `[url]|[version]` will be URL encoded during transmission as `[url]%7C[version]`. when describing the format of `token` parameters, the vertical pipe character (

|

) is used instead of the URL-Encoded value of

%7C

.

As noted in the [Escaping Search Parameters](#escaping) section, URL Encoding is transparent when processing. Clients and servers SHOULD be robust in their handling of URLs present in the content of FHIR bundles, such as in `Bundle.link.url`.[§2](#fcss2)

For example, the following _unencoded_ request:

Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?patient.identifier=http://example.com/fhir/identifier/mrn|123456 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded patient.identifier=http://example.com/fhir/identifier/mrn|123456

would actually be transmitted over-the-wire as:

Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?patient.identifier=http%3A%2F%2Fexample.com%2Ffhir%2Fidentifier%2Fmrn%7C123456 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded patient.identifier=http%3A%2F%2Fexample.com%2Ffhir%2Fidentifier%2Fmrn%7C123456

#### 3.2.1.2.2 Search Overview[](search.html#overview "link to here")

Though search operations are typically performed via REST, search is defined to be useful independently of HTTP. Note that while different formats of search requests are functionally equivalent, implementation considerations can result in differences inherent to searching via a particular protocol. Details can be found on the [Search section](http.html#search) of the HTTP page.

#### 3.2.1.2.3 Search Inputs[](search.html#searchinputs "link to here")

Input to search operations are referred to as `Search Parameters`. A search parameter can be:

*   a filter across [all resources](#all) (e.g., `_id`, which is present on every resource),
*   a filter on a [specific resource](#parameters) (e.g., `Patient.birthDate`, which applies to the Patient resource,)
*   a [textual search](#content) across a resource, or
*   a parameter that affects [search results](#return).

There is a single page that lists [all the search parameters](searchparameter-registry.html) published with this specification. Note that search parameter names are case sensitive, though this specification never defines different parameters with names that differ only in case. Clients SHOULD use correct case, and servers SHALL NOT define additional parameters with different meanings with names that only differ in case.[§3](#fcss3)

Order of operations is not driven by order in the URL - with the exception of sort. First all filters are applied, then the result set is sorted, then paging is applied, and then included resources (`_include`, `_revinclude`) are added for each page.

The server determines which of their resources meet the criteria contained in the search parameters as described below. However, the server has the prerogative to return additional search results if it believes them to be relevant. Note: There is a special search for the most relevant context in which the search set is indeterminate: [Patient MPI Search](patient.html#match).

In the absence of any search filters, e.g., `GET [base]`, `GET [base]/Patient`, or `POST [base]/_search` or `POST [base]/Patient/_search` with no body, a server SHOULD return all records in scope of the search context.[§4](#fcss4) Servers MAY reject a search as overly-broad, and SHOULD return an appropriate error in that situation (e.g., [too-costly](codesystem-issue-type.html#issue-type-too-costly)).[§5](#fcss5)

For more information about how search inputs are tested against resources, please see [Search Parameters](#ptypes).

#### 3.2.1.2.4 Search Contexts[](search.html#searchcontexts "link to here")

[Search operations](http.html#search) are executed in one of three defined contexts that control which set of resources are being searched:

*   All resource types: Note that if the `_type` parameter is included, all other search parameters SHALL be common to all provided types, and if `_type` is not included, all parameters SHALL be common to all resource types.[§6](#fcss6) Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]?parameter(s) POST \[base\]/\_search Content-Type: application/x-www-form-urlencoded parameter(s)
*   A specified resource type: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/\[type\]?parameter(s) POST \[base\]/\[type\]/\_search Content-Type: application/x-www-form-urlencoded parameter(s)
*   A specified [compartment](compartmentdefinition.html), perhaps with a specified resource type in that compartment. Note that the literal `*` (asterisk) is valid as the `[type]` in a GET-based compartment search. This syntax is for disambiguation between an [instance read](http.html#read) and a compartment search, which would otherwise share the same URL.
    
    The outcome of a compartment search is the same as the equivalent system or type search and will have full URLs that are not compartment-specific (e.g. fullUrls reference `[base]/Observation/[id]`, not `[base]/Patient/[pat-id]/Observation/[id]`). References within returned resources are also not rewritten to include the compartment path.
    
    Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/\[compartment\]/\[id\]/\[type\]?parameter(s) POST \[base\]/\[compartment\]/\[id\]/\[type\]/\_search Content-Type: application/x-www-form-urlencoded parameter(s)

### 3.2.1.3 Search Responses[](search.html#return "link to here")

Search responses are always returned as a [Bundle](bundle.html). Search result bundles convey a lot of metadata in addition to any possible results, using the various elements available in the bundle resource.

The response to any search operation is always a list of resources in a Bundle. An alternative approach is to [use GraphQL](graphql.html).

#### 3.2.1.3.1 Bundle Type[](search.html#returnbundletype "link to here")

Search result bundles will always have the `Bundle.type` of [searchset](bundle.html#searchset). The `Bundle.type` value of `searchset` designates that a bundle is a search response and may be used in processing to classify bundles.

#### 3.2.1.3.2 Self Link - Understanding a Performed Search[](search.html#selflink "link to here")

In order to allow the client to be confident about what search parameters were used as criteria by a server, servers SHALL return the parameters that were actually used to process a search.[§7](#fcss7) Applications processing search results SHALL check these returned values where necessary.[§8](#fcss8) For example, if a server did not support some of the filters specified in the search, a client might manually apply those filters to the retrieved result set, display a warning message to the user or take some other action.

These parameters are encoded in the self link of the returned bundle - a [bundle.link](bundle-definitions.html#Bundle.link) with the `relation` set to `self`: Show as: [FHIR+JSON](#json) | [FHIR+XML](#xml) { "resourceType": "Bundle", "type": "searchset", ... "link": { "relation": "self", "url": "http://example.org/Patient?name=peter" } ... } <Bundle> <type value="searchset"/> ... <link> <relation value="self"/> <url value="http://example.org/Patient?name=peter"/> </link> ... </Bundle>

Self links SHALL be expressed as an HTTP GET-based search with the relevant parameters included as query parameters, because of the semantics around the link types.[§9](#fcss9) This means that the same self link is a valid response for any equivalent search, regardless of how a search is performed (e.g., HTTP GET, HTTP POST, Messaging, etc.).

Self links MAY be returned as absolute URIs or URIs relative to the base URL of a server and MAY or MAY NOT be resolvable.[§10](#fcss10) A server that does not support search via GET will return links in the same style as a server that does not support search via POST. Note that this makes the `self` link special in the context of bundle links, as all other links are intended to be resolvable by a client.

Clients SHALL review the returned parameters in the self link to ensure proper processing of results.[§11](#fcss11) Generally, ignored parameters will result in clients receiving more results than intended. In those situations, clients SHOULD filter received records to account for additional data.[§12](#fcss12)

In some cases, a server might introduce additional search parameters into the executed query based on authorization rules. Servers MAY include such filters in the self link.[§13](#fcss13) Clients SHOULD be able to handle search parameters in a self link that they did not request.[§14](#fcss14) Servers SHOULD consider whether exposing search parameters in the self link would constitute inappropriate leakage of security information.[§15](#fcss15)

#### 3.2.1.3.3 Other Links - Paging[](search.html#paginglinks "link to here")

In addition to the self link, many bundles may contain links relevant to paging. These are identified via the `relation` value in the links. Common links include: `first`, `last`, `next`, `prev`, and `previous`. Note that `prev` and `previous` are widely-used synonyms and are interchangeable.

As with the self link, all relevant paging links SHALL be expressed as GET requests.[§16](#fcss16) Servers SHOULD NOT include content that is considered sensitive in URLs, excluding the self link.[§17](#fcss17)

For best practices on when to include or exclude paging links, see the [Paging Best Practices](http.html#paging-best-practices) section on the FHIR HTTP page.

#### 3.2.1.3.4 Matches, Inclusions, and Outcomes - Bundle Entries[](search.html#entries "link to here")

Within the results bundle, there are three types of [entries](bundle-definitions.html#Bundle.entry) that may be present, identified by the [search mode](bundle-definitions.html#Bundle.entry.search.mode) of the entry: `match`, `include`, or `outcome`.

Note that entries are unique (not allowed to repeat) and there is only one mode per entry. In some corner cases, a resource may be included because it is both a `match` and an `include`. In these circumstances, `match` takes precedence.

##### 3.2.1.3.4.1 Match[](search.html#3.2.1.3.4.1 "link to here")

Entries marked with `match` in search results indicate that a record is being returned because it meets the parameters specified in the search request (e.g., a record the client requested).

##### 3.2.1.3.4.2 Include[](search.html#3.2.1.3.4.2 "link to here")

Entries marked with `include` in search results indicate that a record is being returned either because it is referred to from a record in the result set or that the server believes is useful or necessary for the client to process other results.

##### 3.2.1.3.4.3 Outcome[](search.html#3.2.1.3.4.3 "link to here")

Entries marked with `outcome` in search results are [OperationOutcome](operationoutcome.html) resources with information related to the processing of a search. For example, a server may use an outcome record to report to a client that a search was unacceptable.

#### 3.2.1.3.5 Handling Errors[](search.html#errors "link to here")

If a server is unable to execute a search request, it MAY either return an error for the request or return success with an `outcome` containing details of the error.[§18](#fcss18) A HTTP status code of `403` signifies that the server refused to perform the search, while other `4xx` and `5xx` codes signify that some sort of error has occurred. When the search fails, a server SHOULD return an [OperationOutcome](operationoutcome.html) detailing the cause of the failure.[§19](#fcss19) Note that an empty search result is not an error.

In some cases, parameters may cause an error, or might not be able to match anything. For instance:

Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post)

**Issue**

**Search**

A parameter may refer to a non-existent resource, such as if no Observation with an id of "101" exists

GET \[base\]/Observation?\_id=101 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded \_id=101

A parameter may refer to a non-existent resource, such as if no Patient with MRN "1234" exists

GET \[base\]/Observation?patient.identifier=http://example.com/fhir/mrn|1234 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded patient.identifier=http://example.com/fhir/mrn|1234

A parameter may refer to an unknown code, such as if LOINC code "1234-1" is not known to the server

GET \[base\]/Observation?code=loinc|1234-1 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded code=loinc|1234-1

A parameter may refer to a time that is out of scope, such as if the system only has data going back to 2001

GET \[base\]/Condition?onset=le1995 POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded onset=le1995

A parameter may use an illegal or unacceptable modifier, such as if the modifier cannot be processed by the server

GET \[base\]/Condition?onset:text=1995 POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded onset:text=1995

A data time parameter may have incorrect format, such as if the modifier cannot be processed by the server

GET \[base\]/Condition?onset=23.May.2009 POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded onset=23.May.2009

A parameter may be unknown or unsupported (more details below)

GET \[base\]/Condition?myInvalidParameter=true POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded myInvalidParameter=true

Note: Empty parameters are not an error - they are just ignored by the server.

Where the content of the parameter is syntactically incorrect, servers SHOULD return an error.[§20](#fcss20) However, where the issue is a logical condition (e.g., unknown subject or code), the server SHOULD process the search, including processing the parameter - with the result of returning an empty search set, since the parameter cannot be satisfied.[§21](#fcss21)

In such cases, the search process may include an [OperationOutcome](operationoutcome.html) in the search set that contains additional hints and warnings about the search process. This is included in the search results as an entry with a [search mode](bundle-definitions.html#Bundle.entry.search.mode) of [`outcome`](valueset-search-entry-mode.html). Clients can use this information to improve future searches. If, for example, a client performed the following search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?patient.identifier=http://example.com/fhir/identifier/mrn|123456 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded patient.identifier=http://example.com/fhir/identifier/mrn|123456 and there is no patient for MRN 123456, the server may return a bundle with no results and a warning outcome, such as [this](bundle-search-warning.html).

**Unknown and unsupported parameters**

Servers can receive parameters from the client that they do not recognize, or parameters they recognize but do not support (either in general, or for a specific search). In general, servers SHOULD ignore unknown or unsupported parameters for the following reasons:[§22](#fcss22)

*   Various HTTP stacks and proxies may add parameters that aren't under the control of the client
*   The client can determine what parameters the server used by examining the self link in the return (see [below](#conformance))

Clients can request specific server behavior by using the prefer header:

*   `Prefer: handling=strict`: Client requests that the server return an error for any unknown or unsupported parameter
*   `Prefer: handling=lenient`: Client requests that the server ignore any unknown or unsupported parameter

Servers SHOULD honor the client's `prefer` request, but are not required to do so.[§23](#fcss23) For more information, see [HTTP Headers](http.html#Http-Headers) on the HTTP Page.

### 3.2.1.4 Transport Protocols[](search.html#transportprotocols "link to here")

This specification defines FHIR Search operations in both HTTP `POST` and `GET`. For details about the HTTP methods regarding search, please see the [Search section](http.html#search) of the HTTP page.

#### 3.2.1.4.1 Multi-datastore Considerations[](search.html#multi-datastore "link to here")

Some servers expose a FHIR endpoint that actually represents multiple data stores. For example, a system might expose a single Observation endpoint, even though internally there are distinct systems for labs, vitals, clinical assessments, and symptoms. Alternatively, a system might expose a single Patient endpoint even though it is backed by multiple distinct Patient repositories.

In these "single endpoint/multiple back-end" scenarios, it is possible that the internal repositories have different capabilities when it comes to search.

At present, there is no standard way to declare such limitations as part of the [CapabilityStatement](capabilitystatement.html), other than through custom extensions. A future release of FHIR could provide a more standard way of performing such declarations.

In any event, systems can handle such limitations in _at least_ four ways:

1.  By requiring that some searches include certain mandatory criteria. For example, Observation searches might require that the `category` parameter be present (and only have one value) or Patient searches might require that the `organization` parameter be present. These permitted combinations can be conveyed by extensions on the `CapabilityStatement`. Clients that fail to satisfy the 'combination' rules will not be able to search successfully.
2.  By having the intermediary system that handles the dispatch to different back ends perform supplementary filtering such that any search parameters that cannot be enforced by the back-end system are instead applied at the intermediary layer, and thus from a 'client' perspective the search was executed as desired.
3.  By returning a warning message in the search [OperationOutcome](operationoutcome.html) (see also: [Handling Errors](#errors)) indicating that not all filters were applied on all resources. This SHOULD only occur in situations where it is known that some filters haven't been applied.[§24](#fcss24)
4.  By exposing distinct FHIR endpoints for each data source and the different capabilities in an endpoint-specific manner.

#### 3.2.1.4.2 Batching Search Requests[](search.html#batching "link to here")

Servers MAY support [batching](http.html#transaction) multiple requests.[§25](#fcss25) In the context of search, this allows several searches to be performed serially via a single request. Note that each operation of a `batch` is independent, so it is not possible to use the results of one request as input to another in the same batch.

When submitting multiple requests via a batch or transaction, the returned bundle is for the batch or transaction as an operation. Therefore, two search requests in a batch would return a bundle that _contains_ at least two entries, each of which is the result of a search request (e.g., the batch result bundle will contain entries that are the search result bundles).

When bundling requests via `batch` or `transaction`, HTTP verbs and the URLs for RESTful interfaces are used. When requesting searches in a bundle, systems SHOULD accept searches via GET, even if they do not typically accept GET-based searches.[§26](#fcss26) There is no standard way to convey POST-based searches within a Bundle and the architectural differences when searching via GET or POST are not relevant within a Bundle. Servers receiving searches within a Bundle SHOULD NOT impose any GET-specific limitations on search (i.e., restrictions not imposed on POST-based searches) that they would impose if the GET-based search had been received outside a Bundle.[§27](#fcss27)

#### 3.2.1.4.3 Search via Messaging[](search.html#messaging "link to here")

Servers MAY support FHIR [Messaging](messaging.html).[§28](#fcss28) The mapping of search requests into messages is described in more detail in the section [Invoking Search via Messages](messaging.html#search).

### 3.2.1.5 Search Parameters[](search.html#ptypes "link to here")

While search parameters are typically transmitted as URL-Encoded strings (e.g., HTTP Parameters or `x-www-form-urlencoded` body content), FHIR is modeled with a rich set of [primitive](datatypes.html#primitive) and [complex](datatypes.html#complex) data types to describe the data encountered in healthcare. To provide consistent mapping between the two formats, each search parameter is defined by a type that specifies how the search parameter behaves. These are the defined search parameter types:

[number](#number)

Search parameter SHALL be a number (a whole number, or a decimal).

[date](#date)

Search parameter is on a date/time. The date format is the standard XML format, though other formats may be supported.

[string](#string)

Search parameter is a simple string, like a name part. Search is case-insensitive and accent-insensitive. May match just the start of a string. String parameters may contain spaces.

[token](#token)

Search parameter on a coded element or identifier. May be used to search through the text, display, code and code/codesystem (for codes) and label, system and key (for identifier). Its value is either a string or a pair of namespace and value, separated by a "|", depending on the modifier used.

[reference](#reference)

A reference to another resource (Reference or canonical).

[composite](#composite)

A composite search parameter that combines a search on two values together.

[quantity](#quantity)

A search parameter that searches on a quantity.

[uri](#uri)

A search parameter that searches on a URI (RFC 3986).

[special](#special)

Special logic applies to this parameter per the description of the search parameter.

[resource](#resource)

A search parameter defined to chain through into embedded resources.

Individual search parameters MAY also allow "modifiers" that control their behavior.[§29](#fcss29) The kinds of modifiers available depend on the type of the search parameter. More details can be found in the [Modifiers](#modifiers) section of the search page.

Some search parameter types allow "prefixes" (or "comparators") which can be used to request different match-testing functionality (e.g., greater-than instead of equality). The kinds of comparators allowed depends on the type of the search parameter. More details can be found in the [Prefixes](#prefix) section of the search page.

#### 3.2.1.5.1 Search Test Basics[](search.html#matching-basics "link to here")

Generally, an single search test can be broken down into a few parts:

*   a search input value to test with,
*   the type of test being used, and
*   zero or more values from a resource to test against.

For example, a search against the Patient resource for `given=value` is a request to a server to compare values in the given name of a patient against "value", using the default string test (see [string](#string) for details). Each of these 'parts' of a search test have a lot of flexibility.

Search inputs are dereferenced by a server based on their listing in the server's [CapabilityStatement](capabilitystatement.html), according to the `CapabilityStatement.rest.resource.searchParam.name` element. These values are typically a computer-friendly code defined by the [SearchParameter](searchparameter.html) resource for the parameter, either in [SearchParameter.code](searchparameter-definitions.html#SearchParameter.code) or [SearchParameter.alias](searchparameter-definitions.html#SearchParameter.alias). Note that the values listed in the `SearchParameter` resource are only suggestions, though some implementation guides will add requirements for improved interoperability.

Search input values are parsed according to the search parameter type, as defined in the relevant [SearchParameter](searchparameter.html) resource. The rules for parsing a search input value into the equivalent FHIR type can be found on the search page (e.g., converting a `[token](#token)` input into a [Coding](datatypes.html#Coding)).

The type of test used to compare values are controlled by the search parameter type and a search modifier OR a search prefix, if present. For example, the [number](#number) search parameter type describes the default search test as an equality test (i.e., `{element value} equals {input value}`). A [Search Modifier](#modifiers), such as `[not](search.html#modifiernot)` changes the test - in this case negating the default behavior (i.e., `not({element value} equals {input value})`). Finally, a [Search Prefix](search.html#prefix), allowed on the `number` search type, can be used to change the test to something like a 'greater than' (i.e., `{element value} is greater than {input value}`). Note that a single test cannot contain both a modifier and a prefix.

#### 3.2.1.5.2 Matching and Cardinality[](search.html#matching-cardinality "link to here")

Many elements in FHIR are defined as arrays (or collections) of values (e.g., a patient can have more than one name or address). When performing a search test against elements that have a cardinality of more than one, the test is considered a 'contains' type test against the collection of values. For example, if a patient has two `name` elements with values, a test is considered a match if at least one of the values is a match.

#### 3.2.1.5.3 Matching and Sub-Elements[](search.html#matching-backbones "link to here")

When a search parameter points to a complex or backbone element (an element that contains sub-elements), by default the search is interpreted as a search against one or more values in sub-elements with an appropriate type, as selected by the implementation. A search parameter MAY instead explicitly choose elements by using an expression that instead points directly to the sub-elements.[§30](#fcss30)

#### 3.2.1.5.4 Searching Multiple Values (joining with AND or OR)[](search.html#combining "link to here")

Search combination logic is defined by an equivalent set logic on the results of individual tests, performed at the resource level. "OR" joined parameters are "unions of sets" and "AND" joined parameters are "intersections of sets". Note that the joins are considered at the resource level and search parameters are unordered. This means that complex combinations of "AND" and "OR" are not possible using the basic search syntax. Uses requiring advanced combination logic can consider the `[_filter](search.html#filter)` parameter or [Named Queries](search.html#advanced).

When multiple search parameters passed to a single search, they are used to create an intersection of the results - in other words, multiple parameters are joined via "AND". For example, a search for a patient that includes a given name and a family name will only return records that match BOTH: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?given=valueGiven&family=valueFamily POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded given=valueGiven&family=valueFamily will return only Patient records with a family name of "valueFamily" and a given name of "valueGiven".

It is also possible to use an "AND" join repeated on the same element. When doing so, the join behavior is the same as when using different elements. For example, a search that asks for having two specific given names: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?given=valueGivenA&given=valueGivenB POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded given=valueGivenA&given=valueGivenB will return records that have BOTH a match for the given name of "valueGivenA" AND a match for the given name of "valueGivenB". Note that this will match both patient records that have a single `name` containing multiple `given` elements as well as records that have a multiple `name` elements that, _in total_, contain the values: Show as: [FHIR+JSON](#json) | [FHIR+XML](#xml) "Patient": { "name": \[{ "given": \[ "valueA", "valueB" \] }\] } "Patient": { "name": \[{ "given": \[ "valueA" \] }, { "given": \[ "valueB" \] }\] } <Patient> <name> <given value="valueA"/> <given value="valueB"/> </name> </Patient> <Patient> <name> <given value="valueA"/> </name> <name> <given value="valueB"/> </name> </Patient>

In order to search for unions of results (values joined by "OR"), values can be separated by a comma (",") character. For example, a search for a patient with EITHER of two given names: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?given=valueGivenA,valueGivenB POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded given=valueGivenA,valueGivenB will return Patient records with EITHER a given name of "valueGivenA" OR "valueGivenB". This includes patient records that have BOTH "valueGivenA" and "valueGivenB" (e.g., inclusive or).

Each value of an "OR" query may contain a prefix if allowed by the search type. For example, a search for a heart rate value ([LOINC 8867-4 ![icon](external.png)](https://loinc.org/8867-4/) ) outside of the 'normal resting range of adults': Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?code=http://loinc.org|8867-4&value-quantity=lt60,gt100 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded code=http://loinc.org|8867-4&value-quantity=lt60,gt100 will match against Observations that have the LOINC code for a Heart Rate and have a value that is EITHER less than 60 OR greater than 100.

If a modifier is used on a search parameter, it applies to each value of an "OR" query. For example, searching for either of two exact given names: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?given:exact=GivenA,GivenB POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded given:exact=GivenA,GivenB will match against Patients that contain a given name of exactly "GivenA", Patients that contain a given name of exactly "GivenB", and Patients that contain both values.

It is possible to combine both "AND" and "OR" type queries together in a single request. For example, a search that includes a family name and either of two given names: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?given=valueGivenA,valueGivenB&family=valueFamily POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded given=valueGivenA,valueGivenB&family=valueFamily will return records that match a family name of "valueFamily" AND a given name of either "valueGivenA" OR "valueGivenB".

Note that there is not a syntax to request unions on different elements (OR joins). For example, a client that wants records with either a given OR family name of "valueName". However, it is possible to define search parameters that operate on multiple elements. For example, the described use case of searching for a patient by any part of their name has a search parameter on Patient called `name`, which matches any part of a name (e.g., `name=valueName`). It is also possible to define [composite search parameters](#composite), which allows servers to provide this functionality.

If a client wants results based on OR-joins of unrelated elements, or if there is not a combined search parameter defined or supported on a server, the results can be retrieved by issuing multiple queries, either serially or in a batch.

The following table contains examples of how joins work depending on their element types. Note that this table is illustrative and not comprehensive. Additionally, the joins listed below can be combined in a single request to form more complicated logic. For brevity, this table demonstrates the joins only against the same element type, though this is not a requirement. For example, it is common to join one parameter against a simple scalar element with another that is an array of complex elements. In this table, the following definitions are used:

*   `Scalar` refers to elements that have a maximum cardinality of one.
*   `Array` refers to elements that have a maximum cardinality greater than one.
*   `Simple` refers to elements that do not contain additional elements (e.g., [Primitive Types](datatypes.html#primitive)).
*   `Complex` refers to elements that contain additional elements (e.g., [HumanName](datatypes.html#HumanName), etc.)

Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post)

**Context**

**Short**

**Example**

`AND`, `Scalar`, `Simple`

Intersection of records that match each of the search criteria

GET \[base\]/Patient?death-date=ge2022-01-01&death-date=lt2023-01-01 POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded death-date=ge2022-01-01&death-date=lt2023-01-01

The search is asking for patients with a `death-date` on or after January 1, 2022 and a `death-date` before January 1, 2023. The results are the intersection of patients with a `deceasedDateTime` on or after January 1, 2022 and patients with a `deceasedDateTime` before January 1, 2023. Each match will contain a single value in the requested range.

`OR`, `Scalar`, `Simple`

Union of records that match each of the search criteria

GET \[base\]/Patient?death-date=ge2030-01-01,lt1980-01-01 POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded death-date=ge2030-01-01,lt1900-01-01

The search is asking for patients with a `death-date` on or after January 1, 2030 or a `death-date` before January 1, 1900. The results are the union of patients with a `deceasedDateTime` on or after January 1, 2030 and patients with a `deceasedDateTime` before January 1, 1900. Each match will contain a single value in the requested range (e.g., checking for dates outside the allowed range).

`AND`, `Array`, `Simple`

Intersection of records that contain at least one match for each search criteria

GET \[base\]/AllergyIntolerance?category=medication&category=biologic POST \[base\]/AllergyIntolerance/\_search Content-Type: application/x-www-form-urlencoded category=medication&category=biologic

The search is asking for allergies or intolerances with a `category` of `medication` and a `category` of `biologic`. The results are the intersection of AllergyIntolerance records records with each of the requested `category` values. Each matching record will contain at least one of each of the requested category values.

`OR`, `Array`, `Simple`

Union of records that contain at least one match for each search criteria

GET \[base\]/AllergyIntolerance?category=medication,biologic POST \[base\]/AllergyIntolerance/\_search Content-Type: application/x-www-form-urlencoded category=medication,biologic

The search is asking for allergies or intolerances with a `category` of either `medication` or `biologic`. The results are the union of AllergyIntolerance records records with any of the requested `category` values. Each matching record will contain at least one of either of the requested category values, and can contain both.

`AND`, `Array`, `Complex`

Intersection of records that contain at least one match for each search criteria

GET \[base\]/Patient?name=valueA&name=valueB POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded name=valueA&name=valueB

The search is asking for patients with a `name` of 'valueA' a `name` of 'valueB'. The results are the intersection of patients that have each of the requested values in any component of their name. Each match will contain each of the requested values in some form. E.g., a patient with the given names of both 'valueA' and 'valueB' will match, as well as a patient with a given name of 'valueA' and a family name of 'valueB', etc..

`OR`, `Array`, `Complex`

Union of records that contain at least one match for any search criteria

GET \[base\]/Patient?name=valueA,valueB POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded name=valueA,valueB

The search is asking for patients with a `name` of either 'valueA' or 'valueB'. The results are the union of patients with 'valueA' anywhere in their name and patients with 'valueB' anywhere in their name. Each matching record will contain at least one of the search criteria.

#### 3.2.1.5.5 Modifiers[](search.html#modifiers "link to here")

Generally, search parameters are used as filters to refine search results based on one or more resource elements. For example, a query might find patients born in a certain year by using the `birthdate` search parameter, which maps to the `Patient.birthDate` element. Sometimes it is necessary or desirable to use a search parameter with a different behavior, for example, searching for patients that do not have a birth date in their records. Instead of requiring many search parameters on an element to cover each type of use, this specification defines **modifiers** that can be used to change the meaning of a search parameter while leaving element mappings intact.

Search parameter modifiers are defined by the [search-modifier-code](codesystem-search-modifier-code.html) code system. Search parameter definitions MAY include a list of allowed modifiers in the `SearchParameter.modifier` element, which is bound to the [search-modifier-code](valueset-search-modifier-code.html) value set.[§31](#fcss31) Servers SHALL support any modifiers present in search parameters the server advertises support for.[§32](#fcss32) Servers MAY choose to support additional modifiers.[§33](#fcss33) Note that servers MAY support modifiers on types not defined in this specification.[§34](#fcss34)

In search requests, search modifiers are used by appending the modifier as a suffix to the search parameter name, with the syntax of: `[name]:[modifier]`, such as `birthdate:missing`. Note that only a single modifier is allowed on a single search parameter. If multiple layers of modification are necessary, a new search parameter can be defined with the appropriate behavior. If the desired behavior is not possible to define in a search parameter without multiple stacked modifiers, an operation can be defined.

Since modifiers change the meaning of a search parameter, a server SHALL reject any search request that contains a search parameter with an unsupported modifier.[§35](#fcss35) For example, if the server supports the `name` search parameter, but not the `:exact` modifier on the name, it would reject a search with the parameter `name:exact=Bill`, using an HTTP `400` error with an [OperationOutcome](operationoutcome.html) with a [clear error message](operationoutcome-example-searchfail.html).

While support for modifiers is applied per search parameter, modifiers are generally defined according to search parameter type. For example, the ":exact" modifier has meaning when applied to string-type search parameters but has no meaning and cannot be used with token-type search parameters. The exception is the search parameters type [special](#special). The definition for any search parameter of type "special" SHALL explicitly list any allowed modifiers; this list MAY include any value from the [search-modifier-code](codesystem-search-modifier-code.html) code system.[§36](#fcss36)

The modifiers defined by this specification are:

`[above](#modifierabove)`

[reference](#reference), [token](#token), [uri](#uri)

Tests whether the value in a resource is or subsumes the supplied parameter value (is-a, or hierarchical relationships).

`[below](#modifierbelow)`

[reference](#reference), [token](#token), [uri](#uri)

Tests whether the value in a resource is or is subsumed by the supplied parameter value (is-a, or hierarchical relationships).

`[code-text](#modifiercodetext)`

[reference](#reference), [token](#token)

Tests whether a coded value (e.g., `Coding.code`) in a resource matches the supplied parameter value using basic string matching (starts with or is, case-insensitive).

`[contains](#modifiercontains)`

[reference](#reference), [string](#string), [uri](#uri)

Tests whether the value in a resource includes the supplied parameter value anywhere within the field being searched or if a hierarchial relationship has a matching value within the tree it covers.

`[exact](#modifierexact)`

[string](#string)

Tests whether the value in a resource exactly matches the supplied parameter value (the whole string, including casing and accents).

`[identifier](#modifieridentifier)`

[reference](#reference)

Tests whether the `Reference.identifier` in a resource (rather than the `Reference.reference`) matches the supplied parameter value.

`[in](#modifierin)`

[token](#token)

Tests whether the value in a resource is a member of the supplied parameter ValueSet.

`[iterate](#modifieriterate)`

n/a (not allowed anywhere by default)

The search parameter indicates an inclusion directive (\_include, \_revinclude) that is applied to an included resource instead of the matching resource.

`[missing](#modifiermissing)`

[date](#date), [number](#number), [quantity](#quantity), [reference](#reference), [string](#string), [token](#token), [uri](#uri)

Tests whether the value in a resource is present (when the supplied parameter value is `true`) or absent (when the supplied parameter value is `false`).

`[not](#modifiernot)`

[token](#token)

Tests whether the value in a resource does not match the specified parameter value. Note that this includes resources that have no value for the parameter.

`[not-in](#modifiernotin)`

[reference](#reference), [token](#token)

Tests whether the value in a resource is not a member of the supplied parameter ValueSet.

`[of-type](#modifieroftype)`

[token](#token) (only [Identifier](datatypes.html#Identifier))

Tests whether the `Identifier` value in a resource matches the supplied parameter value.

`[text](#modifiertexttoken)`

[reference](#reference), [token](#token)

Tests whether the textual value in a resource (e.g., `CodeableConcept.text`, `Coding.display`, `Identifier.type.text`, or `Reference.display`) matches the supplied parameter value using basic string matching (begins with or is, case-insensitive).

`[text](#modifiertextstring)`

[string](#string)

The search parameter value is to be processed as input to a search with advanced text handling.

`[text-advanced](#modifiertextadvanced)`

[reference](#reference), [token](#token)

Tests whether the value in a resource matches the supplied parameter value using advanced text handling that searches text associated with the code/value - e.g., `CodeableConcept.text`, `Coding.display`, or `Identifier.type.text`.

`[[type]](#modifiertype)`

[reference](#reference)

Tests whether the value in a resource points to a resource of the supplied parameter type. Note: a concrete `ResourceType` is specified as the modifier (e.g., not the literal `:[type]`, but a value such as `:Patient`).

Note that modifiers are allowed in searches that combine multiple terms. When using a modifier in "or" joined search parameter values, the modifier applies to ALL values present. For example, in `given:exact=valueA,valueB`, the `exact` modifier is used for both "valueA" and "valueB". Additional details can be found in the [Searching Multiple Values](#multivalue) section of the Search page.

##### 3.2.1.5.5.1 above[](search.html#modifierabove "link to here")

The `above` modifier allows clients to search hierarchies of data based on relationships. The `above` modifier is only allowed on search parameters with search types [reference](#reference), [token](#token), and [uri](#uri). The exact semantics for use vary depending on the type of search parameter:

###### 3.2.1.5.5.1.1 above with _reference_ search type parameters and `Reference` FHIR type targets[](search.html#3.2.1.5.5.1.1 "link to here")

When the `above` modifier is used with a [reference](#reference) type search parameter and the target is an element with the FHIR type `Reference`, the search is interpreted as a hierarchical search on linked resources of the same type, including exact matches and all children of those matches. The `above` modifier is only valid for [circular](references.html#circular) references - that is, references that point to another instance of the same type of resource where those references establish a hierarchy. For example, the [Location](location.html) resource can define a hierarchy using the `partOf` element to track locations that are inside each other (e.g., `Location/A101`, a room, is _partOf_ `Location/A`, a floor). Meanwhile, the [Patient](patient.html) resource could refer to other patient resources via the `link` element, but no hierarchical meaning is intended. Further discussion about the requirements and uses for this type of search can be found in the section [Searching Hierarchies](#recursive).

When using the `above` modifier on a reference search type parameter, all typically-valid search parameter reference inputs are allowed. Note that the actual Resources that establish hierarchical references and the search parameters used are listed in the [Circular Resource References](references.html#circular) section of the References page.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Procedure?location:above=A101 POST \[base\]/Procedure/\_search Content-Type: application/x-www-form-urlencoded location:above=A101 would match any Procedure resources with locations:

*   `A101`, `Location/A101`, `https://example.org/Location/A101` - this location by ID
*   `A100`, `Location/A100`, `https://example.org/Location/A100` - parent of A101, representing the first floor (A101 - A199)
*   `BuildingA`, `Location/BuildingA`, `https://example.org/Location/BuildingA` - parent of A100, representing the building 'A'

###### 3.2.1.5.5.1.2 above with _reference_ search type parameters and `canonical` FHIR type targets[](search.html#3.2.1.5.5.1.2 "link to here")

When the `above` modifier is used with a [reference](#reference) type search parameter and the target is an element with the FHIR type [canonical](datatypes.html#canonical), the search is interpreted as a version search against the canonical resource. The format of the parameter is either **`[url]`** or **`[url]|[version]`** (see [Encoding Note](#encoding-note)). This search is only allowed if the version scheme for the resource is either specified or the server chooses to infer it. Version-related search criteria against resources with unknown versioning schemes SHALL be either ignored or rejected.[§37](#fcss37) The `above` modifier comparison is performed as a 'greater than' against the version-scheme defined by the resource. Any versions that match the specified search are **excluded** from the result. The versions returned will be listed in no particular order.

When using the `above` modifier on a canonical reference, all typically-valid search parameter token inputs are allowed. Note that any vertical pipe (`|`) characters that are part of the URL must be escaped (`%7C`) and can cause issues with parsing of the parameter.

More information can be found in the section [References and Versions](#versions).

###### 3.2.1.5.5.1.3 above with _token_ search type parameters[](search.html#3.2.1.5.5.1.3 "link to here")

When the `above` modifier is used with a [token](#token) type search parameter, the supplied token is a concept with the form `[system]|[code]` (see [Encoding Note](#encoding-note)) and the intention is to test whether the coding in a resource [subsumes](codesystem.html#subsumption) the specified search code. Matches to the input token concept have an is-a relationship with the coding in the resource, and this includes the coding itself.

When using the `above` modifier on a token, all typically-valid search parameter token inputs are allowed.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?code:above=http://snomed.info/sct|3738000 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded code:above=http://snomed.info/sct|3738000 for Observations with a code `above` SNOMED 'Viral hepatitis (disorder)' will match any Observation resources with codes:

*   SNOMED `3738000` - Viral hepatitis (this code)
*   SNOMED `235862008` - Hepatitis due to infection (parent of 'Viral hepatitis')
*   SNOMED `128241005` - Inflammatory disease of liver (parent of 'Hepatitis due to infection')
*   etc.
*   SNOMED `312130009` - Viral infection by site (parent of 'Viral hepatitis')
*   SNOMED `301810000` - Infection by site (parent of 'Viral infection by site')
*   etc.

Note that there are two hierarchical parents to the requested code and parent matches traverse up each path.

###### 3.2.1.5.5.1.4 above with _uri_ search type parameters[](search.html#3.2.1.5.5.1.4 "link to here")

When the `above` modifier is used with a [uri](#uri) type search parameter, the value is used for partial matching based on URL path segments. Because of the hierarchical behavior of `above`, the modifier only applies to URIs that are URLs and cannot be used with URNs such as OIDs. Note that there are not many use cases where `above` is useful compared to a `below` search.

When using the `above` modifier on a uri, all typically-valid search parameter uri inputs are allowed.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/ValueSet?url:above=http://acme.org/fhir/ValueSet/123/\_history/5 POST \[base\]/ValueSet/\_search Content-Type: application/x-www-form-urlencoded url:above=http://acme.org/fhir/ValueSet/123/\_history/5 would match any ValueSet resources with a url of:

*   `http://acme.org/fhir/ValueSet/123/_history/5` - full match
*   `http://acme.org/fhir/ValueSet/123/_history` - parent of requested URI
*   `http://acme.org/fhir/ValueSet/123` - ancestor of requested URI
*   `http://acme.org/fhir/ValueSet` - ancestor of requested URI
*   `http://acme.org/fhir` - ancestor of requested URI
*   `http://acme.org/` - ancestor of requested URI

##### 3.2.1.5.5.2 below[](search.html#modifierbelow "link to here")

The `below` modifier allows clients to search hierarchies of data based on relationships. The `below` modifier is only allowed on search parameters of type [reference](#reference), [token](#token), and [uri](#uri). The exact semantics for use vary depending on the type of search parameter:

###### 3.2.1.5.5.2.1 below with _reference_ type search parameters and `Reference` FHIR type targets[](search.html#3.2.1.5.5.2.1 "link to here")

When the `below` modifier is used with a [reference](#reference) type search parameter and the target is an element with the FHIR type `Reference`, the search is interpreted as a hierarchical search on linked resources of the same type, including exact matches and all children of those matches. The `below` modifier is only valid for [circular](references.html#circular) references - that is, references that point to another instance of the same type of resource where those references establish a hierarchy. For example, the [Location](location.html) resource can define a hierarchy using the `partOf` element to track locations that are inside each other (e.g., `Location/A101`, a room, is _partOf_ `Location/A`, a floor). Meanwhile, the [Patient](patient.html) resource could refer to other patient resources via the `link` element, but no hierarchical meaning is intended. Further discussion about the requirements and uses for this type of search can be found in the section [Searching Hierarchies](#recursive).

When using the `below` modifier on a hierarchical reference, all typically-valid search parameter reference inputs are allowed. Note that the actual Resources that establish hierarchical references and the search parameters used are listed in the [Circular Resource References](references.html#circular) section of the References page.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Procedure?location:below=BuildingA POST \[base\]/Procedure/\_search Content-Type: application/x-www-form-urlencoded location:below=BuildingA for Procedures with a location `below` BuildingA would match any Procedure resources with locations:

*   `BuildingA`, `Location/BuildingA`, `https://example.org/Location/BuildingA` - this location by ID
*   `A100`, `Location/A100`, `https://example.org/Location/A100` - child of BuildingA, representing the first floor
*   `A101`, `Location/A101`, `https://example.org/Location/A101` - child of A100, room 101
*   `A1..`, etc. - child of A100, rooms on the first floor
*   `A200`, `Location/A200`, `https://example.org/Location/A200` - child of BuildingA, representing the second floor
*   etc.

###### 3.2.1.5.5.2.2 below with _reference_ type search parameters and `canonical` FHIR type targets[](search.html#3.2.1.5.5.2.2 "link to here")

When the `below` modifier is used with a [reference](#reference) type search parameter and the target is an element with the FHIR type [canonical](datatypes.html#canonical), the search is interpreted as a version search against the canonical resource. The format of the parameter is either **`[url]`** or **`[url]|[version]`** (see [Encoding Note](#encoding-note)). This search is only allowed if the version scheme for the resource is either specified or the server chooses to infer it. Version-related search criteria against resources with unknown versioning schemes SHALL be either ignored or rejected.[§38](#fcss38) The `below` modifier comparison is performed as a 'less than' against the version-scheme defined by the resource. Any versions that match the specified search are **excluded** from the result. The versions returned will be listed in no particular order.

When using the `below` modifier on a canonical reference, all typically-valid search parameter token inputs are allowed. Note that any vertical pipe (`|`) characters that are part of the URL must be escaped (`%7C`) and can cause issues with parsing of the parameter.

More information can be found in the section [References and Versions](#versions).

###### 3.2.1.5.5.2.3 below with _token_ type search parameters[](search.html#3.2.1.5.5.2.3 "link to here")

When the `below` modifier is used with a [token](#token) type search parameter, the supplied token is a concept with the form `[system]|[code]` (see [Encoding Note](#encoding-note)) and the intention is to test whether the coding in a resource [is subsumed by](codesystem.html#subsumption) the specified search code. Matches include resources that have a coding that has an is-a relationship with the input concept, and this includes the coding itself.

When using the `below` modifier on a token, all typically-valid search parameter token inputs are allowed.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?code:below=http://snomed.info/sct|235862008 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded code:below=http://snomed.info/sct|235862008 for Observations with a code `below` SNOMED 'Hepatitis due to infection' would match any Observation resources with codes:

*   SNOMED `235862008` - Hepatitis due to infection (this code)
*   SNOMED `773113008` - Acute infectious hepatitis (child)
*   SNOMED `95897009` - Amebic hepatitis (child)
*   etc.

Note that there is no limit inherent to the referential search. If child concepts contain child concepts of their own, the search continues down the tree in each path until reaching a terminal node.

The `below` modifier is useful when trying to resolve MIME types. Further documentation can be found in the [Searching MIME Types](#mimetype) section of this page.

###### 3.2.1.5.5.2.4 below with _uri_ type search parameters[](search.html#3.2.1.5.5.2.4 "link to here")

When the `below` modifier is used with a [uri](#uri) type search parameter, the value is used for partial matching based on URL path segments. Because of the hierarchical behavior of `below`, the modifier only applies to URIs that are URLs and cannot be used with URNs such as OIDs.

When using the `below` modifier on a uri, all typically-valid search parameter uri inputs are allowed.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/ValueSet?url:below=http://acme.org/fhir POST \[base\]/ValueSet/\_search Content-Type: application/x-www-form-urlencoded url:above=http://acme.org/fhir would match any ValueSet resources with a url of:

*   `http://acme.org/fhir` - full match
*   `http://acme.org/fhir/ValueSet` - child of requested URI
*   `http://acme.org/fhir/ValueSet/123` - descendant of requested URI
*   `http://acme.org/fhir/ValueSet/123/_history` - descendant of requested URI
*   `http://acme.org/fhir/ValueSet/123/_history/1` - descendant of requested URI
*   etc.

##### 3.2.1.5.5.3 code-text[](search.html#modifiercodetext "link to here")

The `code-text` modifier allows clients to indicate that a supplied `string` input is used as a case-insensitive and combining-character insensitive test when matching against the start of target string. This modifier is used to do a 'standard' string search against code values. Note that the handling of extended [grapheme clusters ![icon](external.png)](http://unicode.org/reports/tr29/#Grapheme_Cluster_Boundaries) is within the discretion of the server, i.e., the server decides if a search parameter matches on canonically equivalent characters or matches on the actual used Unicode code points. Case-insensitive comparisons do not take locale into account, and will result in unsatisfactory results for certain locales. Character case definitions and conversions are out of scope for the FHIR standard, and the results of such operations are implementation dependent.

`code-text` is only allowed on [reference](#reference) and [token](#token) type search parameters. When using the 'code-text' modifier, all typically-valid search parameter string inputs are allowed.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?language:code-text=en POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded language:code-text=EN would match any Patient resources with a communication language of:

*   `en` - requested code text (case-insensitive)
*   `en-AU` - starts with requested code text (case-insensitive)
*   `en-CA` - starts with requested code text (case-insensitive)
*   `en-GB` - starts with requested code text (case-insensitive)
*   etc.

##### 3.2.1.5.5.4 contains[](search.html#modifiercontains "link to here")

The `contains` modifier allows clients to indicate that a supplied input is used as a containment test when matching against the target value. The exact semantics for use vary depending on the type of search parameter.

###### 3.2.1.5.5.4.1 contains with _reference_ search type parameters[](search.html#3.2.1.5.5.4.1 "link to here")

When using the `contains` modifier with [reference](#reference) type search parameters, reference test values are allowed (e.g., a logical identifier).

The search parameter is a reference to a resource that is part of a hierarchical tree, and the search should return the specified resource along with all resources above (ancestors) and below (descendants) it in the hierarchy. This is equivalent to combining both `:above` and `:below` searches plus the specified resource itself.

This modifier is useful when you need the full context of a hierarchical resource in a single query. For example, when working on a specific task in a task tree, you may want to see the entire branch from root to leaves in one request.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Task?part-of:contains=Task/T1A POST \[base\]/Task/\_search Content-Type: application/x-www-form-urlencoded part-of:contains=Task/T1A would match any Task resources that contain task T1A in a hierarchy, including T1A itself, its ancestors, and its descendants. Given a task tree where:

*   T1 is the root task
*   T1A and T1B are sub-tasks of T1
*   T1A1 and T1A2 are sub-tasks of T1A

This search would return: T1 (ancestor), T1A (the specified task), T1A1 and T1A2 (descendants).

Note: The `contains` modifier can only be used with reference search parameters that support hierarchical relationships (typically those that reference the same resource type they are defined on, like `Location.partOf` or `Task.partOf`).

###### 3.2.1.5.5.4.2 contains with _string_ and _uri_ search type parameters[](search.html#3.2.1.5.5.4.2 "link to here")

When using the `contains` modifier with [string](#string) or [uri](search.html) type search parameters, all typically-valid search parameter string inputs are allowed.

The `contains` modifier in this context allows clients to indicate that a supplied `string` input is used as a case-insensitive and combining-character insensitive test when matching anywhere in the target string. Note that the handling of extended [grapheme clusters ![icon](external.png)](http://unicode.org/reports/tr29/#Grapheme_Cluster_Boundaries) is within the discretion of the server, i.e., the server decides if a string search parameter matches on canonically equivalent characters or matches on the actual used Unicode code points. Case-insensitive comparisons do not take locale into account, and will result in unsatisfactory results for certain locales. Character case definitions and conversions are out of scope for the FHIR standard, and the results of such operations are implementation dependent.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?family:contains=son POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded family:contains=son would match any Patient resources with a family names such as:

*   `Son` - requested string (case-insensitive)
*   `Sonder` - begins with requested string (case-insensitive)
*   `Erikson` - ends with requested string (case-insensitive)
*   `Samsonite` - contains requested string (case-insensitive)
*   etc.

##### 3.2.1.5.5.5 exact[](search.html#modifierexact "link to here")

The `exact` modifier allows clients to indicate that a supplied `string` input is the complete and exact value that tested for a match, including casing and combining characters. Note that the handling of extended [grapheme clusters ![icon](external.png)](http://unicode.org/reports/tr29/#Grapheme_Cluster_Boundaries) is within the discretion of the server, i.e., the server decides if a string search parameter matches on canonically equivalent characters or matches on the actual used Unicode code points. Case-insensitive comparisons do not take locale into account, and will result in unsatisfactory results for certain locales. Character case definitions and conversions are out of scope for the FHIR standard, and the results of such operations are implementation dependent.

`exact` is only allowed on [string](#string) type search parameters. When using the 'exact' modifier, all typically-valid search parameter string inputs are allowed.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?family:exact=Son POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded family:exact=Son will _only_ match Patient resources with a family name of:

*   `Son` - requested string (case-sensitive)

##### 3.2.1.5.5.6 identifier[](search.html#modifieridentifier "link to here")

The `identifier` modifier allows clients to indicate that a supplied `token` is used to match against the [Reference.identifier](references-definitions.html#Reference.identifier) element of a reference instead of the [Reference.reference](references-definitions.html#Reference.reference) element. The format of the parameter is **`[system]|[code]`** (see [Encoding Note](#encoding-note)). Note that chaining behavior is not defined when using the `identifier` modifier, nor is the behavior defined when targeting references without explicit `reference` values (e.g., canonical references, etc.). Server implementers have discretion on behavior for searches in these cases.

`identifier` is only allowed on [reference](#reference) type search parameters and target elements that are the `Reference` FHIR type. When using the 'identifier' modifier, all typically-valid search parameter token inputs are allowed.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?subject:identifier=http://example.org/fhir/mrn|12345 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded subject:identifier=http://example.org/fhir/mrn|12345 for observations with a subject containing the identifier 'http://example.org/fhir/mrn|12345' would match Observation resources such as: Show as: [FHIR+JSON](#json) | [FHIR+XML](#xml) "Observation": { "subject": { "reference": "Patient/abc", "identifier": { "system": "http://example.org/fhir/mrn", "value": "12345" } } } <Observation> <subject> <reference value="Patient/abc"/> <identifier> <system value="http://example.org/fhir/mrn"/> <value value="12345"/> </identifier> </subject> </Observation> but **will not** match: Show as: [FHIR+JSON](#json) | [FHIR+XML](#xml) "Observation": { "subject": { "reference": "Patient/abc" } } <Observation> <subject> <reference value="Patient/abc"/> </subject> </Observation> even if the Patient resource for `Patient/abc` includes the requested identifier among its `Patient.identifier` values.

For more details about the difference between the `identifier` modifier and chained-searches on the identifier element, see [Searching Identifiers](#specialidentifiers).

##### 3.2.1.5.5.7 in[](search.html#modifierin "link to here")

The `in` modifier is used to filter based on value membership of codes in Value Sets. The `in` modifier is only allowed on [token](#token) type search parameters.

When the `in` modifier is used with a [token](#token) search parameter, the input is a [uri](#uri) (relative or absolute) that identifies a value set, and the search parameter tests whether the coding is in the specified value set. The search input value may be literal (to an address where the value set can be found) or logical (values of `ValueSet.url`). If the server can treat the value as a literal URL, it does, else it tries to match known logical `ValueSet.url` values. Note that the URI need not point to the root value set, `http://snomed.info/sct?fhir_vs=isa/235862008` is a valid input as a reference to a subset of SNOMED CT 'Hepatitis due to infection', but the parameter value will need to be URL Encoded in order to be a valid parameter.

When using the `in` modifier on a token, only tokens targeting value sets are allowed (e.g., a boolean token parameter target is not allowed).

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Condition?code:in=ValueSet/123 POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded code:in=ValueSet/123 would match any conditions that contain any code from 'ValueSet/123'.

Similarly, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Condition?code:in=http://snomed.info/sct?fhir\_vs=isa/235862008 POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded code:in=http://snomed.info/sct?fhir\_vs=isa/235862008 would match any conditions that contain any code from SNOMED CT that is a `235862008`, e.g.:

*   SNOMED `235862008` - Hepatitis due to infection (this code)
*   SNOMED `773113008` - Acute infectious hepatitis (is-a 235862008)
*   SNOMED `95897009` - Amebic hepatitis (is-a 235862008)
*   etc.

##### 3.2.1.5.5.8 iterate[](search.html#modifieriterate "link to here")

The `iterate` modifier is used to indicate that an inclusion directive should be applied to included resources as well, rather than only the matching resources. Note that this modifier is not defined for any search parameter types and can only be applied to the search result parameters of `_include` and `_revinclude`.

When the `iterate` modifier is used, the input provided is the same as the input for the inclusion directive (see [Including other resources](#include)).

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?code=http://snomed.info/sct|3738000&\_include=Observation:patient&\_include:iterate=Patient:link POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded code=http://snomed.info/sct|3738000&\_include=Observation:patient&\_include:iterate=Patient:link would match any observations with the SNOMED code `3738000` (Viral hepatitis (disorder)). The results would include resources found by following the chained search reference `Observation.patient`, which are `Patient` resources linked via `Observation.subject`. Additionally, the server would _iterate_ through the included patient records and follow the `Patient.link` FHIR references, including linked `Patient` or `RelatedPerson` resources.

More information can be found in the section [Including other resources](#include)

![](assets/images/dragon.png "Here Be Dragons!")

###### 3.2.1.5.5.8.1 Performance Warning[](search.html#3.2.1.5.5.8.1 "link to here")

Note that the iterate modifier causes implementations to perform a sub-query for each iterate directive for each resource it is based on. Using iterate directives can often reduce the total processing time of a set of queries (by reducing the number of round-trips) but can significantly increase the processing time of a single query.As noted in [Including Referenced Resources](#include), servers have discretion about limiting the depth of recursion, which iterations to support, and how many resources to include. When results are truncated, it is recommended for servers to inform clients via search outcome.

##### 3.2.1.5.5.9 missing[](search.html#modifiermissing "link to here")

The `missing` modifier allows clients to filter based on whether resources contain values that _can_ match a search parameter. Usually, this equates to testing if a resource has an element or not.

`missing` is allowed on search parameter types that represent single-elements: [date](#date), [number](#number), [quantity](#quantity), [reference](#reference), [string](#string), [token](#token), [uri](#uri). When using the 'missing' modifier, the only valid input values are the literal values `true` and `false`.

Searching for `[parameter]:missing=true` requests all resources that **do not** have a value in the matching element or where the element is present with extensions, but no `value` is specified.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?given:missing=true POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded given:missing=true would match any Patient records that do not have _any_ value in `Patient.name` that contains a value for `given`, even if a patient contains a `Patient.name` that has a `given` with an extension and no value (e.g., a Data Absent Reason).

Searching for `[parameter]:missing=false` requests all resources that **do** have a value in the matching element.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?given:missing=false POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded given:missing=false would match any Patient records that have _any_ value in `Patient.name` that contains a value for `given`.

##### 3.2.1.5.5.10 not[](search.html#modifiernot "link to here")

The `not` modifier allows clients to filter based on whether resources do not contain a specified token based on the search parameter input.

`not` is only allowed on search parameters of type [token](#token). When using the 'not' modifier, all typically-valid search parameter token inputs are allowed.

Searching for `[parameter]:not=[value]` requests all resources that **do not** have any matching value in the searched element(s).

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?gender:not=male POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded gender:not=male would match any Patient records that do not have `male` as the value in `Patient.gender`. This includes:

*   `female` - [Administrative Gender](valueset-administrative-gender.html) that is not 'male'
*   `other` - [Administrative Gender](valueset-administrative-gender.html) that is not 'male'
*   `unknown` - [Administrative Gender](valueset-administrative-gender.html) that is not 'male'
*   records without a `Patient.gender` value (even if a record has extensions, such as a Data Absent Reason)

Similarly, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Composition?section:not=48765-2 POST \[base\]/Composition/\_search Content-Type: application/x-www-form-urlencoded Composition?section:not=48765-2 for documents without an "Allergies and adverse reaction" section ([LOINC 48765-2 ![icon](external.png)](https://loinc.org/48765-2/) ) would return all Composition records that _do not contain_ any section with a code of '48765-2'. This search _does not_ return "any document that has a section that is not an Allergies and adverse reaction section". In the presence of multiple possible matches, the negation applies to the set and not each individual entry.

![](assets/images/dragon.png "Here Be Dragons!")

Note that the `not` modifier can behave in undesirable ways with "or"-joined parameter values. Specifically, consider a query that uses a scalar (max cardinality one) value, such as: `gender:not=male,female`. Using the 'default' rules, the query is expanded to be the union of the sets of: patients that do not have a gender element value or that contain any value other than "male", and patients that do not have a gender element value or that contain any value other than "female" - the union of those sets will always contain all records.

Implementer feedback is requested on this behavior and if additional rules concerning the `not` modifier would be appropriate. In the meantime, use of the `not` modifier with "or"-joined search terms is not recommended. Implementers are instead recommended to use either the [\_filter](#_filter) parameter or [Named Queries](#advanced).

##### 3.2.1.5.5.11 not-in[](search.html#modifiernotin "link to here")

The `not-in` modifier is used to filter based on a value exclusion test for codes of Value Sets. The `not-in` modifier is only allowed on [token](#token) type search parameters.

When the `not-in` modifier is used with a [token](#token) search parameter, the input is a [uri](#uri) (relative or absolute) that identifies a value set, and the search parameter tests whether the coding is not in the specified value set. The search input value may be literal (to an address where the value set can be found) or logical (values of `ValueSet.url`). If the server can treat the value as a literal URL, it does, else it tries to match known logical `ValueSet.url` values. Note that the URI need not point to the root value set, `http://snomed.info/sct?fhir_vs=isa/235862008` is a valid input as a reference to a subset of SNOMED CT 'Hepatitis due to infection', but the parameter value will need to be URL Encoded in order to be a valid parameter.

When using the `not-in` modifier on a token, only tokens targeting value sets are allowed (e.g., a boolean token parameter target is not allowed).

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Condition?code:not-in=ValueSet/123 POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded code:not-in=ValueSet/123 would match any conditions that _do not_ contain any code from 'ValueSet/123'.

Similarly, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Condition?code:not-in=http://snomed.info/sct?fhir\_vs=isa/235862008 POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded code:not-in=http://snomed.info/sct?fhir\_vs=isa/235862008 would match any conditions that _do not_ contain any code from SNOMED CT that is a `235862008`, e.g., all codes **excluding**:

*   SNOMED `235862008` - Hepatitis due to infection (this code)
*   SNOMED `773113008` - Acute infectious hepatitis (is-a 235862008)
*   SNOMED `95897009` - Amebic hepatitis (is-a 235862008)
*   etc.

##### 3.2.1.5.5.12 of-type[](search.html#modifieroftype "link to here")

The `of-type` modifier allows clients to filter for resource [Identifier](datatypes.html#Identifier), based on the `Identifier.type.coding.system`, `Identifier.type.coding.code` and `Identifier.value`. This allows searches for specific values only within a specific identifier code system. The format when using 'of-type' is `[system]|[code]|[value]` (see [Encoding Note](#encoding-note)), where `[system]` and `[code]` refer to the code and system in `Identifier.type.coding`; the system and code portion is considered a match if the the `system|code` token would match a given `Identifier.type.coding`. The `[value]` test is a string match against `Identifier.value`. All three parts must be present.

`of-type` is only allowed on search parameters of type [token](#token), and further restricted to parameters targeting the [Identifier](datatypes.html#Identifier) type. When using the 'of-type' modifier, all typically-valid token values are valid for the `system` and `code` segments, and all typically-valid string values are valid for the `value` segment. Note that input values need to escaped in order to be processed correctly and values including reserved characters such as a vertical pipe (`|`) can cause parsing issues.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?identifier:of-type=http://terminology.hl7.org/CodeSystem/v2-0203|MR|12345 POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded identifier:of-type=http://terminology.hl7.org/CodeSystem/v2-0203|MR|12345 for patients that contain an identifier that has a type coding of with a system of `http://terminology.hl7.org/CodeSystem/v2-0203`, a code of `MR` (which identifies Medical Record Numbers), and a value of `12345` will return records such as: Show as: [FHIR+JSON](#json) | [FHIR+XML](#xml) "Patient": { "identifier": \[{ "type": { "coding" : { "system": "http://terminology.hl7.org/CodeSystem/v2-0203", "code": "MR" } } "system": "http://example.org/ehr-primary/", "value": "12345" }\] } <Patient> <identifier> <type> <coding> <system value="http://terminology.hl7.org/CodeSystem/v2-0203"/> <value value="MR"/> </coding> </type> <system value="http://example.org/ehr-primary"/> <value value="12345"/> </identifier> </Patient> This can be used to disambiguate between data sets that contain collisions. For example, the above search will NOT return values with a different identifying type, such as: Show as: [FHIR+JSON](#json) | [FHIR+XML](#xml) "Patient": { "identifier": \[{ "type": { "coding" : { "system": "http://terminology.hl7.org/CodeSystem/v2-0203", "code": "MRT" } } "system": "http://example.org/ehr-er", "value": "12345" }\] } <Patient> <identifier> <type> <coding> <system value="http://terminology.hl7.org/CodeSystem/v2-0203"/> <value value="MRT"/> </coding> </type> <system value="http://example.org/ehr-er"/> <value value="12345"/> </identifier> </Patient> which signifies that the identifier is a Temporary Medical Record Number.

##### 3.2.1.5.5.13 text with _reference_or _token_ type search parameters[](search.html#modifiertexttoken "link to here")

The `text` modifier on search parameters of type [reference](#reference) and [token](#token) allows clients to indicate that a supplied `string` is to be used to perform a string search against a textual value associated with a code or value. For example, `CodeableConcept.text`, `Coding.display`, `Identifier.type.text`, or `Identifier.assigner.display`. Search matching is performed using basic string matching rules - begins with or is, case-insensitive.

In this context, the `text` modifier is only allowed on [reference](#reference) and [token](#token) type search parameters. When using the 'text' modifier, all typically-valid search parameter string inputs are allowed.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Condition?code:text=headache POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded code:text=headache would match Condition resources containing any codes that start with or equal the string 'headache' (case-insensitive), such as:

*   SNOMED `25064002` - Headache finding
*   SNOMED `398987004` - Headache following lumbar puncture
*   SNOMED `230480006` - Headache following myelography (disorder)
*   ICD-10 `R51` - Headache
*   ICD-10 `R51.0` - Headache with orthostatic component, not elsewhere classified
*   ICD-10 `R51.9` - Headache, unspecified
*   etc.

Note that the search is _not_ expected to return results with codes such as SNOMED `735938006`, since the code text of 'Acute headache' does not match a default string search of the term headache (case-insensitive, begins with or equals). For advanced searching of related text, see the [text-advanced](#modifiertextadvanced) search modifier.

For more details about the difference between the `identifier` modifier and chained-searches on the identifier element, see [Searching Identifiers](#specialidentifiers).

##### 3.2.1.5.5.14 text with _string_ type search parameters[](search.html#modifiertextstring "link to here")

The `text` modifier allows clients to request matching based on advanced string processing of the search parameter input. Implementers of the `text` modifier SHOULD support a sophisticated search functionality of the type offered by typical text indexing services.[§39](#fcss39) The value of the parameter is a text-based search, which may involve searching multiple words with thesaurus and proximity considerations, and logical operations such as AND, OR, etc.. Note that only a few servers are expected to offer this facility.

Implementers could consider using the rules specified by the [OData specification for the $search parameter ![icon](external.png)](http://docs.oasis-open.org/odata/odata/v4.0/cs01/part1-protocol/odata-v4.0-cs01-part1-protocol.html#_The_$search_System) . Typical implementations would use Lucene, Solr, an SQL-based full text search, or some similar indexing service.

`text` is only allowed on search parameters of type [string](#string). When using the 'text' modifier, all typically-valid search parameter string inputs are allowed.

For example, assuming a search parameter `section-text` that applies to `Composition.section.text`, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Composition?section:text=(bone OR liver) and metastases POST \[base\]/Composition/\_search Content-Type: application/x-www-form-urlencoded section:text=(bone OR liver) and metastases for compositions about metastases in the bones or liver of subjects will search for those literal values, but may also search for terms such as 'cancerous growth', 'tumor', etc..

##### 3.2.1.5.5.15 text-advanced[](search.html#modifiertextadvanced "link to here")

The `text-advanced` modifier allows clients to request matching based on advanced string processing of the search parameter input against the text associated with a code or value. For example, `CodeableConcept.text`, `Coding.display`, or `Identifier.type.text`. Implementers of the `text-advanced` modifier SHOULD support a sophisticated search functionality of the type offered by typical text indexing services, but MAY support only basic search with minor additions (e.g., word-boundary recognition).[§40](#fcss40) The value of the parameter is a text-based search, which may involve searching multiple words with thesaurus and proximity considerations, and logical operations such as AND, OR, etc..

`text-advanced` is allowed on search parameters of type [reference](#reference) and [token](#token). When using the 'text-advanced' modifier, all typically-valid search parameter string inputs are allowed.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Condition?code:text-advanced=headache POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded code:text-ad=headache would match Condition resources containing codes with text that equals or begins with 'headache', case-insensitive (e.g., same as the [text](#modifiertexttoken) modifier) such as:

*   SNOMED `25064002` - Headache finding
*   SNOMED `398987004` - Headache following lumbar puncture
*   SNOMED `230480006` - Headache following myelography (disorder)
*   ICD-10 `R51` - Headache
*   ICD-10 `R51.0` - Headache with orthostatic component, not elsewhere classified
*   ICD-10 `R51.9` - Headache, unspecified
*   etc.

However, it would also match Condition resources containing codes with text _containing_ the word 'headache' such as:

*   SNOMED `735938006` - Acute headache
*   SNOMED `95660002` - Thunderclap headache
*   SNOMED `4969004` - Sinus headache
*   ICD-10 `G44.019` - Episodic cluster headache, not intractable
*   ICD-10 `G44.81` - Hypnic headache
*   etc.

Additionally, a server may return Condition resources with codes containing synonymous text such as conditions with the codes:

*   SNOMED `37796009` - Migraine
*   SNOMED `49605003` - Ophthalmoplegic migraine (disorder)
*   ICD-10 `G43.4` - Hemiplegic migraine
*   ICD-10 `G43.B0` - Ophthalmoplegic migraine, not intractable
*   etc.

##### 3.2.1.5.5.16 \[type\][](search.html#modifiertype "link to here")

The `[type]` modifier allows clients to restrict the resource type when following a resource reference. The modifier does not use the literal '\[type\]' in any way, but rather the name of a resource - e.g., `Patient`, `Encounter`, etc.. Note that servers can impose restrictions on the use of the modifier (e.g., only allowing on local references, only allowing if `Reference.type` is populated, etc.) and allow for extended behavior (e.g., allowing on `canonical` references, etc.). Note that since external references are always absolute references, there can be no ambiguity about the resource type when resolving.

When resolving references, it is possible that a resolved resource does not match the specified type. In this case, the reference is considered to not match the search parameter. If a reference resolves to a type different than indicated or expected, a server SHOULD consider if it is appropriate to include the issue via an `OperationOutcome` in the response.[§41](#fcss41)

`[type]` is only allowed on [reference](#reference) type search parameters that target elements with the [Reference](references.html#Reference) FHIR type. When using the '\[type\]' modifier, all typically-valid search parameter reference inputs are allowed, but the value format is restricted to only `[id]`.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?subject:Patient=23 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded subject:Patient=23 for observations where the subject is 'Patient 23' is functionally equivalent to: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?subject=Patient/23 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded subject=Patient/23 as well as: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?patient=23 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded patient=23 However, the modifier becomes more useful when used with [Chaining](#chaining) and [Reverse Chaining](#has) of search parameters.

#### 3.2.1.5.6 Prefixes[](search.html#prefix "link to here")

For the ordered parameter types of [number](#number), [date](#date), and [quantity](#quantity), a prefix to the parameter value may be used to control the nature of the matching. To avoid URL escaping and visual confusion, the prefixes described in the table below are used. Note that in the following table:

*   `parameter value` refers to the value provided to search against (e.g., the parameter value in the request)
*   `resource value` refers to the value being tested against in a resource (e.g., an element value in a data store)

**Prefix Code**

**Description**

**Formal Definition**

`eq`

The resource value is equal to or fully contained by the parameter value.

The range of the parameter value fully contains the range of the resource value.  
**Boundary interpretation:** The lower boundary of the resource value is greater than or equal to the lower boundary of the parameter value AND the upper boundary of the resource value is less than or equal to the upper boundary of the parameter value.  
**Resource interval tests:**  
 `resource-lower: [parameter-lower, parameter-upper]`  
 `resource-upper: [parameter-lower, parameter-upper]`

`ne`

The resource value is not equal to the parameter value.

The range of the parameter value does not fully contain the range of the resource value.  
**Boundary interpretation:** The lower boundary of the resource value is greater than the upper boundary of the parameter value OR the upper boundary of the resource value is less than the lower boundary of the parameter value.  
**Resource interval tests:**  
 `resource-lower: (parameter-upper, ∞)`  
 `resource-upper: (-∞, parameter-lower)`

`gt`

The resource value is greater than the parameter value.

The range above the parameter value intersects (i.e., overlaps) with the range of the resource value.  
**Boundary interpretation:** The upper boundary of the resource value is greater than the upper boundary of the parameter value.  
**Resource interval tests:**  
 `resource-upper: (parameter-upper, ∞)`

`ge`

The resource value is greater or equal to the parameter value.

The combined range from the parameter value (inclusive) to +infinite intersects (i.e., overlaps) with the range of the resource value.  
**Boundary interpretation:** The lower OR upper boundaries of the resource value are greater than or equal to the upper boundary of the parameter value.  
**Resource interval tests:**  
 `resource-lower: [parameter-lower, ∞)`  
 `resource-upper: [parameter-lower, ∞)`

`lt`

The resource value is less than the parameter value.

The range below the parameter value intersects (i.e., overlaps) with the range of the resource value.  
**Boundary interpretation:** The lower boundary of the resource value is less than the lower boundary of the parameter value.  
**Resource interval tests:**  
 `resource-lower: (-∞, parameter-lower)`

`le`

The resource value is less or equal to the parameter value.

The combined range from -infinite to the parameter value (inclusive) intersects (i.e., overlaps) with the range of the resource value.  
**Boundary interpretation:** The upper OR lower boundaries of the resource value are less than or equal to the upper boundary of the parameter value.  
**Resource interval tests:**  
 `resource-lower: (-∞, parameter-upper]`  
 `resource-upper: (-∞, parameter-upper]`

`sa`

The resource value starts after the parameter value.

The range of the parameter value does not overlap with the range of the resource value, and the range above the parameter value contains the range of the resource value.  
**Boundary interpretation:** The lower boundary of the resource value is greater than the upper boundary of the parameter value.  
**Resource interval tests:**  
 `resource-lower: (parameter-upper, ∞)`

`eb`

The resource value ends before the parameter value.

The range of the parameter value does not overlap with the range of the resource value, and the range below the parameter value contains the range of the resource value.  
**Boundary interpretation:** The upper boundary of the resource value is less than the lower boundary of the parameter value.  
**Resource interval tests:**  
 `resource-upper: (-∞, parameter-lower)`

`ap`

The resource value is approximately the same to the parameter value.  
Note that the recommended value for the approximation is 10% of the stated value (or for a date, 10% of the gap between now and the date), but systems may choose other values where appropriate.

The range of the parameter value overlaps with the range of the resource value.  
**Boundary interpretation:** The lower boundary of the resource value is less than or approximately the upper boundary of the parameter value AND the upper boundary of the resource value is greater than or approximately the lower boundary of the parameter value.  
 `resource-lower: [~parameter-lower, ~parameter-lower]`  
 `resource-upper: [~parameter-upper, ~parameter-upper]`

Following is a table showing some example ranges and how they interact with the prefixes, with `[P]` representing the parameter range and `[R]` representing the resource range, with each additional column showing whether the test result is a match or not. Empty columns indicate no match, while a match for a particular test will show the prefix code used for that test. Note that for 'approximately', the actuality of a match depends on both the actual spans of ranges and the server's definition of 'approximately'. Any test that could possibly match is indicated with `ap`.

Test

`eq`

`ne`

`gt`

`ge`

`lt`

`le`

`sa`

`eb`

`ap`

             |--\[P\]--|
|--\[R\]--|

`ne`

`lt`

`le`

`eb`

        |--\[P\]--|
|--\[R\]--|

`ne`

`lt`

`le`

    |--\[P\]--|
|--\[R\]--|

`ne`

`lt`

`le`

    |--\[P\]--|
|--\[R\]------|

`ne`

`lt`

`le`

  |--\[P\]--|
|----\[R\]----|

`ne`

`gt`

`ge`

`lt`

`le`

`ap`

 |----\[P\]----|
\[R\]

`eq`

`ge`

`le`

`ap`

|----\[P\]----|
|--\[R\]--|

`eq`

`ge`

`le`

`ap`

|--\[P\]--|
|--\[R\]--|

`eq`

`ge`

`le`

`ap`

|--\[P\]--|
|--\[R\]----|

`ne`

`gt`

`ge`

`ap`

|----\[P\]----|
  |--\[R\]--|

`eq`

`ge`

`le`

`ap`

|---\[P\]---|
  |--\[R\]--|

`eq`

`ge`

`le`

`ap`

|--\[P\]--|
    |--\[R\]--|

`ne`

`gt`

`ge`

`ap`

|----\[P\]----|
           \[R\]

`eq`

`ge`

`le`

`ap`

|--\[P\]--|
        |--\[R\]--|

`ne`

`gt`

`ge`

`le`

|--\[P\]--|
            |--\[R\]--|

`ne`

`gt`

`ge`

`sa`

Note that prefixes **always** test against values present in elements. In all cases, a prefix-based test against an element that does not exist will fail the test. To search for elements missing values, the use of search modifiers is required - either [`missing`](#modifiermissing) or [`not`](#modifiernot).

Prefixes are allowed in searches that combine multiple terms. When using a prefix in "or" joined search parameter values, the prefix only applies to the value it precedes. For example, in `value-quantity=lt60,gt100`, the `lt` prefix is applied to the value "60" and the `gt` prefix is applied to the value "100". Additional details can be found in the [Searching Multiple Values](#multivalue) section of the Search page.

If no prefix is present, the prefix `eq` is assumed. Note that the way search parameters operate is not the same as the way the operations on two numbers work in a mathematical sense. `sa` (`starts-after`) and `eb` (`ends-before`) are not used with integer values but are used for decimals.

For each prefix above, two interpretations are provided - the simple intent of the prefix and the interpretation of the parameter when applied to ranges. The range interpretation is provided for decimals and dates. Searches that support prefixes are always performed on values that are implicitly or explicitly a range. For instance, the number 2.0 has an implicit range of 1.95 to 2.05, and the date 2015-08-12 has an implicit range of all the time during that day. If the resource value is a [Range](datatypes.html#range), a [Period](datatypes.html#period), or a [Timing](datatypes.html#timing), then the target is explicitly a range. Three ranges are identified:

**Decimal Values**

range of the value

The limits implied by the precision of the value

The number 2.0 has a range of 1.95 to 2.05  
The range 1.5 -> 2.3 has a range 1.50000000000000000000 to 2.50000000000000000000 (range is explicit, boundaries are treated as precise)

range below the value

Up to the specified value

The range below 2.0 includes any value less or equal to <2.00000000000000000000  
The range below 1.5 -> 2.3 includes any value less or equal to <1.50000000000000000000

range above the value

The specified value and up

The range above 2.0 includes any value greater or equal to <2.00000000000000000000  
The range above 1.5 -> 2.3 includes any value greater or equal to <2.30000000000000000000

**Date / Time Values**

range of the value

The limits implied by the precision of the value

The date 2015-08-12 has a range from 2015-08-12T00:00:00.0000 inclusive to 2015-08-13T00:00:00.0000 exclusive  
The date 2015-08 has a range from 2015-08-12T00:00:00.0000 inclusive to 2015-09-01T00:00:00.0000 exclusive

range below the value

Up to the specified value

The range before 2015-08-12T05:23:45 includes any time up to 2015-08-12T05:23:45.000000000000000  
The range before 2015-08 includes any time up to 2015-08-00T00:00:00.000000000000000

range above the value

The specified value and up

The range after 2015-08-12T05:23:45 includes any time from 2015-08-12T05:23:46.000000000000000 and later  
The range after 2015-08 includes any time from 2015-09-01T00:00:00.000000000000000 and later

Additional details about how ranges are applied are specific to each data type and are documented in the relevant search data type.

#### 3.2.1.5.7 Escaping Search Parameters[](search.html#escaping "link to here")

In the rules described above, special rules are defined for the characters `$`, `,`, and `|`. As a consequence, if these characters appear in an actual parameter value, they must be differentiated from their use as separator characters. When any of these characters appear in an actual parameter value, they must be prepended by the character `\`, which also must be used to prepend itself. Therefore, `param=xxx$xxx` indicates that it is a composite parameter, while `param=xx\$xx` indicates that the parameter has the literal value `xx$xx`. The parameter value `xx\xx` is illegal, and the parameter value `param=xx\\xx` indicates a literal value of `xx\xx`. This means that: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?code=a,b POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded code=a,b is a request for any Observation that has a code of either `a` or `b`, whereas: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?code=a\\,b POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded code=a\\,b is a request for any Observation that has a code of `a,b`.

This escaping is at a different level to the percent encoding that applies to all URL parameters (as defined in [RFC 3986 ![icon](external.png)](https://datatracker.ietf.org/doc/html/rfc3986) ). Standard percent escaping still applies, such that these URL strings have the same meaning:

*   `http://acme.org/fhir/ValueSet/123,http://acme.org/fhir/ValueSet/124,ValueSet/125`
*   `http://acme.org/fhir/ValueSet/123,http://acme.org/fhir/ValueSet/124\,ValueSet/125`
*   `http%3A%2F%2Facme.org%2Ffhir%2FValueSet%2F123%2Chttp%3A%2F%2Facme.org%2Ffhir%2FValueSet%2F124%2CValueSet%2F125`

Note that all of the above representations would be transmitted via an HTTP request in the same form as: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/ValueSet?url=http%3A%2F%2Facme.org%2Ffhir%2FValueSet%2F123%2Chttp%3A%2F%2Facme.org%2Ffhir%2FValueSet%2F124%2CValueSet%2F125 POST \[base\]/ValueSet/\_search Content-Type: application/x-www-form-urlencoded url=http%3A%2F%2Facme.org%2Ffhir%2FValueSet%2F123%2Chttp%3A%2F%2Facme.org%2Ffhir%2FValueSet%2F124%2CValueSet%2F125

Note that an escaped delimiter (e.g., an escaped comma) can appear within a multi-value delimited request, for example: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/ValueSet?url=http://acme.org/fhir/ValueSet/123,http://acme.org/fhir/ValueSet/124\\,ValueSet/125 POST \[base\]/ValueSet/\_search Content-Type: application/x-www-form-urlencoded url=http://acme.org/fhir/ValueSet/123,http://acme.org/fhir/ValueSet/124\\,ValueSet/125 is a request for ValueSets with a url literal of `http://acme.org/fhir/ValueSet/123` or `http://acme.org/fhir/ValueSet/124,ValueSet/125`.

This specification defines this additional form of escape because the escape syntax using backslash-escaping (`\`) applies to all parameter values after they have been 'unescaped' on the server while being read from the HTTP headers.

#### 3.2.1.5.8 Search Types and FHIR Types[](search.html#type-mapping "link to here")

The following table shows the mapping between search types and FHIR types that they can be tested against.

Note that in addition to elements of the specified type, any search type is allowed to be tested against elements that contain other elements, such as [Backbone Elements](types.html#BackboneElement) and complex [datatypes](datatypes.html), that contain at least one element of a valid FHIR type.

Generally, search testing against complex elements are performed by "or-joining" each valid sub-element of an appropriate FHIR type (see [Combining](search.html#combining) for details). However, some types may include additional guidance or specific behavior - for example, when performing [string](search.html#string) searches against [HumanName](datatypes.html#humanname) and [Address](datatypes.html#address) elements (see [string](search.html#string)). For clarity, the table below generally excludes FHIR types that are just containers for other elements.

Search Type

Short

FHIR Types

[date](datatypes.html#date)

Partial or complete date or time values

[date](datatypes.html#date), [dateTime](datatypes.html#dateTime), [instant](datatypes.html#instant), [Period](datatypes.html#Period), [Timing](datatypes.html#Timing)

[number](datatypes.html#number)

Numbers of various types

[decimal](datatypes.html#decimal), [integer](datatypes.html#integer), [integer64](datatypes.html#integer64), [unsignedInt](datatypes.html#unsignedInt), [positiveInt](datatypes.html#positiveInt)

[quantity](datatypes.html#quantity)

Quantity values (including currencies)

[Age](datatypes.html#Age), [Count](datatypes.html#Count), [Distance](datatypes.html#Distance), [Duration](datatypes.html#Duration), [MoneyQuantity](datatypes.html#MoneyQuantity), [Quantity](datatypes.html#Quantity), [SimpleQuantity](datatypes.html#SimpleQuantity),

[reference](datatypes.html#reference)

References to other resources

[canonical](references.html#canonical), [OID](references.html#oid), [URI](references.html#uri), [URL](references.html#url), [UUID](references.html#uuid), [Reference](references.html#Reference)

[string](datatypes.html#string)

String values

[id](datatypes.html#id), [markdown](datatypes.html#markdown), [string](datatypes.html#string), [xHTML](datatypes.html#xhtml)

[token](datatypes.html#token)

Token values

[boolean](datatypes.html#boolean), [canonical](datatypes.html#canonical), [code](datatypes.html#code), [CodeableConcept](datatypes.html#CodeableConcept), [Coding](datatypes.html#Coding), [ContactPoint](datatypes.html#ContactPoint), [id](datatypes.html#id), [Identifier](datatypes.html#Identifier), [OID](datatypes.html#oid), [string](datatypes.html#string), [URI](datatypes.html#uri), [URL](datatypes.html#url), [UUID](datatypes.html#uuid)

[uri](datatypes.html#uri)

URI values

[canonical](datatypes.html#canonical), [OID](datatypes.html#oid), [URI](datatypes.html#uri), [URL](datatypes.html#url), [UUID](datatypes.html#uuid)

#### 3.2.1.5.9 date[](search.html#date "link to here")

A date parameter searches on a date/time or period. As is usual for date/time related functionality, while the concepts are relatively straight-forward, there are a number of subtleties involved in ensuring consistent behavior.

The date parameter format is `yyyy-mm-ddThh:mm:ss.ssss[Z|(+|-)hh:mm]` (the standard XML format). Note that fractional seconds MAY be ignored by servers.[§42](#fcss42)

The `date` search type can be used to represent data from any of the [date](datatypes.html#date), [dateTime](datatypes.html#dateTime), and [instant](datatypes.html#instant) datatypes. Any degree of precision can be provided, but it SHALL be populated from the left (e.g., can't specify a month without a year), except that the minutes SHALL be present if an hour is present, and you SHOULD provide a timezone if the time part is present.[§43](#fcss43) Note: Time can consist of hours and minutes with no seconds, unlike the XML Schema dateTime type. Some user agents may escape the `:` characters in the URL, and servers SHALL handle this correctly.[§44](#fcss44)

Date searches are intrinsically matches against 'periods', regardless of the underlying element type. Date parameters can be used with the following data types:

[date](datatypes.html#date)

The period start and end are both set to the date value.

[dateTime](datatypes.html#dateTime)

The period start and end are both set to the dateTime value; e.g., the date 2013-01-10 specifies all the time from 00:00 on 10-Jan 2013 to immediately before 00:00 on 11-Jan 2013.

[instant](datatypes.html#instant)

The period start and end are both set to the instant value.

[Period](datatypes.html#Period)

No conversion necessary; the period is explicit, though note that the upper or lower bound might not be specified in resources.

[Timing](datatypes.html#Timing)

The period has a start of the lowest dateTime within the timing and an end of the highest dateTime within the timing. The specified scheduling details are ignored and only the outer limits matter. For instance, a schedule that specifies a certain time on every second day between 31-Jan 2013 and 24-Mar 2013 includes 1-Feb 2013, even though that is on an odd day that is not specified by the period. Similarly, the period would include the start of 31-Jan 2023 through the end of 24-Mar 2023, ignoring the scheduled time (unless the outer limits were accurate to that time). This is to keep the server load processing queries reasonable. **Note that servers are allowed to process internal intervals, if it is desired and relevant.**

The 'period' of a date search is from the earliest instant of the value through to the latest instant of the value. When comparing, the period start is adjusted to have maximum precision with the lowest possible value of all non-specified levels. For example, first month of the year, first day of the month, 0-filled time. The period end is adjusted to have maximum precision with the highest possible value of all non-specified levels - last month of the year, last day of the month (allowing for leap years), last time of the day (i.e. 23:59:59.99999).

For more information about how the different search prefixes work when comparing periods/ranges, refer to the [Prefixes](#prefix) section.

A missing lower boundary is "less than" any actual date. A missing upper boundary is "greater than" any actual date. Date searches can be controlled through the use of prefixes as described in the following table:

`[parameter]=eq2013-01-14`

*   2013-01-14T00:00 matches (obviously)
*   2013-01-14T10:00 matches
*   2013-01-15T00:00 does not match - it's not in the range

`[parameter]=ne2013-01-14`

*   2013-01-15T00:00 matches - it's not in the range
*   2013-01-14T00:00 does not match - it's in the range
*   2013-01-14T10:00 does not match - it's in the range

`[parameter]=lt2013-01-14T10:00`

*   2013-01-14 matches, because it includes the part of 14-Jan 2013 before 10am
*   "2013-01-13T12:00 to 2013-01-14T12:00" matches, because it includes the part of 14-Jan 2013 until noon
*   "2013-01-14T08:00 to 2013-01-15T08:00" matches, because it includes the part of 14-Jan 2013 before 10:00

`[parameter]=gt2013-01-14T10:00`

*   2013-01-14 matches, because it includes the part of 14-Jan 2013 after 10am
*   "2013-01-13T12:00 to 2013-01-14T12:00" matches, because it includes the part of 14-Jan 2013 until noon
*   "2013-01-14T12:00 to 2013-01-15T12:00" matches, because it includes the part of 14-Jan 2013 after noon

`[parameter]=ge2013-03-14`

*   "from 21-Jan 2013 onwards" is included because that period may include times after 14-Mar 2013

`[parameter]=ge2015-04-13T20:27:01-04:00`

*   matches a Period `[2015-04-13T20:27:01-04:00, 2015-04-13T20:42:01-04:00)` because the combined search range overlaps starting at the boundary instant.
*   does NOT match a Period `[2015-04-13T20:26:01-04:00, 2015-04-13T20:27:01-04:00)` because the resource ends exactly at the parameter instant and the end is exclusive, so there is no overlap.

`[parameter]=le2013-03-14`

*   "from 21-Jan 2013 onwards" is included because that period may include times before 14-Mar 2013

`[parameter]=le2015-04-13T20:27:01-04:00`

*   matches a [Period](datatypes.html#period) with range `[2015-04-13T20:27:01-04:00, 2015-04-13T20:42:01-04:00)` because the combined search range overlaps the resource at the inclusive boundary instant.
*   does NOT match a Period `[2015-04-13T20:27:02-04:00, 2015-04-13T20:42:01-04:00)` because there is no overlap with`(-∞, 2015-04-13T20:27:01-04:00]`.

`[parameter]=sa2013-03-14`

*   "from 15-Mar 2013 onwards" is included because that period starts after 14-Mar 2013
*   "from 21-Jan 2013 onwards" is not included because that period starts before 14-Mar 2013
*   "before and including 21-Jan 2013" is not included because that period starts (and ends) before 14-Mar 2013
*   "2013-01-13T12:00 to 2013-01-14T12:00" does not match, because it starts before 14-Jan 2013 begins
*   "2013-01-14T12:00 to 2013-01-15T12:00" does not match, because it starts before 14-Jan 2013 ends

`[parameter]=eb2013-03-14`

*   "from 15-Mar 2013 onwards" is not included because that period starts after 14-Mar 2013
*   "from 21-Jan 2013 onwards" is not included because that period starts before 14-Mar 2013, but does not end before it
*   "before and including 21-Jan 2013" is included because that period ends before 14-Mar 2013
*   "2013-01-13T12:00 to 2013-01-14T12:00" does not match, because it ends after 14-Jan 2013 begins
*   "2013-01-14T12:00 to 2013-01-15T12:00" does not match, because it starts before 14-Jan 2013 ends

`[parameter]=ap2013-03-14`

*   Note that actual comparison used for the `ap` prefix for `date` search types is an implementation detail. For these examples, the system will be based on a 10% difference between target and execution date, and execution on 01-Jan 2023.
*   14-Mar 2013 is included, because it exactly matches
*   21-Jan 2013 is included, because it is near 14-Mar 2013 (less than 10% difference between search date and current date)
*   15-Jun 2015 is not included, because it is not near 14-Mar 2013 (more than 10% difference between search date and current date)

Please note in particular the differences between range comparison and boundary comparison prefixes when comparing ranges on both sides. Tests such as 'greater than' are evaluated true if _any part_ of the value range is higher than the test range. Similarly, a test such as 'starts after' is non-inclusive of the range of the test value, so a request such as `sa2013-01-14` is requesting values that start after January 14th has passed, not after January 14th started. In order to avoid unintentional overlapping of ranges, requests can increase the specificity of their request. For example, the test for `sa2013-01-14T00:00:00` is a much clearer test.

Other notes:

*   Matches against dates are based on the behavior of intervals, where:
    *   Dates with only the year specified are equivalent to an interval that starts at the first instant of January 1st to the last instant of December 31st, e.g., 2000 is equivalent to an interval of \[2000-01-01T00:00, 2000-12-31T23:59\].
    *   Dates with the year and month are equivalent to an interval that starts at the first instant of the first day of the month and ends on the last instant of the last day of the month, e.g., 2000-04 is equivalent to an interval of \[2000-04-01T00:00, 2000-04-30T23:59\].
    *   Dates with the year, month, and day are equivalent to an interval that starts at the first instant of day and ends on the last instant of the day, e.g. 2000-04-04 is equivalent to an interval of \[2000-04-04T00:00, 2000-04-04T23:59.9999\].
*   Where possible, the system SHOULD correct for timezones when performing queries.[§45](#fcss45) Dates do not have timezones, and timezones SHOULD NOT be considered.[§46](#fcss46) Where both search parameters and resource element date-times do not have timezones, the servers local timezone SHOULD be assumed.[§47](#fcss47)

To search for all the procedures in a patient compartment that occurred over a 2-year period: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient/23/Procedure?date=ge2010-01-01&date=le2011-12-31 POST \[base\]/Patient/23/\_search Content-Type: application/x-www-form-urlencoded Patient/23/Procedure?date=ge2010-01-01&date=le2011-12-31

![](assets/images/dragon.png "Here Be Dragons!")

Managing timezones and offsets and their impact on search is a very difficult area. The FHIR implementation community is still investigating and debating the best way to handle timezones. Implementation guides may make additional rules in this regard.

Future versions of this specification may impose rules around the use of timezones with dates. Implementers and authors of implementation guides should be aware of ongoing work in this area.

Implementer feedback is welcome [on the issue tracker ![icon](external.png)](http://hl7.org/fhir-issues) or [chat.fhir.org ![icon](external.png)](https://chat.fhir.org/#narrow/stream/4-implementers) .

#### 3.2.1.5.10 number[](search.html#number "link to here")

Searching on a simple numerical value in a resource. Examples:

`[parameter]=100`

Values that equal 100, to 3 significant figures precision, so this is actually searching for values in the range \[99.5 ... 100.5)

`[parameter]=100.00`

Values that equal 100, to 5 significant figures precision, so this is actually searching for values in the range \[99.995 ... 100.005)

`[parameter]=1e2`

Values that equal 100, to 1 significant figure precision, so this is actually searching for values in the range \[50 ... 150)

`[parameter]=lt100`

Values that are less than exactly 100

`[parameter]=le100`

Values that are less or equal to exactly 100

`[parameter]=gt100`

Values that are greater than exactly 100

`[parameter]=ge100`

Values that are greater or equal to exactly 100

`[parameter]=ne100`

Values that are not equal to 100 (actually, in the range 99.5 to 100.5)

Notes about searching on Numbers:

*   When a number search is used against a resource element that stores a simple integer (e.g. [ImmunizationRecommendation.recommendation.doseNumber ![icon](external.png)](https://build.fhir.org/ig/HL7/immunization-incubator/en/StructureDefinition-ImmunizationRecommendation-definitions.html#ImmunizationRecommendation.recommendation.doseNumber) ), and the search parameter is not expressed using the exponential forms, and does not include any non-zero digits after a decimal point, the significance issues cancel out and searching is based on exact matches. Note that if there are non-zero digits after a decimal point, there cannot be any matches
*   When a comparison prefix in the set `gt, lt, ge, le, sa & eb` is provided, the implicit precision of the number is ignored, and they are treated as if they have arbitrarily high precision
*   The way search parameters operate in resources is not the same as whether two numbers are equal to each other in a mathematical sense
*   Searching on decimals without using one of the comparisons listed in the earlier bullet involves an implicit range. The number of significant digits of the implicit range is the number of digits specified in the search parameter value, excluding leading zeros. So 100 and 1.00e2 both have the same number of significant digits - three

Here are some example searches:

Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post)

**Search**

**Description**

GET \[base\]/RiskAssessment?probability=gt0.8 POST \[base\]/RiskAssessment/\_search Content-Type: application/x-www-form-urlencoded probability=gt0.8

Search for all the Risk Assessments with probability great than 0.8 (could also be `probability=gt8e-1` using exponential form)

GET \[base\]/ImmunizationRecommendation?dose-number=2 POST \[base\]/ImmunizationRecommendation/\_search Content-Type: application/x-www-form-urlencoded dose-number=2

Search for any immunization recommendation recommending a second dose

#### 3.2.1.5.11 quantity[](search.html#quantity "link to here")

A quantity parameter searches on the [Quantity](datatypes.html#Quantity) datatype. The syntax for the value follows the form (see [Encoding Note](#encoding-note)):

*   `[parameter]={[prefix]}[number]` matches a quantity by `value`, with an optional prefix
*   `[parameter]={[prefix]}[number]|[system]|[code]` matches a quantity by `value`, `system` and `code`, with an optional prefix
*   `[parameter]={[prefix]}[number]||[code]` matches a quantity by `value` and `code` or `unit`, with an optional prefix

The prefix is optional, and is as described in the section on [Prefixes](#prefix), both regarding how precision and comparator/range operators are interpreted. Like a number parameter, the number part of the search value can be a decimal in exponential format. The `system` and `code` follow the same pattern as [token parameters](#token) are also optional. Note that when the `[system]` component has a value, it is implied that a precise (and potentially canonical) match is desired. In this case, it is inappropriate to search on the human display for the unit, which can be is uncontrolled and may unpredictable. Example searches:

Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post)

**Search**

**Description**

GET \[base\]/Observation?value-quantity=5.4|http://unitsofmeasure.org|mg POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded value-quantity=5.4|http://unitsofmeasure.org|mg

Search for all the observations with a value of 5.4(+/-0.05) mg where mg is understood as a UCUM unit (`system`/`code`)

GET \[base\]/Observation?value-quantity=5.40e-3|http://unitsofmeasure.org|g POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded value-quantity=5.40e-3|http://unitsofmeasure.org|g

Search for all the observations with a value of 0.0054(+/-0.00005) g where g is understood as a UCUM unit (`system`/`code`)

GET \[base\]/Observation?value-quantity=5.4||mg POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded value-quantity=5.4||mg

Search for all the observations with a value of 5.4(+/-0.05) mg where the unit - either the code (`code`) or the stated human unit (`unit`) are "mg"

GET \[base\]/Observation?value-quantity=5.4 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded value-quantity=5.4

Search for all the observations with a value of 5.4(+/-0.05) irrespective of the unit

GET \[base\]/Observation?value-quantity=le5.4|http://unitsofmeasure.org|mg POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded value-quantity=le5.4|http://unitsofmeasure.org|mg

Search for all the observations where the value of is less than 5.4 mg exactly where mg is understood as a UCUM unit

GET \[base\]/Observation?value-quantity=ap5.4|http://unitsofmeasure.org|mg POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded value-quantity=ap5.4|http://unitsofmeasure.org|mg

Search for all the observations where the value of is about 5.4 mg where mg is understood as a UCUM unit (typically, within 10% of the value - see above)

Specifying a system and a code for the search implies that the search is based on a particular code system - usually [UCUM ![icon](external.png)](http://unitsofmeasure.org) , and that a precise (and potentially canonical) match is desired. In this case, it is inappropriate to search on the human display for the unit, which can be is uncontrolled and may be unpredictable.

The search processor may choose to perform a search based on [canonical units](datatypes.html#quantity) (e.g., any value where the units can be converted to a value in mg in the case above). For example, an observation may have a value of `23 mm/hr`. This is equal to `0.023 m/hr`. The search processor can choose to normalize all the values to a canonical unit such as `6.4e-6 m/sec`, and convert search terms to the same units (m/sec). Such conversions can be performed based on the semantics defined in [UCUM ![icon](external.png)](http://unitsofmeasure.org) . Servers that do not search based on canonical units will not be able to search against ranges whose low and high quantities use different units or where the search expression uses different units than are found in low and high.

#### 3.2.1.5.12 reference[](search.html#reference "link to here")

The _reference_ search parameter type is used to search [references between resources](references.html). For example, find all Conditions where the subject is a particular patient, where the patient is selected by name or identifier. The interpretation of a _reference_ parameter is either (see [Encoding Note](#encoding-note)):

*   **`[parameter]=[id]`** the logical \[id\] of a resource using a local reference (i.e., a relative reference).
*   **`[parameter]=[type]/[id]`** the logical \[id\] of a resource of a specified type using a local reference (i.e., a relative reference), for when the reference can point to different types of resources (e.g., [Observation.subject](observation-definitions.html#Observation.subject)).
*   **`[parameter]=[type]/[id]/_history/[version]`** the logical \[id\] of a resource of a specified type using a local reference (i.e., a relative reference), for when the reference can point to different types of resources and a specific version is requested. Note that server implementations may return an error when using this syntax if resource versions are not supported. For more information, see [References and Versions](#versions).
*   **`[parameter]=[url]`** where the \[url\] is an absolute URL - a reference to a resource by its absolute location, or by its canonical URL
*   **`[parameter]=[url]|[version]`** where the search element is a canonical reference, the \[url\] is an absolute URL, and a specific version or partial version is desired. For more information, see [References and Versions](#versions).

Notes:

*   A relative reference resolving to the same value as a specified absolute URL, or vice versa, qualifies as a match.
*   If a reference value is a non-versioned relative reference (e.g., does not contain `[url]` or `_history/[version]` parts), the search SHOULD match instances that match the reference in it contains a versioned reference.[§48](#fcss48)

For example, if the base URL of a server is http://example.org/fhir, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET http://example.org/fhir/Observation?subject=Patient/123 POST http://example.org/fhir/Observation/\_search Content-Type: application/x-www-form-urlencoded subject=Patient/123 will match Observations with `subject.reference` values:

*   `Patient/123` - exact match of search input
*   `http://example.org/fhir/Patient/123` - search input with implicit resolution to the local server
*   `Patient/123/_history/1` - reference to a specific version of the search input

Similarly, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET http://example.org/fhir/Observation?subject=http://example.org/fhir/Patient/123 POST http://example.org/fhir/Observation/\_search Content-Type: application/x-www-form-urlencoded subject=http://example.org/fhir/Patient/123 will match Observations with `subject.reference` values:

*   `http://example.org/fhir/Patient/123` - exact match of search input
*   `Patient/123` - search input with implicit reference to the local server

Note that it will **not** match an Observation with `Patient/123/_history/1`, since the original reference was not a relative reference.

Also, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET http://example.org/fhir/Observation?subject=123 POST http://example.org/fhir/Observation/\_search Content-Type: application/x-www-form-urlencoded subject=123 will match Observations with `subject.reference` values:

*   `Patient/123` - search input of type Patient
*   `http://example.org/fhir/Patient/123` - search input with implicit resolution to the local server, of type Patient
*   `Practitioner/123` - search input of type Practitioner
*   `http://example.org/fhir/Practitioner/123` - search input with implicit resolution to the local server, of type Practitioner
*   etc.

Some resource references may point to more than one type of resource; e.g., `subject: Reference(Patient|Group|Device|..)`. In these cases, multiple resources may have the same logical identifier. Servers SHOULD fail a search where the logical id in a reference refers to more than one matching resource across different types.[§49](#fcss49) In order to allow the client to perform a search in these situations the type is specified explicitly: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?subject=Patient/23 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded subject=Patient/23

This searches for any observations where the subject refers to the patient resource with the logical identifier "23". A modifier is also defined to allow the client to be explicit about the intended type: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?subject:Patient=23 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded subject:Patient=23

This has the same effect as the previous search. The modifier becomes useful when used with chaining as explained in the next section. Note: The `[type]` modifier can't be used with a reference to a resource found on another server, since the server would not usually know what type that resource has. However, since external references are always absolute references, there can be no ambiguity about the type.

In some cases, search parameters are defined with an implicitly limited scope. For example, [`Observation`](observation.html) has an element `subject`, which is a reference to one of a number of types. This has a matching search parameter `subject`, which refers to any of the possible types. In addition to this, there is another search parameter `patient`, which also refers to `Observation.subject`, but is limited to only include references of type [`Patient`](patient.html). When using the patient search parameter, there is no need to specify ":Patient" as a modifier, or "Patient/" in the search value, as this must always be true.

FHIR [References](references.html#Reference) are also allowed to have an `identifier`. The modifier `:identifier` allows for searching by the identifier rather than the literal reference: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?subject:identifier=http://acme.org/fhir/identifier/mrn|123456 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded subject:identifier=http://acme.org/fhir/identifier/mrn|123456

This is a search for all observations that reference a patient by a particular patient MRN. When the :identifier modifier is used, the search value works as a [token search](#token). The :identifier modifier is not supported on canonical elements since they do not have an identifier separate from the reference itself.

Chaining is not supported when using the :identifier modifier, nor are chaining, includes or reverse includes supported for reference search parameters that target elements without a `reference` element. For example, a search parameter that targets an element with the [Reference](references.html#Reference) FHIR type can be chained through, but a search parameter that targets a [canonical](references.html#canonical) element cannot.

The reference search parameter type is mostly used for resource elements of type `Reference` or `canonical`. However, it is also be used to search resource elements of type [Resource](resource.html) - i.e., where one resource is directly nested within another - see the [Bundle search parameters](bundle.html#search) 'message' and 'composition' as an example of this.

#### 3.2.1.5.13 string[](search.html#string "link to here")

For a simple string search, a string parameter serves as the input for a search against sequences of characters. This search is insensitive to casing and included combining characters, like accents or other diacritical marks. Punctuation and non-significant whitespace (e.g., repeated space characters, tab vs space) SHOULD also be ignored.[§50](#fcss50) Note that case-insensitive comparisons do not take locale into account, and will result in unsatisfactory results for certain locales. Character case definitions and conversions are out of scope for the FHIR standard, and the results of such operations are implementation dependent. By default, a field matches a string query if the value of the field equals or starts with the supplied parameter value, after both have been normalized by case and combining characters. Therefore, the default string search only operates on the base characters of the string parameter. The `:contains` modifier returns results that include the supplied parameter value anywhere within the field being searched. The `:exact` modifier returns results that match the entire supplied parameter, including casing and accents.

When a string type search parameter points to a complex or backbone element (an element that contains sub-elements), by default the search SHOULD be considered as a search against one or more string values in sub-elements, as selected by the implementation.[§51](#fcss51) A search parameter MAY explicitly choose elements by using an expression that instead points directly to the sub-elements.[§52](#fcss52)

For example, if a search parameter is of type `string` and the expression points to `Patient.name`, the implementation can test against any one or more elements of the [HumanName](datatypes.html#humanname) datatype (e.g., `given`, `family`, `prefix`, `suffix`, etc.). However, if the search parameter intends to explicitly match against `given` and `family` only, the search parameter should use an expression of `Patient.name.given | Patient.name.family`.

If the SearchParameter's narrative description includes additional considerations about what fields are indexed, `SearchParameter.processingMode` should have the value `other`.

Examples:

Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post)

GET \[base\]/Patient?given=eve POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded given=eve

Any patients with a name containing a given part with "eve" at the start of the name. This would include patients with the given name "Eve", "Evelyn".

GET \[base\]/Patient?given:contains=eve POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded given:contains=eve

Any patients with a name with a given part containing "eve" at any position. This would include patients with the given name "Eve", "Evelyn", and also "Severine".

GET \[base\]/Patient?given:exact=Eve POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded given:exact=Eve

Any patients with a name with a given part that is exactly "Eve". Note: This would not include patients with the given name "eve" or "EVE".

An additional modifier `:text` can be used to specify a search with advanced text handling (see [below](#text)) though only a few servers are expected to offer this facility.

When a string search parameter refers to the types [HumanName](datatypes.html#HumanName) and [Address](datatypes.html#Address), the search covers the elements of type string, and does not cover elements such as `use` and `period`. For robust search, servers SHOULD search the parts of a family name independently.[§53](#fcss53) e.g., searching either "Carreno" or "Quinones" should match a family name of "Carreno Quinones". HL7 affiliates MAY make more specific recommendations about search expectations regarding names in their specific culture.[§54](#fcss54)

It is at the discretion of the server whether to pre-process names, addresses, and contact details to remove separator characters prior to matching in order to ensure more consistent behavior. For example, a server might remove all spaces and `-` characters from phone numbers. What is most appropriate varies depending on culture and context. A server may also use a free-text style searches on this property to achieve the best results.  
When searching whole names and addresses (not parts), servers may also use flexible match or a free-text style searches on names to achieve the best results.

#### 3.2.1.5.14 token[](search.html#token "link to here")

A token type is a parameter that provides a close to exact match search on a string of characters, potentially scoped by a URI. It is mostly used against a code or identifier datatype where the value may have a URI that scopes its meaning, where the search is performed against the pair from a Coding or an Identifier. Tokens are also used against other fields where exact matches are required - uris, booleans, [ContactPoints](datatypes.html#ContactPoint), and [ids](datatypes.html#id). In these cases the URI portion (`[system]|`) is not used (only the `[code]` portion).

For tokens, matches are literal (e.g., not based on [subsumption](codesystem.html#subsumption) or other code system features). Match is case sensitive unless the underlying semantics for the context indicate that tokens are to be interpreted case-insensitively (see, e.g., [CodeSystem.caseSensitive](codesystem-definitions.html#CodeSystem.caseSensitive)). Note that matches on `_id` are always case sensitive. If the underlying datatype is `[string](datatypes.html#string)` then the search is not case sensitive.

**Note**: There are many challenging issues around case sensitivity and token searches. Some code systems are case sensitive (e.g., UCUM) while others are known not to be. For many code systems, it's ambiguous. Other kinds of values are also ambiguous. When in doubt, servers SHOULD treat tokens in a case-insensitive manner, on the grounds that including undesired data has less safety implications than excluding desired behavior.[§55](#fcss55) Clients SHOULD always use the correct case when possible, and allow for the server to perform case-insensitive matching.[§56](#fcss56)

To use subsumption-based logic, use the modifiers below, or list all the codes in the hierarchy. The syntax for the value is one of the following (see [Encoding Note](#encoding-note)):

*   **`[parameter]=[code]`**: the value of `[code]` matches a Coding.code or Identifier.value irrespective of the value of the system property
*   **`[parameter]=[system]|[code]`**: the value of `[code]` matches a Coding.code or Identifier.value, and the value of `[system]` matches the system property of the Identifier or Coding
*   **`[parameter]=|[code]`**: the value of `[code]` matches a Coding.code or Identifier.value, and the Coding/Identifier has no system property
*   **`[parameter]=[system]|`**: any element where the value of `[system]` matches the system property of the Identifier or Coding

Notes:

*   The namespace URI and code both must be [escaped](#escaping) correctly. If a system is not applicable (e.g., an element of type [uri](datatypes.html#uri)), then just the form `[parameter]=[code]` is used.
*   For token parameters on elements of type [id](datatypes.html#id), [ContactPoint](datatypes.html#ContactPoint), [uri](datatypes.html#uri), or [boolean](datatypes.html#boolean), the pipe symbol SHALL NOT be used - only the `[parameter]=[code]` form is allowed[§57](#fcss57)

Token search parameters are used for the following datatypes:

**Datatype**

**URI**

**Code**

**Comments**

[Coding](datatypes.html#Coding)

Coding.system

Coding.code

[CodeableConcept](datatypes.html#CodeableConcept)

CodeableConcept.coding.system

CodeableConcept.coding.code

Matches against any coding in the CodeableConcept

[Identifier](datatypes.html#Identifier)

Identifier.system

Identifier.value

Clients can search by `type` not `system` using the :of-type modifier, see below. To search on a CDA `II.root` - which may appear in either `Identifier.system` or `Identifier.value`, use the syntax `identifier=|[root],[root]`

[ContactPoint](datatypes.html#ContactPoint)

ContactPoint.value

At the discretion of the server, token searches on ContactPoint may use special handling, such as ignoring punctuation, performing partial searches etc.

[code](datatypes.html#code)

(implicit)

code

the system is defined in the value set (though it's not usually needed)

[boolean](datatypes.html#boolean)

boolean

The implicit system for boolean values is [http://terminology.hl7.org/CodeSystem/special-values ![icon](external.png)](https://terminology.hl7.org/ValueSet-special-values.html) but this is never actually used

[id](datatypes.html#id)

id

[uri](datatypes.html#uri)

uri

[string](datatypes.html#string)

n/a

string

Token is sometimes used for string to indicate that case-insensitive full-string matching is the correct default search strategy

Note: The use of token search parameters for boolean fields: the boolean values "true" and "false" are also represented as formal codes in the [Special Values ![icon](external.png)](https://terminology.hl7.org/ValueSet-special-values.html) code system, which is useful when boolean values need to be represented in a [Coding](datatypes.html#coding) datatype. The namespace for these codes is http://terminology.hl7.org/CodeSystem/special-values, though there is usually no reason to use this, as a simple true or false is sufficient.

Most servers will only process value sets that are already known/registered/supported internally. However, servers can elect to accept any valid reference to a value set. Servers may elect to consider concept mappings when testing for subsumption relationships.

Example searches:

Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post)

**Search**

**Description**

GET \[base\]/Patient?identifier=http://acme.org/patient|2345 POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded identifier=http://acme.org/patient|2345

Search for all the patients with an identifier with key = "2345" in the system "http://acme.org/patient"

GET \[base\]/Patient?gender=male POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded gender=male

Search for any patient with a gender that has the code "male"

GET \[base\]/Patient?gender:not=male POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded gender:not=male

Search for any patient with a gender that does not have the code "male", including those that do not have a code for gender at all.

GET \[base\]/Composition?section=48765-2 POST \[base\]/Composition/\_search Content-Type: application/x-www-form-urlencoded section=48765-2

Search for any Composition that contains an Allergies and adverse reaction section

GET \[base\]/Composition?section:not=48765-2 POST \[base\]/Composition/\_search Content-Type: application/x-www-form-urlencoded section:not=48765-2

Search for any Composition that does not contain an Allergies and adverse reaction section. Note that this search does not return "any document that has a section that is not an Allergies and adverse reaction section" (e.g., in the presence of multiple possible matches, the _negation_ applies to the set, not each individual entry)

GET \[base\]/Patient?active=true POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded active=true

Search for any patients that are active

GET \[base\]/Condition?code=http://acme.org/conditions/codes|ha125 POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded code=http://acme.org/conditions/codes|ha125

Search for any condition with a code "ha125" in the code system "http://acme.org/conditions/codes"

GET \[base\]/Condition?code=ha125 POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded code=ha125

Search for any condition with a code "ha125". Note that there is not often any useful overlap in literal symbols between code systems, so the previous example is generally preferred

GET \[base\]/Condition?code:text=headache POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded code:text=headache

Search for any Condition with a code that has a text "headache" associated with it (either in the text, or a display)

GET \[base\]/Condition?code:in=http://snomed.info/sct?fhir\_vs=isa/126851005 POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded code:in=http://snomed.info/sct?fhir\_vs=isa/126851005

Search for any condition in the SNOMED CT value set "http://snomed.info/sct?fhir\_vs=isa/126851005" that includes all descendants of "Neoplasm of liver"

GET \[base\]/Condition?code:below=126851005 POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded code:below=126851005

Search for any condition that is subsumed by the SNOMED CT Code "Neoplasm of liver". Note: This is the same outcome as the previous search

GET \[base\]/Condition?code:in=http://acme.org/fhir/ValueSet/cardiac-conditions POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded code:in=http://acme.org/fhir/ValueSet/cardiac-conditions

Search for any condition that is in the institutions list of cardiac conditions

GET \[base\]/Patient?identifier:of-type=http://terminology.hl7.org/CodeSystem/v2-0203|MR|446053 POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded identifier:of-type=http://terminology.hl7.org/CodeSystem/v2-0203|MR|446053

Search for the Medical Record Number 446053 - this is useful where the system id for the MRN is not known

#### 3.2.1.5.15 uri[](search.html#uri "link to here")

The uri search parameter type is used to search elements that contain a URI ([RFC 3986 ![icon](external.png)](https://tools.ietf.org/html/rfc3986) ). By default, matches are precise, case and accent sensitive, and the entire URI must match. The modifier `:above` or `:below` can be used to indicate that partial matching is used. For example (note that the search parameter `ValueSet.url` is of type `uri`): Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/ValueSet?url=http://acme.org/fhir/ValueSet/123 POST \[base\]/ValueSet/\_search Content-Type: application/x-www-form-urlencoded url=http://acme.org/fhir/ValueSet/123 GET \[base\]/ValueSet?url:below=http://acme.org/fhir/ POST \[base\]/ValueSet/\_search Content-Type: application/x-www-form-urlencoded url:below=http://acme.org/fhir/ GET \[base\]/ValueSet?url:above=http://acme.org/fhir/ValueSet/123/\_history/5 POST \[base\]/ValueSet/\_search Content-Type: application/x-www-form-urlencoded url:above=http://acme.org/fhir/ValueSet/123/\_history/5 GET \[base\]/ValueSet?url=urn:oid:1.2.3.4.5 POST \[base\]/ValueSet/\_search Content-Type: application/x-www-form-urlencoded url=urn:oid:1.2.3.4.5

*   The first line is a request to find any value set with the exact url "http://acme.org/fhir/ValueSet/123"
*   The second line performs a search that will return any value sets that have a URL that starts with "http://acme.org/fhir/"
*   The third line shows the converse - search for any value set above a given specific URL. This will match on any value set with the specified URL, but also on http://acme.org/ValueSet/123. Note that there are not many use cases where :above is useful as compared to the `:below` search
*   The fourth line shows an example of searching by an OID. Note that the :above and :below modifiers only apply to URLs, and not URNS such as OIDs

The search type `uri` can be used with elements of type [canonical](datatypes.html#canonical), [uri](datatypes.html#uri), and [url](datatypes.html#url). The search type [reference](#reference) can also be used for those same types and is always used for the type [Reference](references.html#Reference). The search type [reference](#reference) is generally preferred for elements of type [canonical](datatypes.html#canonical) because it can handle versioned canonical values. Where more than one search type can apply to a search parameter, the decision of which to use is based on the desired search behavior.

#### 3.2.1.5.16 Special Parameters[](search.html#special "link to here")

A few parameters have the type 'special'. That indicates that the way this parameter works is unique to the parameter and described with the parameter. The general modifiers and comparators do not apply, except as stated in the description.

Implementers will generally need to do special implementations for these parameters. These parameters are special:

*   [\_filter](search_filter.html) (all resources)
*   `section-text` on [Composition](composition.html#search)
*   `contains` on [Location](location.html#search)
*   `near` on [Location](location.html#search)

#### 3.2.1.5.17 Composite Search Parameters[](search.html#composite "link to here")

Composite search parameters allow joining multiple elements into distinct single values with a `$`. This is different from doing a simple intersection - the intersection rules apply at the resource level, so, for example, an Observation with multiple [component](observation-definitions.html#Observation.component) repetitions may match because one repetition has a desired code and a _different_ repetition matches a value filter.

The `composite` parameter approach works in this context because it allows searches based on a tuples of values. For example: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?code-value-quantity=loinc|12907-2$ge150|http://unitsofmeasure.org|mmol/L POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded code-value-quantity=loinc|12907-2$ge150|http://unitsofmeasure.org|mmol/L will match Observation records with a LOINC code of "12907-2" (Sodium \[Moles/volume\] in Red Blood Cells) AND a value greater than "150 mmol/L".

Note that the sequence is a single value and itself can be composed into a set of values. For example, searching [Group.characteristic](group-definitions.html#Group.characteristic), searching for multiple key/value pairs (instead of an intersection of matches on key and value): Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Group?characteristic-value=gender$mixed,owner$john-smith POST \[base\]/Group/\_search Content-Type: application/x-www-form-urlencoded characteristic-value=gender$mixed,owner$john-smith will match Groups that have a `gender` characteristic of "mixed" (the group contains people of multiple genders) OR the group has a `owner` characteristic of "john-smith".

Note that [search modifiers](#modifiers) are NOT allowed on composite parameters.

Composite search parameters SHALL NOT declare components that reference other composite search parameters.[§58](#fcss58) All components of a composite search parameter must be non-composite (i.e., of one of the other basic or extended types). If a desired design would logically include the behavior of an existing composite search parameter, that nesting can be flattened by listing the underlying "leaf" component search parameters directly in the new composite.

For example, instead of defining a composite parameter `code-value-quantity-and-device` whose components are the existing `code-value-quantity` composite plus a `device` component (which would constitute nesting), define a new composite parameter with components `code`, `value-quantity`, and `device` (each pointing to the appropriate non-composite search parameters).

Examples of using composite parameters:

Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post)

**Search**

**Description**

GET \[base\]/DiagnosticReport?result.code-value-quantity=http://loinc.org|2823-3$gt5.4|http://unitsofmeasure.org|mmol/L POST \[base\]/DiagnosticReport/\_search Content-Type: application/x-www-form-urlencoded result.code-value-quantity=http://loinc.org|2823-3$gt5.4|http://unitsofmeasure.org|mmol/L

Search for all diagnostic reports that contain an observation with a potassium value of >5.4 mmol/L (UCUM)

GET \[base\]/Observation?component-code-value-quantity=http://loinc.org|8480-6$lt60 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded component-code-value-quantity=http://loinc.org|8480-6$lt60

Search for all the observations with a systolic blood pressure < 60. Note that in this case, the unit is assumed (everyone uses mmHg)

GET \[base\]/Group?characteristic-value=gender$mixed POST \[base\]/Group/\_search Content-Type: application/x-www-form-urlencoded characteristic-value=gender$mixed

Search for all groups that have a characteristic "gender" with a text value of "mixed"

GET \[base\]/Questionnaire?context-type-value=focus$http://snomed.info/sct|408934002 POST \[base\]/Questionnaire/\_search Content-Type: application/x-www-form-urlencoded context-type-value=focus$http://snomed.info/sct|408934002

Search for all questionnaires that have a clinical focus = "Substance abuse prevention assessment (procedure)"

#### 3.2.1.5.18 Resource Parameters[](search.html#resource "link to here")

A `resource` type parameter is a special case parameter type that is used to enable chaining through elements that are actual resources (e.g., [Bundle.entry.resource](bundle-definitions.html#Bundle.entry.resource)).

Note that the `resource` search parameter type does not define a test behavior for the parameter itself. For example, when using the [Bundle-composition](bundle-search.html#Bundle-composition) search parameter, the behavior of a test such as `Bundle?composition=Composition/123` is _undefined_. Since search parameters of `resource` type are only defined to chain a search through to a resource, thus the correct search would be shaped like `Bundle?composition._id=123`.

Implementers will generally need to do special implementations for these parameters.

> **Implementation Note:**
> 
> In FHIR R5 and earlier versions, the `resource` search type did not exist and some search parameters of type `reference` were defined expecting to chain through embedded resources.

### 3.2.1.6 Special Search Conditions[](search.html#specialconditions "link to here")

In addition to 'basic' searching, this specification defines behaviors for many 'special' cases that are common across implementations.

#### 3.2.1.6.1 Searching ranges[](search.html#specialranges "link to here")

Explicit boundary searching on elements with the [Range](datatypes.html#Range) datatype can be performed using [composite search parameters](#composite). Composite search parameters allow a single search to test against multiple aspects of the same range.

When a composite search parameter references the same underlying search parameter that targets an element with a `Range` type multiple times, the search performs object-level joining rather than set-level joining. This means that the criteria apply to the same Range instance, not independently across multiple Range values in a repeating element.

For example, if we define a search parameter for `Observation.valueRange` named `value-range` and a composite search parameter named `value-range-values` that includes two components both referencing `value-range`, then a search such as: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?value-range-values=ge20$le30 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded value-range-values=le20$ge30 will match Observation records with a value range that has a low boundary greater than or equal to 20 AND a high boundary less than or equal to 30 on the _same Range instance_.

Note that this search will NOT match an Observation with two value ranges, one with a low of 25 and another with a high of 28, because the low and high values do not belong to the same Range instance.

#### 3.2.1.6.2 Searching Identifiers[](search.html#specialidentifiers "link to here")

In FHIR data, there are a few types of identifiers that may apply to a resource: a single [logical identifier](#identifierslogical) (the `Resource.id` of a resource), zero or more [Identifier-type](datatypes.html#Identifier) elements (e.g., [Patient.Identifier](patient-definitions.html#Patient.identifier)), and, if the resource is a [Canonical Resource](canonicalresource.html), a [canonical url](canonicalresource-definitions.html#CanonicalResource.url). Each of these 'identifiers' have a different datatype and expose different search functionality. Some details about each are included below.

##### 3.2.1.6.2.1 Logical Identifiers[](search.html#identifierslogical "link to here")

The [Logical Identifier](resource.html#id) of a resource represents a unique key for an instance within a specific context (e.g., a single resource type in a server collection, a resource in a bundle, etc.). Searching by the logical id of an instance is done via the [`_id`](#_id) search parameter.

Given that a logical id is unique in the context, searching by logical id will always result in either zero or one records. In many ways, the search is functionally equivalent to an equivalent [simple read operation](http.html#read). However, there are some differences in using search instead of read:

*   Search operations always return bundles.
    *   If the logical id exists and the resource can be returned, the result will be a bundle containing the resource (search) instead of the resource itself (read).
    *   If the logical id does not exist or cannot be returned, the result of a valid search will still be a bundle. The bundle may include further information in the form of an [OperationOutcome](operationoutcome.html). See [Handling Errors](#errors) for more information.
*   Related resources can be requested and/or returned in the same request - see [Including Other Resources](#include) for more information.
*   Additional search functionality is available. For example, asking for a Patient resource by logical id, but also setting the search criteria of `deceased` to `true` would only return the Patient if they are deceased.

Since `_id` requires a resource type context and there can only be zero or one resources of a type with a given logical id, the search is functionally equivalent to the [simple read operation](http.html#read) of `GET [base]/Patient/23`. However, searching by logical identifier means that all other search functionality is available - e.g., `OperationOutcome` resources for issues, additional search parameters, inclusion of related resources, etc..

##### 3.2.1.6.2.2 Searching by Membership[](search.html#membership "link to here")

Some resources (e.g., [Group](group.html), [List](list.html), and [CareTeam](careteam.html)) are used to maintain collections of other resources. For example, a Group of Patients representing a cohort or a List of patient allergies. While it is possible to retrieve the collection resource, iterate over the entries, and fetch each resource listed therein, using search criterion allows for fewer round-trips and additional search criteria to be specified in-line.

There are two standard search parameters defined by this specification to support searching against collection resources:

*   [`_in`](#_in) to test against active membership, and
*   [`_list`](#_list) to test against advanced `List` functionality (e.g., [functional list definitions](lifecycle.html#current)).

##### 3.2.1.6.2.3 Identifiers and References[](search.html#identifierreferences "link to here")

The [Identifier datatype](datatypes.html#Identifier) is typically only used on by a single element in a resource and is typically named `identifier`. The element is typically cardinality `0..*`, meaning that resources may have zero, one, or more identifiers attached to them (e.g., a single patient record with MRNs for several related facilities).

Resources containing identifier elements usually expose a matching search parameter, e.g., [Patient.identifier](patient.html#search) or [CanonicalResource.identifier](canonicalresource.html#search). Given the structure of the identifier type, the search parameter is a [token](#token) type - see the [token search type](#token) for details.

It is often useful to search via identifier across resource links. For example, if you have the MRN for a patient and do not know the logical identifier, it is desireable to perform searches like "encounters for the patient with MRN 1234". When elements are of the [Reference](references.html#Reference) FHIR type, there are two ways of traversing resource references by identifier, depending on how data is stored in the system: via [chaining](#chaining) or via the [`identifier`](#modifieridentifier) search modifier.

##### 3.2.1.6.2.4 Canonical Identifiers[](search.html#identifiercanonical "link to here")

Chaining functions by resolving a resource reference and then searching inside it. In this case, requesting: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Encounter?patient.identifier=http://example.org/facilityA|1234 POST \[base\]/Encounter/\_search Content-Type: application/x-www-form-urlencoded patient.identifier=http://example.org/facilityA|1234 will search Encounter resources, following references in the `patient` search parameter ("the patient present at the encounter"), and testing those Patient resources against the `Patient.identifier` value of `1234` in system `http://example.org/facilityA`. In short, this search will return all Encounters for Patient records matching "1234" in "facilityA".

This search is flexible, but does require resolving (searching against) Patient resources, even though the request is only for Encounters. If a system wants to enable searches via identifiers _without_ chaining, the server can include additional information in the [Reference Datatype](references.html#Reference) (in [Reference.identifier](references-definitions.html#Reference.identifier)), which is then accessible by the [`identifier`](#modifieridentifier) search modifier. Note that this approach requires additional bookkeeping by the server - if a patient record is modified, e.g., to add an additional identifier, **every** resource that refers to that patient record would need to be updated.

The `identifier` modifier functions by testing the `Reference.identifier` element within resources. In this case, requesting: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Encounter?patient:identifier=http://example.org/facilityA|1234 POST \[base\]/Encounter/\_search Content-Type: application/x-www-form-urlencoded patient:identifier=http://example.org/facilityA|1234 will search Encounter resources, testing the `Encounter.subject.identifier` element against the value of `1234` in system `http://example.org/facilityA`. In short, this search will return all Encounters for that have Patient references and include the identifier "1234" from "facilityA".

Note that the difference between the two searches is a single character - the `.` (period) in the first example represents a chained search and the `:` (colon) in the second indicates the use of a search modifier.

[Canonical Resources](canonicalresource.html) are identified by their [url](canonicalresource-definitions.html#CanonicalResource.url) and possibly a version. Similarly, references to canonical resources are made with the [canonical](datatypes.html#canonical) datatype. More details about canonical references can be found in the [Canonical URLs](references.html#canonical) section of the references page.

When searching canonical references, the search type is [reference](#reference), though with an additional syntax for version information. For more information about version resolution, see the [References and Versions](#versions) section of this page.

#### 3.2.1.6.3 References and Versions[](search.html#versions "link to here")

Elements of the FHIR type [Reference](references.html#Reference) may contain a [versioned](references.html#versions) reference: Show as: [FHIR+JSON](#json) | [FHIR+XML](#xml) { "resourceType": "Condition", ... "evidence": { "detail": { "reference": "Observation/123/\_history/234234" } }, ... } <Condition> ... <evidence> <detail> <reference value="Observation/123/\_history/234234"/> </detail> </evidence> ... </Condition>

When searching on versioned References, the following rules apply:

*   If a resource has a reference that is versioned and chaining is performed, the criteria will ideally be evaluated against the version referenced, but most systems will not be capable of this because search is only defined to function against the current version of a resource
*   Where a search does not act on the referenced version, search results SHOULD contain a [OperationOutcome](operationoutcome.html) with a warning that indicates the discrepancy.[§59](#fcss59)
*   If a resource has a reference that is versioned and the resource is included (e.g., `_include`, `_revinclude`), the specified version SHOULD be provided.[§60](#fcss60)
*   If a reference-type search parameter does not contain a version (i.e., no `/_history/[version]` in the URL), the search will match against resources containing both versioned and un-versioned references.

Elements of type [canonical](references.html#canonical) may contain a [version specific](references.html#versions) reference, but this version is different in both meaning and format to version specific references that might be found in a [Reference](references.html#Reference): Show as: [FHIR+JSON](#json) | [FHIR+XML](#xml) { "resourceType": "QuestionnaireResponse", ... "questionnaire": "http://example.org/fhir/questionnaire/patient-intake|3.0", ... } <QuestionnaireResponse> ... <questionnaire value="http://example.org/fhir/questionnaire/patient-intake|3.0"/> ... </QuestionnaireResponse>

This version is a canonical reference to the business version of the resource.

For canonical references, servers SHOULD support searching by Canonical URLs, and SHOULD support automatically detecting a `|[version]` portion as part of the search parameter and interpreting that portion as a search on the business version of the target resource.[§61](#fcss61) The modifier `:below` is used with canonical references, to control whether the version is considered in the search. The search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/QuestionnaireResponse?questionnaire:below=http://example.org/fhir/questionnaire/patient-intake POST \[base\]/QuestionnaireResponse/\_search Content-Type: application/x-www-form-urlencoded questionnaire:below=http://example.org/fhir/questionnaire/patient-intake would match QuestionnaireResponses based on Questionnaires:

*   http://example.org/fhir/questionnaire/patient-intake|1.0
*   http://example.org/fhir/questionnaire/patient-intake|1.1
*   http://example.org/fhir/questionnaire/patient-intake|2.0
*   etc.

The search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/QuestionnaireResponse?questionnaire:below=http://example.org/fhir/questionnaire/patient-intake|1 POST \[base\]/QuestionnaireResponse/\_search Content-Type: application/x-www-form-urlencoded questionnaire:below=http://example.org/fhir/questionnaire/patient-intake|1 would match QuestionnaireResponses based on Questionnaires with a major version of '1', e.g.:

*   http://example.org/fhir/questionnaire/patient-intake|1.0
*   http://example.org/fhir/questionnaire/patient-intake|1.1

When a canonical URL with an appended version (i.e. `[canonical]|[version]`) is supplied in a search parameter, the version portion is matched by the `versionAlgorithm` of the resource if known (explicit or derived). If the server does not know the algorithm of the versioning scheme, the version matching falls back to an exact-match, equivalent to [token](#token) search semantics. If additional version matching behavior is required, the `:above` and `:below` modifiers can be used.

For further information about searching canonical references, see [Choosing the right Canonical Reference](references.html#canonical-matching).

#### 3.2.1.6.4 Searching Hierarchies[](search.html#recursive "link to here")

Some resource references are [circular](references.html#circular) - that is, the Reference points to another resource of the same type. When the reference establishes a strict hierarchy, the modifiers `[above](#modifierabove)` and `[below](#modifierbelow)` can be used to search transitively through the hierarchy: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Procedure?location:below=42 POST \[base\]/Procedure/\_search Content-Type: application/x-www-form-urlencoded location:below=42

This search returns not only all procedures that occurred at location with id 42, but also any procedures that occurred in locations that are part of location with id 42.

Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/MedicationAdministration?encounter:above=21 POST \[base\]/MedicationAdministration/\_search Content-Type: application/x-www-form-urlencoded encounter:above=21

Returns all medication administrations that happened during encounter with id 21 or during any "parent" encounter of that encounter.

Resources that establish hierarchical references are listed in the [Circular Resource References](references.html#circular) section of the References page.

Servers indicate that `[above](#modifierabove)`/`[below](#modifierbelow)` is supported on a search parameter by defining them as [Modifiers](searchparameter-definitions.html#SearchParameter.modifier) on the Search Parameter definition.

#### 3.2.1.6.5 Searching MIME Types[](search.html#mimetype "link to here")

The `:below` modifier is also very useful with searching [MIME type ![icon](external.png)](https://datatracker.ietf.org/doc/html/rfc6838) , such as the search parameter [DocumentReference.contenttype](documentreference.html#search), which refers to [Attachment.contentType](datatypes.html#Attachment). A simple search such as: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/DocumentReference?contenttype=text/xml POST \[base\]/DocumentReference/\_search Content-Type: application/x-www-form-urlencoded contenttype=text/xml will miss documents with a MIME type such as `text/xml; charset=UTF-8`. This search will find all text/xml documents: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/DocumentReference?contenttype:below=text/xml POST \[base\]/DocumentReference/\_search Content-Type: application/x-www-form-urlencoded contenttype:below=text/xml

For ease of processing on the server, servers are only required to support `:below` on the base part of the MIME type; servers are not required to sort between different parameters and do formal subsumption logic.

Additionally, the `below` modifier can be applied to the first segment only: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/DocumentReference?contenttype:below=image POST \[base\]/DocumentReference/\_search Content-Type: application/x-www-form-urlencoded contenttype:below=image will match all `image/` content types, e.g., "image/png", "image/jpeg", etc.

#### 3.2.1.6.6 Chaining (chained parameters)[](search.html#chaining "link to here")

In order to save a client from performing a series of search operations, some reference parameters may be "chained" by appending them with a period (`.`) followed by the name of a search parameter defined for the target resource. This can be done recursively, following a logical path through a graph of related resources, separated by `.`. Note that chaining is only defined for [reference](#reference) type search parameters that target [Reference](references.html#Reference) FHIR type elements and [resource](#resource) type search parameters. Server implementers have discretion regarding additional requirements in content (e.g., whether external references are allowed) and behavior in extended scenarios (e.g., if canonical resolution is performed, etc.).

When chaining through references, it is possible that a resolved resource does not match the expected type. In this case, the reference is considered to not match the search parameter. If a reference resolves to a type different than allowed via the reference, a server SHOULD consider if it is appropriate to include the issue via an `OperationOutcome` in the response.[§62](#fcss62)

For instance, given that the resource [DiagnosticReport](diagnosticreport.html) has a search parameter named _subject_, which is usually a reference to a [Patient](patient.html) resource, and the Patient resource includes a parameter _name_ which searches on patient name, then the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/DiagnosticReport?subject.name=peter POST \[base\]/DiagnosticReport/\_search Content-Type: application/x-www-form-urlencoded subject.name=peter is a request to return all the lab reports that have a subject whose name includes "peter". Because the Diagnostic Report subject can be one of a set of different resources, it's necessary to limit the search to a particular type: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/DiagnosticReport?subject:Patient.name=peter POST \[base\]/DiagnosticReport/\_search Content-Type: application/x-www-form-urlencoded subject:Patient.name=peter This request returns all the lab reports that have a subject which is a patient, whose name includes "peter".

Note that chained parameters are applied independently to the target resource. For example, Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?general-practitioner.name=Joe&general-practitioner.address-state=MN POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded general-practitioner.name=Joe&general-practitioner.address-state=MN is asking for Patients that have _any_ general practitioner with 'Joe' in their name and that have _any_ general practitioner that resides in Minnesota. Each chained parameter is evaluated individually, so a Patient that sees 'Dr. Joe' in California and 'Dr. Jane' in Minnesota matches. Following the logic, an implementation will:

*   find the set of Patients that have a `generalPractitioner` with a name matching "Joe"
*   find the set of Patients that have a `generalPractitioner` with an address matching "MN"
*   join the two sets of results with **and** logic at the Patient level.

E.g., the chains are evaluated separately. For use cases where the joins must be evaluated in groups, implementers can use either [Composite](#composite) search parameters or the [\_filter](#filter) parameter.

Note that chained search parameters are not intended to work with [search modifiers](#modifiers). While their use is not prohibited, searches requiring advanced matching across resources are encouraged to use [Named Queries](#advanced) or [Advanced filtering](#_filter).

Advanced Search Note: Where a chained parameter searches a resource reference that may have more than one type of resource as its target, the parameter chain may end up referring to search parameters with the same name on more than one kind of resource at once. Servers SHOULD fail a search where the logical id of a reference refers to more than one matching resource across different types.[§63](#fcss63) In those scenarios, the client has to specify the type explicitly using the syntax in the second example above.

#### 3.2.1.6.7 Reverse Chaining[](search.html#has "link to here")

The \_has parameter provides limited support for reverse chaining - that is, selecting resources based on the properties of resources that refer to them (instead of chaining, above, where resources can be selected based on the properties of resources that they refer to). Here is an example of the \_has parameter: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?\_has:Observation:patient:code=1234-5 POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded \_has:Observation:patient:code=1234-5

This requests the server to return Patient resources, where the patient resource is referred to by at least one Observation where the observation has a code of 1234, and where the Observation refers to the patient resource in the patient search parameter.

"Or" searches are allowed (e.g., `_has:Observation:patient:code=123,456`), and multiple `_has` parameters are allowed (e.g., `_has:Observation:patient:code=123&_has:Observation:patient:code=456`). Note that each `_has` parameter is processed independently of other `_has` parameters.

The `_has` parameter can be nested, like this: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?\_has:Observation:patient:\_has:AuditEvent:entity:agent=MyUserId POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded \_has:Observation:patient:\_has:AuditEvent:entity:agent=MyUserId This search will fetch all Patient records that have an Observation where the observation has an audit event from a specific user.

The `_has` parameter can also be used in chained-searches, for example: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Encounter?patient.\_has:Group:member:\_id=102 POST \[base\]/Encounter/\_search Content-Type: application/x-www-form-urlencoded patient.\_has:Group:member:\_id=102 to search for the Encounters of patients that are members of the Group "102".

Note that reverse-chained search parameters are not intended to work with [search modifiers](#modifiers). While their use is not prohibited, the syntax of `_has` makes construction and parsing error-prone. Searches requiring advanced matching across resources are encouraged to use [Named Queries](#advanced) or [Advanced filtering](#_filter).

#### 3.2.1.6.8 Handling Missing Data[](search.html#missing "link to here")

Consider the case of searching for all AllergyIntolerance resources: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/AllergyIntolerance?clinical-status=http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical|active POST \[base\]/AllergyIntolerance/\_search Content-Type: application/x-www-form-urlencoded clinical-status=http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical|active

This search will only return resources that have a value for clinicalStatus: Show as: [FHIR+JSON](#json) | [FHIR+XML](#xml) { "resourceType" : "AllergyIntolerance", "clinicalStatus": { "coding": \[ { "system": "http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical", "code": "active" } \] } } <AllergyIntolerance> <clinicalStatus> <coding> <system value="http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical"/> <code value="active"/> </coding> </clinicalStatus> </AllergyIntolerance>

Resources missing a clinicalStatus will not be returned. This is probably unsafe - it would not usually be appropriate to ignore AllergyIntolerance warnings with an unknown clinical status, and only return resources with an explicit clinicalStatus. Instead, it might be desired to return AllergyIntolerance resources with either an explicit value for clinicalStatus, or none: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/AllergyIntolerance?clinical-status=http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical|active POST \[base\]/AllergyIntolerance/\_search Content-Type: application/x-www-form-urlencoded clinical-status=http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical|active GET \[base\]/AllergyIntolerance?clinical-status:missing=true POST \[base\]/AllergyIntolerance/\_search Content-Type: application/x-www-form-urlencoded clinical-status:missing=true

Note that this is 2 separate queries. They can be [combined in a batch](http.html#transaction), but not in a single operation. This query will always return an empty list, as no resource can satisfy both criteria at once: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/AllergyIntolerance?clinical-status=http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical|active&clinical-status:missing=true POST \[base\]/AllergyIntolerance/\_search Content-Type: application/x-www-form-urlencoded clinical-status=http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical|active&clinical-status:missing=true

There is no way to use the :missing modifier and mix with a value using the comma syntax documented above for composite search parameters.

An alternative approach is to use the [`_filter`](#filter) parameter, for servers that support this parameter.

#### 3.2.1.6.9 Advanced filtering[](search.html#_filter "link to here")

The search mechanism described above is flexible, and easy to implement for simple cases, but is limited in its ability to express combination queries. To complement this mechanism, the `_filter` search expression parameter can be used.

For example, "Find all the observations for patient with a name including `peter` that have a LOINC code `1234-5`": Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?code=http://loinc.org|1234-5&patient.name=peter POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded code=http://loinc.org|1234-5&patient.name=peter

Using the `_filter` parameter, the search would be expressed like this (note that the value is unescaped for readability): Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?\_filter=code eq http://loinc.org|1234-5 and patient.name co "peter" POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded \_filter=code eq http://loinc.org|1234-5 and patient.name co "peter"

The `_filter` parameter is described in detail on the ["\_Filter Parameter" page](search_filter.html).

#### 3.2.1.6.10 Searching Multiple Resource Types[](search.html#type "link to here")

Normally, a search is initiated against a known type of resource, e.g.: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?params... POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded params... However, in some circumstances, a search is executed where there is no fixed type of resource, for example:

*   Using search across all resource types (`[base]?params...`)
*   Using search with [messaging](messaging.html)
*   Some search specifications inside other services e.g., decision support usage

In these circumstances, the search criteria may need to specify one or more resource types that the search applies to. This can be done by using the standard search parameter [`_type`](#_type).

If no type is specified, the only search parameters that can be used in global search like this are the base parameters that apply to [all resources](#all).

If a single type is specified, all search parameters for that type are allowed.

If multiple types are specified, all search parameters that can be referenced commonly between resources can be used (e.g., listed with the same `name` in the CapabilityStatement, typically the value of [SearchParameter.code](searchparameter-definitions.html#SearchParameter.code) or one of its [SearchParameter.alias](searchparameter-definitions.html#SearchParameter.alias) values). For example, both [Patient](patient.html#search) and [Person](person.html#search) define a search parameter referenced by `name`. Assuming a server implements both parameters and uses `name` to do so, the `name` parameter can be used to search across both Patient and Person resources. Note that this is allowed even though the search parameters do not share a common definition. If a request references multiple parameters that are not the same type (e.g., if one parameter is a `number` type and another is a `token` type, despite sharing the same name), this is an error and servers SHOULD return a `400` status.[§64](#fcss64)

#### 3.2.1.6.11 Implicit Resources[](search.html#implicitresources "link to here")

Some resources are defined _implicitly_ rather than as explicit instances (e.g., [Implicit Code Systems](codesystem.html#detailed-metadata)). When searching resources where implicit instances might exist, it is up to the server whether they will include implicit resources as part of the search set. Implementation Guides MAY set specific expectations about search behavior around implicit resources.[§65](#fcss65)

#### 3.2.1.6.12 Named Queries[](search.html#advanced "link to here")

The search framework described above is a useful framework for providing a simple search based on indexed criteria, but more sophisticated query capability is needed to handle precise queries, complex decision support-based requests, and direct queries that have human resolution. To support more advanced functionality, this specification defines a method for creating and using _named queries_.

The [`_query`](#_query) parameter names a custom search profile that describes a specific query operation. A named query may define additional named parameters that are used with that particular named query. Servers can define their own additional named queries to meet their own uses using an [OperationDefinition](operationdefinition.html).

Query operations are only allowed in the `base` and `type` [search contexts](#searchcontexts). Named queries cannot be defined at the `instance` level. Named-query operations SHOULD NOT be invoked via any method other than search with the `_query` parameter (e.g., servers SHOULD NOT execute query operations via `[base]/$[operation code]` or `[base]/[type]/$[operation code]`, etc.).[§66](#fcss66)

For named queries, all the [standard search parameters](#standard) (parameters for [all resources](#all) and [search result](#modifyingresults) parameters) are potentially available for use in addition to any parameters defined by the query operation. In addition, if the query operation is defined in the scope of a specific resource, the search parameters for that resource are also potentially available. For example, a named query with a scope of `Patient` could include the [`_lastUpdated`](#_lastUpdated), [`_sort`](#_sort), and/or [Patient.gender](patient.html#search) search parameters, even if those aren't explicitly listed as parameters on the query operation.

Servers do not need to support ANY such 'inherited' parameters unless explicitly documented in the operation's description. Clients SHOULD NOT assume the availability of any search parameter that is not supported for general queries (i.e., listed in the server's [CapabilityStatement](capabilitystatement.html), either in [CapabilityStatement.rest.searchParam](capabilitystatement-definitions.html#CapabilityStatement.rest.searchParam) or [CapabilityStatement.rest.resource.searchParam](capabilitystatement-definitions.html#CapabilityStatement.rest.resource.searchParam)).[§67](#fcss67) Servers MAY further document the search parameters they support for a specific `query` operation - either in text or by defining a constrained operation based on the original that explicitly enumerates all supported parameters.[§68](#fcss68)

In order to ensure consistent behavior, authors SHOULD include relevant search parameters in the named query definition (`OperationDefinition`).[§69](#fcss69)

For example, the operation definition: Show as: [FHIR+JSON](#json) | [FHIR+XML](#xml) { "resourceType": "OperationDefinition", "id": "current-high-risk", "url": "http://example.org/OperationDefinition/current-high-risk", "version": "0.0.1", "name": "Current High-Risk Patients", "status": "draft", "kind": "query", "description": "Filter Patients based on risk assessments", "code": "current-high-risk", "resource": \[ "Patient" \], "system": false, "type": true, "instance": false, "parameter": \[ { "name": "ward", "use": "in", "min": 0, "max": "\*", "documentation": "Ward filters to apply to patient locations", "type": "string", "searchType": "reference" }, { "name": "return", "use": "out", "min": 1, "max": "1", "documentation": "Searchset bundle", "type": "Bundle" } \] } <OperationDefinition xmlns="http://hl7.org/fhir"> <id value="current-high-risk" /> <url value="http://example.org/OperationDefinition/current-high-risk" /> <version value="0.0.1" /> <name value="Current High-Risk Patients" /> <status value="draft" /> <kind value="query" /> <description value="Filter Patients based on risk assessments" /> <code value="current-high-risk" /> <resource value="Patient" /> <system value="false" /> <type value="true" /> <instance value="false" /> <parameter> <name value="ward" /> <use value="in" /> <min value="0" /> <max value="\*" /> <documentation value="Ward filters to apply to patient locations" /> <type value="string" /> <searchType value="reference" /> </parameter> <parameter> <name value="return" /> <use value="out" /> <min value="1" /> <max value="1" /> <documentation value="Searchset bundle" /> <type value="Bundle" /> </parameter> </OperationDefinition> can be executed via: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post)

**Request**

**Description**

GET \[base\]/Patient?\_query=current-high-risk POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded \_query=current-high-risk

Query with no additional parameters

GET \[base\]/Patient?\_query=current-high-risk&ward=Location/A1 POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded \_query=current-high-risk&ward=Location/A1

Query with a `ward` parameter value passed to the filter

GET \[base\]/Patient?\_query=current-high-risk&birthdate=ge1970-01-1 POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded \_query=current-high-risk&birthdate=ge1970-01-01

Query with a no additional parameters, filter results according to a standard patient birthdate query.  
Note that servers SHOULD accept query parameters that are not enumerated in named queries based on their standard support (e.g., parameters that are supported for a given resource type), but servers MAY choose to ignore parameters not specified by the OperationDefinition.[§70](#fcss70)

GET \[base\]/Patient?\_query=current-high-risk&birthdate=ge1970-01-01&ward=Location/A1 POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded \_query=current-high-risk&birthdate=ge1970-01-01&ward=Location/A1

Query with both a `ward` parameter value passed to the named query and a standard patient query filter by birthdate.  
Note that query parameters are unordered, so parameters to a named query MAY appear in any location or sequence.[§71](#fcss71)

#### 3.2.1.6.13 Searching for Additional Resources[](search.html#additional "link to here")

FHIR allows specifications to define ["additional" resources](resource.html#additional) beyond those in the core set. These are defined outside the base specification but use the same infrastructure and search framework. There is no special behavior or different syntax required when searching for these resources: servers and clients use the same search parameter names and processing rules as for "standard" (core) resources.

The specification (or Implementation Guide) that defines an additional resource **SHOULD** define a set of standard search parameters for that resource[§72](#fcss72) so that a minimal level of interoperability is possible. Other specifications are free to define further search parameters (subject to the usual namespacing and conformance declarations). Servers MAY also define custom parameters as with any other resource type.

Clients might encounter previously unknown additional resource types in search results. This can happen if the server returns them via unrequested `_include` / `_revinclude` processing or if they appear as [contained](#contained) resources within search bundle entries. Clients SHOULD be robust in the presence of such resources (e.g., safely ignore, render generically, or retrieve definitions as appropriate) and avoid failing the entire response simply due to an unfamiliar additional resource type.

No extra conformance statement elements or flags are needed to enable searching additional resources beyond those already described for ordinary resources; capability exposure and parameter support are declared in the same way (e.g., via `CapabilityStatement.rest.resource` and associated `searchParam` elements).

### 3.2.1.7 Modifying Search Results[](search.html#modifyingresults "link to here")

Along with search parameters that are used as input to search filters, there are also Search Result Parameters defined by this specification to modify the results of a performed search. These parameters control aspects of the results such as sort order, including related resources, etc., and are defined in the following sections. Note that since these are not search parameters, they do not have definitions as [SearchParameter](searchparameter.html) resources and do not appear in the [search parameter registry](searchparameter-registry.html#common).

Note that with the exception of `_include` and `_revinclude`, search result parameters SHOULD only appear once in a search. If such a parameter appears more than once, the behavior is undefined and a server MAY treat the situation as an error.[§73](#fcss73)

The search result parameters defined by this specification are:

`[_contained](#_contained)`

Request different types of handling for contained resources.

`[_count](#_count)`

Limit the number of `match` results per page of response.

`[_elements](#_elements)`

Request that only a specific set of elements be returned for resources.

`[_graph](#_graph)`

Include related resources according to a `GraphDefinition`.

`[_include](#_include)`

Include related resources, based on following links forward across references.

`[_maxresults](#_maxresults)`

Limit the total number of `match` results a request can return.

`[_revinclude](#_revinclude)`

Include related resources, based on following reverse links across references.

`[_score](#_score)`

Request match relevance in results.

`[_sort](#_sort)`

Request which order results are returned in.

`[_summary](#_summary)`

Return only portions of resources, based on pre-defined levels.

`[_total](#_total)`

Request a precision of the total number of results for a request.

#### 3.2.1.7.1 Sorting (\_sort)[](search.html#_sort "link to here")

The client can indicate which order to return the results by using the parameter `_sort`, which can contain a comma-separated list of sort rules in priority order: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?\_sort=status,-date,category POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded \_sort=status,-date,category

Each item in the comma-separated list is a search parameter, optionally with a '-' prefix. The prefix indicates decreasing order; in its absence, the parameter is applied in increasing order.

Notes:

*   When sorting, the actual sort value used is not returned explicitly by the server for each resource, just the resource contents.
*   To sort by relevance, use `_score`.
*   The server returns the sort it performs as part of the returned search parameters (see [below](#conformance)).
*   Servers have discretion on the implementation of sorting for both repeated elements and complex elements. For example, if requesting a sort on `Patient.name`, servers might search by family name then given, given name then family, or prefix, family, and then given. Similarly, when sorting with multiple given names, the sort might be based on the 'earliest' name in sort order or the first name in the instance.
*   When sorting on string search parameters, sorting SHOULD be performed on a case-insensitive basis.[§74](#fcss74) Accents may either be ignored or sorted as per realm convention.
*   This specification does not specify exact rules for consistency of sorting across servers. In general, this is deemed to be not as essential as consistency of filtering (though even that is a little variable). The purpose of sorting is to provide data in a "reasonable" order for end-users. "Reasonable" may vary by use case and realm, particularly for accented characters.
*   When sorting on search parameters of `reference` and `token` types, the search parameter can include the [text modifier](#modifiertexttoken) to request sorting based on the text or display portions of the element values. For example, `_sort=code:text` or `_sort:-code:text`. Servers MAY or MAY NOT support the `text` sort modifier behavior, and MAY fall back to token-based sorting or disregard the value.[§75](#fcss75)
*   When sorting on search parameters of `date` type, if the source data is not a single instance (e.g., a [Period](datatypes.html#Period)) the sort behavior is server-defined. If more prescribed search behavior is needed, custom search parameters that assert specific behavior can be defined. Note that most reference implementations search by the `start` of the period.

#### 3.2.1.7.2 Total number of matching resources (\_total)[](search.html#_total "link to here")

The return [Bundle](bundle.html) has an element `total` which is the number of resources that match the search parameters.

Note that `Bundle.total` represents the total number of matches, not how many resources are returned in a particular response (see [paging](#count)).

Providing a precise number of matching resources may be onerous for the server, depending on how the server is designed. To help reduce the server load, a client can provide the parameter `_total` to indicate its preference with regard to the total, which can have one of the following values:

none

There is no need to populate the total count; the client will not use it

estimate

A rough estimate of the number of matching resources is sufficient

accurate

The client requests that the server provide an exact total of the number of matching resources

The `Bundle.total` element is still optional, and the servers can ignore the `_total` parameter: it is just an optimization hint, that might possibly save the server some work.

#### 3.2.1.7.3 Limiting Page Size (\_count)[](search.html#_count "link to here")

In order to keep the load on clients, servers, and the network minimized, the server may choose to return the results in a series of pages. The search result set contains the URLs that the client uses to request additional pages from the search set. For a simple RESTful search, the page links are [contained in the returned bundle as links](http.html#paging).

Typically, a server will provide its own parameters in the links that it uses to manage the state of the search as pages are retrieved. These parameters do not need to be understood or processed by the client.

The parameter `_count` is defined as an instruction to the server regarding the maximum number of resources that can be returned in a single page. Servers SHALL NOT return more resources in a single page than requested, even if they don't support paging, but MAY return less than the client requested.[§76](#fcss76) NOTE: This means that all servers that support search or history SHALL support checking the `_count` parameter.[§77](#fcss77) The server SHOULD ensure that any pages reachable via links (e.g., previous/next) respect the the original requested `_count` expectations (or a server-overridden max page size).[§78](#fcss78) Note: It is at the discretion of the search engine as to how to handle ongoing updates to the resources while the search is proceeding.

The combination of `_sort` and `_count` can be used to return only the latest resource that meets a particular criteria - set the criteria, and then sort by date in descending order, with `_count=1`. This way, the last matching resource will be returned.

If `_count` has the value 0 (zero), this SHALL be treated the same as `_summary=count`: the server returns a bundle that reports the total number of resources that match in `Bundle.total`, but with no entries, and no previous/next/last links.[§79](#fcss79) Note that the `Bundle.total` only includes the total number of matching resources. It does not count extra resources such as [OperationOutcome](operationoutcome.html) or [included](#include) resources that may also be returned. In the same way, the `_count` parameter only applies to resources with `entry.search.mode = match`, and does not include included resources or operation outcomes.

The `_count` parameter has no impact on the value of `Bundle.total` as the latter represents the total number of matches, not how many are returned in a single Bundle response.

#### 3.2.1.7.4 Limiting Total Result Size (\_maxresults)[](search.html#_maxresults "link to here")

In some scenarios, a client knows that it will only process a certain number of results for a search, regardless of how many records there are. For example, if an application is providing an overview of patient activity and will only display the most recent observation on a summary screen, telling the server that additional records will never be retrieved allows the server to free any resources that might have been reserved for paging.

The parameter `_maxresults` is defined as a hint to the server regarding the limit on how many total resources to returned from a query. If supported, a server SHOULD NOT return more resources than requested.[§80](#fcss80) As with other parameters, servers supporting \_maxresults SHALL include this parameter in the [Self Link](search.html#selflink) when returning results if it is used.[§81](#fcss81)

The `_maxresults` parameter has no impact on the value of `Bundle.total` as the latter represents the total number of matches, not how many are returned in a response.

The `_maxresults` parameter only affects the number of resources returned as matches (`entry.search.mode=match`). It does not affect the number of resources returned as included resources (`entry.search.mode=include`) or operation outcomes (`entry.search.mode=outcome`).

The combination of `_sort` and `_maxresults` can be used to return only the latest resource that meets a particular criteria - set the criteria, and then sort by date in descending order, with `_maxresults=1`. This way, the last matching resource will be returned.

Note that `_maxresults` does not absolve a server from paging requirements. For example, a client can request `_maxresults=10&_count=5` to receive (at most) two pages of five results each.

#### 3.2.1.7.5 Summary (\_summary)[](search.html#_summary "link to here")

The client can request the server to return only a portion of the resources by using the parameter `_summary`: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/ValueSet?\_summary=true POST \[base\]/ValueSet/\_search Content-Type: application/x-www-form-urlencoded \_summary=true

The `_summary` parameter requests the server to return a subset of the resource. It can contain one of the following values:

[true](#summary-true)

Return a limited subset of elements from the resource. This subset SHOULD consist solely of all supported elements that are marked as "summary" in the base definition of the resource(s) (see [ElementDefinition.isSummary](elementdefinition-definitions.html#ElementDefinition.isSummary)).[§82](#fcss82)

[text](#summary-text)

Return only the `text`, `id`, `meta`, and top-level mandatory elements (these mandatory elements are included to ensure that the payload is valid FHIR; servers MAY omit elements within these sub-trees as long as they ensure that the payload is valid).[§83](#fcss83) Servers MAY return extensions in summary mode, but clients SHOULD NOT rely on extensions being present and SHOULD use another search mode if data contained in extensions is required.[§84](#fcss84)

[data](#summary-data)

Remove the `text` element

[count](#summary-count)

Search only: just return a count of the matching resources, without returning the actual matches

[false](#summary-false)

Return all parts of the resource(s)

The intent of the `_summary` parameter is to reduce the total processing load on server, client, and resources between them such as the network. It is most useful for resources that are large, particularly ones that include images or elements that may repeat many times. The purpose of the summary form is to allow a client to quickly retrieve a large set of resources, and let a user pick the appropriate one. The summary for an element is defined to allow a user to quickly sort and filter the resources, and typically omit important content on the basis that the entire resource will be retrieved when the user selects a resource.

Servers are not obliged to return just a summary as requested. There are only a limited number of summary forms defined for resources in order to allow servers to store the summarized form(s) in advance. Servers SHOULD mark the by populating `meta.tag` resources with the code [`SUBSETTED` ![icon](external.png)](https://terminology.hl7.org/CodeSystem-v3-ObservationValue.html#v3-ObservationValue-SUBSETTED) to ensure that the incomplete resource is not accidentally used to overwrite a complete resource.[§85](#fcss85)

Note that the [self link](#selflink) in the search result Bundle is important for the interpretation of the result and SHALL always be returned, regardless of the type of `_summary`.[§86](#fcss86)

Note that the `_include` and `_revinclude` parameters cannot be mixed with `_summary=text`.

> **Implementation Note:** There is some question about the inclusion of extensions in the summary. Additional rules may be made around this in the future.

#### 3.2.1.7.6 Elements (\_elements)[](search.html#_elements "link to here")

If one of the summary views defined above is not appropriate, a client can request a specific set of elements be returned as part of a resource in the search results using the `_elements` parameter: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?\_elements=identifier,active,link POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded \_elements=identifier,active,link

The `_elements` parameter consists of a comma-separated list of base element names such as, elements defined at the root level in the resource. Each element name SHALL be the base element name without specifying `[x]` or one of the types (e.g., "value" is valid, while "value\[x\]" and "valueQuantity" are not).[§87](#fcss87) Only elements that are listed are to be returned. Clients SHOULD list all mandatory and modifier elements in a resource as part of the list of elements.[§88](#fcss88) Servers have the right to return more data than request, however clients can generally expect that returned elements will be:

*   elements explicity requested,
*   mandatory elements, or
*   modifier elements that have values.

This parameter is allowed to be used for interactions other than 'search'. See the [RESTful API](http.html#parameters) page.

Servers are not obliged to return just the requested elements. Servers SHOULD return mandatory and modifier elements whether they are requested or not.[§89](#fcss89) Servers SHOULD mark the resources with the tag [`SUBSETTED` ![icon](external.png)](https://terminology.hl7.org/CodeSystem-v3-ObservationValue.html#v3-ObservationValue-SUBSETTED) to ensure that the incomplete resource is not actually used to overwrite a complete resource.[§90](#fcss90)

If a client supplies `_elements` values prefixed with resource types, such as `_elements=Patient.gender`, then servers SHOULD apply these element-level restrictions to all resources in the returned bundle, rather than just to direct search matches (i.e., restrictions should be applied to bundle entries with any search.mode).[§91](#fcss91)

#### 3.2.1.7.7 Relevance (\_score)[](search.html#_score "link to here")

Where a search specifies a non-deterministic sort, the search algorithm may generate a ranking score to indicate which resources meet the specified criteria better than others. The server can return this score in [entry.score](bundle-definitions.html#Bundle.entry.score): Show as: [FHIR+JSON](#json) | [FHIR+XML](#xml) "entry": { "score": 0.45, "resource": { "resourceType": "Patient", ... patient data ... } } <entry> <score value=".45"/> <resource> <Patient> ... patient data ... </Patient> </resource> </entry>

The score is a decimal number with a value between (and including) 0 and 1, where 1 is best match, and 0 is least match.

#### 3.2.1.7.8 Including Referenced Resources[](search.html#include "link to here")

Along with resources matching search criteria, clients may request that related resources are included with results (i.e., to reduce the overall network delay of repeated retrievals). For example, when searching for clinical resources (e.g., [Condition](condition.html), [Observation](observation.html), etc.), the client will also generally need the subject ([Patient](patient.html)) resource that the clinical resource refers to. This section describes several mechanisms for requesting additional data within FHIR search.

Note that implementers can also consider whether using [GraphQL](graphql.html) is an appropriate approach. GraphQL is outside the scope of FHIR Search and thus not described here.

##### 3.2.1.7.8.1 Inline Requests (`_include` and `_revinclude`)[](search.html#_revinclude "link to here")

The most common method for requesting related resources is inline, via the search result parameters `_include` and `_revinclude`. The parameters are similar to [chaining](#chaining) search parameters, in that they follow links between resources based on search parameters.

The parameter `_include` is used to follow links 'forward'. For example, to include relevant `Patient` resources for requested `Encounter` matches, based on the `Encounter.subject` element, using either the `subject` or `patient` search parameters.

The parameter `_revinclude` is used to follow reverse links. For example, to include relevant `Observation` resources for requested `Patient` matches, based on the `Observation.subject` element, using either the `subject` or `patient` search parameters.

Both `_include` and `_revinclude` share the same syntax - up to three components, separated by colon characters (`:`). The details of the syntax are:

*   **`[_include|_revinclude]=*`**: the wildcard literal `*` (asterisk) is requesting all resources and references supported by the server (will typically only be supported references that are indexed).
*   **`[_include|_revinclude]=[resource]:*`**: where `[resource]` is the name of the source resource from which the join comes and the wildcard literal `*` (asterisk) is requesting all supported [reference](#reference) and [resource](#resource) type search parameters for that resource. Note that the list of search parameters supported for `_include` or `_revinclude` is not the same as the list of supported search parameters.
*   **`[_include|_revinclude]=[resource]:[parameter]`**: where `[resource]` is the name of the source resource from which the join comes and `[parameter]` is the name of a search parameter which must be of type `reference`.
*   **`[_include|_revinclude]=[resource]:[parameter]:[targetType]`**: where `[resource]` is the name of the source resource from which the join comes, `[parameter]` is the name of a search parameter which must be of type `reference` or `resource`, and `[targetType]` is a specific resource type (for when a search parameter refers to references of multiple types).

Note that implementers intending to support wildcard (`*`) segments SHOULD advertise them along with other resources and search parameters in their capability statement (e.g., in `CapabilityStatement.rest.resource.searchInclude`).[§92](#fcss92)

`_include` and `_revinclude` parameters do not include multiple values. Instead, the parameters are repeated for each different include criteria.

Each `_include` parameter specifies a search parameter to join on: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/MedicationRequest?\_include=MedicationRequest:requester POST \[base\]/MedicationRequest/\_search Content-Type: application/x-www-form-urlencoded \_include=MedicationRequest:requester GET \[base\]/MedicationRequest?\_revinclude=Provenance:target POST \[base\]/MedicationRequest/\_search Content-Type: application/x-www-form-urlencoded \_revinclude=Provenance:target

The first search requests all matching MedicationRequests, and asks the server to follow the resource references found via the `requester` search parameter value for each medication returned, and include them in the result set. The second search requests all prescriptions and any Provenance resources that refer to those prescriptions.

For each returned resource, the server identifies the resources that meet the criteria expressed in the join, and adds to the results, with the [entry.search.mode](bundle-definitions.html#Bundle.entry.search.mode) set to `include`. Note that if a resource is both a `match` directly and an `include`, the mode will be set to `match` (in some searches, it is not obvious which resources arematches, and which are includes). If there is no value in the reference element, no matching resource, or the resource cannot be retrieved (e.g., on a different server), then the resource is omitted and no error is returned.

Servers SHOULD resolve `_include` and `_revinclude` requests for version-specific references by resolving the version named in the reference.[§93](#fcss93)

`_include` and `_revinclude` are, by default, invoked only on the initial results of the search set, not on any 'included' resources. To repeatedly perform the `_include` and `_revinclude` instructions on included resources, use the [`iterate`](#modifieriterate) modifier.

For example, this search returns all [MedicationRequest](medicationrequest.html) resources and their [prescribing Practitioner](practitioner.html) Resources for the matching [MedicationDispense](medicationdispense.html) resources: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/MedicationDispense?\_include=Medication:prescription &\_include:iterate=MedicationRequest:requester&criteria... POST \[base\]/MedicationDispense/\_search Content-Type: application/x-www-form-urlencoded \_include=MedicationDispense:prescription&\_include:iterate=MedicationRequest:requester&criteria...

Details about the `iterate` modifier can be found in the section [Modifier: iterate](#modifieriterate), including potential performance considerations.

This technique applies to circular relationships as well. For example, the first of these two searches includes any related observations to the target relationships, but only those directly related. The second search asks for the `_include` based on `related` parameter to be executed iteratively, so it will retrieve observations that are directly related, and also any related observations to any other included observation. Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?\_include=Observation:has-member&criteria... POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded \_include=Observation:has-member&criteria... GET \[base\]/Observation?\_include:iterate=Observation:has-member&criteria... POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded \_include:iterate=Observation:has-member&criteria...

Both `_include` and `_revinclude` allow the use of a wild card "\*" for the search parameter name, indicating that any search parameter of `type=reference` or `type=resource` be included. Both clients and servers need to take care not to request or return too many resources when doing this. Most notably, using iterative wildcards inclusions might lead to the retrieval of the full patient's record, or even more than that: resources are organized into an interlinked network and broad `_include` paths may eventually traverse all possible paths on the server. For servers, these iterative and wildcard `_include`s are demanding and may slow the search response time significantly.

It is at the server's discretion how deep to iteratively evaluate the inclusions. Servers are expected to limit the number of iterations done to an appropriate level and are not obliged to honor requests to include related resources in the search results. Because iterative search is generally resource intensive, it is not the default behavior.

When search results are paged, each page of search results SHOULD include the matching includes for the resources in each page, so that each page stands alone as a coherent package.[§94](#fcss94)

##### 3.2.1.7.8.2 External References[](search.html#external "link to here")

If the `_include` path selects a reference that refers to a resource on another server, the server can elect to include that resource in the search results for the convenience of the client.

If the `_include` path selects a reference that refers to an entity that is not a Resource, such as an image attachment, the server may also elect to include this in the returned results as a [Binary](binary.html) resource. For example, the include path may point to an attachment which is by reference, like this: Show as: [FHIR+JSON](#json) | [FHIR+XML](#xml) "content": { "contentType": "image/jpeg", "url": "http://example.org/images/2343434/234234.jpg" } <content> <contentType>image/jpeg</contentType> <url>http://example.org/images/2343434/234234.jpg</url> </content>

The server can retrieve the target of this reference, and add it to the results for the convenience of the client.

##### 3.2.1.7.8.3 Canonical References[](search.html#includecanonical "link to here")

When including resources referenced via `canonical` elements, servers perform resolution as described for [Canonical Identifiers](#identifiercanonical). Server implementers have discretion about whether or not to support canonical resolution in includes and reverse includes, and may impose additional restrictions (e.g., only resolving canonicals that are pre-loaded, from specific packages, or within certain namespaces).

Servers that support canonical resolution in includes SHOULD advertise this capability by listing the relevant search parameters in `CapabilityStatement.rest.resource.searchInclude` and `.revInclude`.[§95](#fcss95) Including a search parameter in these elements indicates that the server will attempt to resolve and include the referenced resources.

For example, some servers will want to allow inclusion of [Questionnaire](questionnaire.html) resources referenced by [QuestionnaireResponse](questionnaireresponse.html) resources via the `questionnaire` canonical reference element: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/QuestionnaireResponse?\_include=QuestionnaireResponse:questionnaire POST \[base\]/QuestionnaireResponse/\_search Content-Type: application/x-www-form-urlencoded \_include=QuestionnaireResponse:questionnaire

##### 3.2.1.7.8.4 Graph Definitions (\_graph)[](search.html#_graph "link to here")

While inline requests for related resources are flexible, there are times when a more structured approach is desired (e.g., optimizing frequent requests, etc.). To facilitate a structured approach, the `_graph` result parameter is defined.

The `_graph` result parameter functions as a [reference](#reference) type search parameter. The input is a reference to a [GraphDefinition ![icon](external.png)](http://build.fhir.org/HL7/api-incubator/StructureDefinition-GraphDefinition) resource, which is used to define the graph of desired resources for inclusion.

##### 3.2.1.7.8.5 Contained Resources (\_contained)[](search.html#_containedType "link to here")

By default, search results only include resources that are not contained in other resources. However, chained parameters SHOULD be evaluated inside contained resources.[§96](#fcss96) To illustrate this, consider a [MedicationRequest](medicationrequest.html) resource (id: `MedicationRequest/23`) that has a contained Medication resource specifying a custom formulation that has ingredient with an `itemCodeableConcept` "abc" in "http://example.org/medications" (id: `Medication/m1`).

In this case, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/MedicationRequest?medication.ingredient-code=abc POST \[base\]/MedicationRequest/\_search Content-Type: application/x-www-form-urlencoded medication.ingredient-code=abc

will include `MedicationRequest/23` in the results as a match. The search chained into the contained resource and met the search criteria.

However, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Medication?ingredient-code=abc POST \[base\]/Medication/\_search Content-Type: application/x-www-form-urlencoded ingredient-code=abc will not find `Medication/m1` - the medication is a contained resource within a MedicationRequest and not subject to search. This is because either the wrong type of resource would be returned (the MedicationRequest containing the Medication), or the contained resource would be returned without its container resource (just the Medication), which would be missing the required context provided by the container resource.

Clients can modify the search behavior for contained resources using the `_contained` parameter, which can have one of the following values:

*   `false` (default): Do not search and return contained resources
*   `true`: search and return only contained resources
*   `both`: search and return both contained and non-contained (normal) resources

When contained resources are being returned, the server can return either the container resource, or the contained resource alone. The client can specify which by using the `_containedType` parameter, which can have one of the following values:

*   `container` (default): Return the container resources
*   `contained`: return only the contained resource

To return just the contained Medication (`Medication/m1`), the following search could be used: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Medication?ingredient-code=abc&\_contained=true&\_containedType=contained POST \[base\]/Medication/\_search Content-Type: application/x-www-form-urlencoded ingredient-code=abc&\_contained=true&\_containedType=contained This search would return a result bundle with the contained `Medication` records. Note that the `fullUrl` of the entry points to the containing resource first and includes the required resolution for the contained resource: Show as: [FHIR+JSON](#json) | [FHIR+XML](#xml) { "resourceType": "Bundle", ... "entry": \[{ "fullUrl": "http://example.com/fhir/MedicationRequest/23#m1", "resource": { "resourceType": "Medication", "id": "m1", ... }, "search": { "mode": "match" } }\] } <Bundle> ... <entry> <fullUrl value="http://example.com/fhir/MedicationRequest/23#m1"/> <resource> <Medication> <id value="m1"> ... </Medication> </resource> <search> <mode value="match"/> </search> </entry> </Bundle>

Similarly, the search could be performed to return the MedicationRequests containing the Medication records: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Medication?ingredient-code=abc&\_contained=true&\_containedType=container POST \[base\]/Medication/\_search Content-Type: application/x-www-form-urlencoded ingredient-code=abc&\_contained=true&\_containedType=container This search would return a result bundle with the container `MedicationRequest` records and the relevant contained resources. Note that even though the request was for `Medication` resources, the container `MedicationRequest` entries are categorized with the `mode` of `match`: Show as: [FHIR+JSON](#json) | [FHIR+XML](#xml) { "resourceType": "Bundle", ... "entry": \[{ "resource": { "resourceType": "MedicationRequest", "id": "23", ... "contained": \[{ "resourceType": "Medication", "id": "m1", ... }\], ... }, "search": { "mode": "match" } }\] } <Bundle> ... <entry> <resource> <MedicationRequest> <id value="23"> ... <contained> <Medication> <id value="m1"> ... </Medication> <contained> </MedicationRequest> </resource> <search> <mode value="match"/> </search> </entry> </Bundle>

##### 3.2.1.7.8.6 Paging and Other Resources[](search.html#Paging "link to here")

When returning paged results for a search with `_include` resources, all `_include` resources that are related to the primary resources returned for the page SHOULD also be returned as part of that same page, even if some of those resource instances have previously been returned on previous pages.[§97](#fcss97) This approach allows both sender and receiver to avoid caching results of other pages.

### 3.2.1.8 Standard Parameters[](search.html#standard "link to here")

Search parameters can be defined in either the core specification or externally (e.g., via Implementation Guides). The parameters defined in the core specification fall into a few categories, as listed below. Note that all search parameters defined in this specification are listed by the [search parameter registry](searchparameter-registry.html).

Note that if a 'base' [SearchParameter](searchparameter.html) defined by HL7 or an implementation guide (i.e., one that does not inherit from another `SearchParameter` definition) is referenced and the `SearchParameter` does not otherwise enumerate the modifiers and comparators, then all [modifiers](#modifiers) and [prefixes](#prefix) that are permitted for that parameter's [search type](#ptypes) are expected to be supported. If an implementation wishes to restrict which modifiers they support, the implementation must define their own `SearchParameter` instance, which SHOULD inherit from the original definition.[§98](#fcss98) The definition of any locally-defined search parameters SHALL be available to clients, either as resources at the `[base]/SearchParameter` endpoint or as [contained](references.html#contained) resources in the [CapabilityStatement](capabilitystatement.html).[§99](#fcss99)

#### 3.2.1.8.1 Parameters for all resources[](search.html#all "link to here")

The following parameters apply [to all resources](resource.html#search): `_content`, `_filter`, `_has`, `_id`, `_in`, `_language`, `_lastUpdated`, `_list`, `_profile`, `_query`, `_security`, `_source`, `_tag`, `_type`. Note that in addition to these search parameters, the search result parameters also apply to all resources.

##### 3.2.1.8.1.1 \_content[](search.html#_content "link to here")

The standard search parameter `_content` is used to allow searching all textual content of a resource.

The `_content` search parameter is defined as a [special](#special) type parameter. While the actual format used by the parameter is implementation-dependant, the search is some form of string which can be used for advanced searching (similar to [`_text`](#_text)).

For example, the following search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?\_content=cancer OR metastases OR tumor POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded \_content=cancer OR metastases OR tumor is a search for Observations that contain any of the phrases "cancer", "metastases", and "tumor" anywhere in the resource. For example, the key words may appear in:

*   `Observation.text` - Text summary of the resource, for human interpretation
*   `Observation.code.text` - Type of observation, Plain text representation of the concept
*   `Observation.note` - Comments about the observation
*   etc.

The server may choose to search for related terms (e.g., "carcinoma", etc.), but is not required to do so.

More information can be found in the section [Text Search Parameters](#text).

##### 3.2.1.8.1.2 \_has[](search.html#_has "link to here")

The `_has` parameter provides limited support for reverse chaining - that is, selecting resources based on the properties of resources that refer to them. Details and examples can be found in the [Reverse Chaining](#has) section of the search page.

##### 3.2.1.8.1.3 \_id[](search.html#_id "link to here")

The standard search parameter `_id` is used to allow searching based on the logical identifier of resources (`Resource.id`). The parameter requires a resource type specified in the [search context](#searchcontexts) - it can only be used in searches that contain a `[type]` component in the search request.

The `_id` search parameter is defined as a [token](#token) type parameter, with the restriction that only the `code` segment is allowed (e.g., no vertical pipes). This means that the literal provided as search input is matched as a code instead of a string - e.g., exact match instead of case-insensitive starts-with.

For example, the following search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?\_id=23 POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded \_id=23 is a search for Patient records with a logical id of "23".

For information about the difference between performing a read operation and searching by identifier, see [Searching Identifiers](#specialidentifiers).

##### 3.2.1.8.1.4 \_in[](search.html#_in "link to here")

The standard search parameter `_in` is used to match resources against **active** membership in collection resources. Matching is performed against the `Resource.id` of the resource against the membership test of a collection resource (e.g., CareTeam, Group, List). Details about how each resource applies the active membership test can be found on the relevant resource pages:

*   [Group Membership](group.html#membership)
*   [List Membership](list.html#membership)

The `_in` search parameter is defined as a [reference](#reference) type parameter. The target of the reference must be one of: `CareTeam`, `Group`, or `List`.

For example: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post)

**Description**

**Search**

Any resource in a CareTeam, List, or Group with id "101"

GET \[base\]/?\_in=101 POST \[base\]/\_search Content-Type: application/x-www-form-urlencoded \_in=101

Conditions in List "102"

GET \[base\]/Condition?\_in=List/102 POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded \_in=List/102

Patients which are participants of CareTeam "103"

GET \[base\]/Patient?\_in=CareTeam/103 POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded \_in=CareTeam/103

Encounters for Patients in Group "104"

GET \[base\]/Encounter?patient.\_in=Group/104 POST \[base\]/Encounter/\_search Content-Type: application/x-www-form-urlencoded patient.\_in=Group/104

Smooth muscle relaxant medications that are not in the allergy list "105"

GET \[base\]/Medication?code:below=http://snomed.info/sct|90000002&ingredient.\_in:not=List/105 POST \[base\]/Medication/\_search Content-Type: application/x-www-form-urlencoded code:below=http://snomed.info/sct|90000002&ingredient.\_in:not=List/105

Note that inactive/excluded members are not considered 'in' the membership. If a full reference is required, [chaining](#chaining) can be used to search all references.

For more information about membership testing, see [Searching by Membership](#membership).

##### 3.2.1.8.1.5 \_language[](search.html#_language "link to here")

The standard search parameter `_language` is used to match resources based on the language of the resource used. Note that match testing is performed against the element `Resource.language` and does not match against the actual language used in elements.

The `_language` search parameter is defined as a [token](#token) type parameter. [Resource.language](resource-definitions.html#Resource.language) is restricted to [Common Languages](valueset-languages.html) and [All Languages](valueset-all-languages.html).

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Questionnaire?\_language=es POST \[base\]/Questionnaire/\_search Content-Type: application/x-www-form-urlencoded \_language=es will match Questionnaire resources with a `Resource.language` of `es` (Spanish).

##### 3.2.1.8.1.6 \_lastUpdated[](search.html#_lastUpdated "link to here")

The standard search parameter `_lastUpdated` is used to match resources based on when the most recent change has been made.

The `_lastUpdated` search parameter is defined as a [date](#date) type parameter. Matching is performed against the element [Resource.meta.lastUpdated](resource-definitions.html#Meta.lastUpdated), or an implementation's internal equivalent.

Note that there may be situations where the returned data elements might change without any change to the `lastUpdated` element. For example, if a server returns a code value and its corresponding display by using a foreign-key reference to a code system, updates to the code system that affect the display name might not be reflected in the `lastUpdated` value of the resource holding the code. It is also possible that an underlying record might be updated in a way that is not surfaced in the FHIR representation of a resource (e.g., changing non-exposed metadata) which can cause `lastUpdated` to change even when there is no apparent change in the data.

For example, the following search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?\_lastUpdated=gt2010-10-01 POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded \_lastUpdated=gt2010-10-01

is a search for Observations changed since October 1, 2010. Applications considering using this parameter should also consider defined synchronization approaches - [RESTful history](http.html#history) and [Subscriptions framework](subscriptions.html).

##### 3.2.1.8.1.7 \_list[](search.html#_list "link to here")

The standard search parameter `_list` is used to test resources against references in a `List` resource.

The `_list` search parameter is defined as a [special](#special) type parameter. Input values are treated as [token](#token) values - either the logical identifier (id) of a `List` or a literal for a functional list, as defined in [Current Resource Lists](lifecycle.html#current). Note that when using functional lists, servers are not required to make the lists available to the clients as `List` resources, but may choose to do so.

For example: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post)

**Description**

**Search**

Any resource referenced by List "101"

GET \[base\]/?\_list=101 POST \[base\]/\_search Content-Type: application/x-www-form-urlencoded \_list=101

Conditions referenced by List "102"

GET \[base\]/Condition?\_list=102 POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded \_list=102

Female Patients referenced by List "103"

GET \[base\]/Patient?\_list=013&gender=female POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded \_list=013&gender=female

Encounters for Patients in Group "104"

GET \[base\]/Encounter?patient.\_in=Group/104 POST \[base\]/Encounter/\_search Content-Type: application/x-www-form-urlencoded patient.\_in=Group/104

Current allergy list for patient "42"

GET \[base\]/AllergyIntolerance?patient=42&\_list=\\$current-allergies POST \[base\]/AllergyIntolerance/\_search Content-Type: application/x-www-form-urlencoded patient=42&\_list=\\$current-allergies

For more information about membership testing, see [Searching by Membership](#membership).

##### 3.2.1.8.1.8 \_profile[](search.html#_profile "link to here")

The standard search parameter `_profile` is used to match resources based on values in the [Resource.meta.profile](resource-definitions.html#Meta.profile) element. Note that the profile search does not test conformance of a resource against any profile, just the values of that element.

The `_profile` search parameter is defined as a [reference](#reference) type parameter.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?\_profile=http://hl7.org/fhir/StructureDefinition/bp POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded \_profile=http://hl7.org/fhir/StructureDefinition/bp will match Observation resources that contain a `Resource.meta.profile` of `http://hl7.org/fhir/StructureDefinition/bp`.

##### 3.2.1.8.1.9 \_query[](search.html#_query "link to here")

The standard search parameter `_query` is used to execute a pre-defined and named query operation. Note that there can only ever be one `_query` parameter in a set of search parameters. Servers processing search requests SHALL refuse to process a search request if they do not recognize the `_query` parameter value.[§100](#fcss100)

The `_query` search parameter is defined as a [special](#special) type parameter. Input to the parameter is treated as a token-based search, requiring an exact match to the query name, as defined by the `OperationDefinition.code` element.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?\_query=current-high-risk&ward=1A POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded \_query=current-high-risk&ward=1A is a request to run the named query `current-high-risk` against Patient resources, with a parameter named `ward` set to the value `1A`. If a server does not have a definition for `current-high-risk`, it would reject the request.

More information about named queries can be found in the section [Named Queries](#advanced).

##### 3.2.1.8.1.10 \_security[](search.html#_security "link to here")

The standard search parameter `_security` is used to match resources based on security labels in the [Resource.meta.security](resource-definitions.html#Meta.security) element.

The `_security` search parameter is defined as a [token](#token) type parameter.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?\_security=http://terminology.hl7.org/CodeSystem/v3-Confidentiality|R POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded \_security=http://terminology.hl7.org/CodeSystem/v3-Confidentiality|R will match Observation resources that contain a `Resource.meta.security` value matching the system `http://terminology.hl7.org/CodeSystem/v3-Confidentiality` and code of `R`.

For more information about security labels, please see the [Security Labels](security-labels.html) page.

##### 3.2.1.8.1.11 \_source[](search.html#_source "link to here")

The standard search parameter `_source` is used to match resources based on source information in the [Resource.meta.source](resource-definitions.html#Meta.source) element. `Meta.source` is a lightweight way of providing minimal provenance for resources. If more functionality is required, use of [Provenance](provenance.html) is recommended.

The `_source` search parameter is defined as a [uri](#uri) type parameter.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Patient?\_source=http://example.com/Organization/123 POST \[base\]/Patient/\_search Content-Type: application/x-www-form-urlencoded \_source=http://example.com/Organization/123 will match Patient resources that contain the `Resource.meta.source` value of `http://example.com/Organization/123`.

##### 3.2.1.8.1.12 \_tag[](search.html#_tag "link to here")

The standard search parameter `_tag` is used to match resources based on tag information in the [Resource.meta.tag](resource-definitions.html#Meta.tag) element. Tags are intended to be used to identify and relate resources to process and workflow.

The `_tag` search parameter is defined as a [token](#token) type parameter.

For example, the search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?\_tag=http://terminology.hl7.org/ValueSet/v3-SeverityObservation|H POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded \_tag=http://terminology.hl7.org/ValueSet/v3-SeverityObservation|H will match Observation resources that contain a tag value matching the system `http://terminology.hl7.org/ValueSet/v3-SeverityObservation` and code `H`, indicating that an observation has been tagged as potentially life-threatening or with the potential cause permanent injury.

##### 3.2.1.8.1.13 \_text[](search.html#_text "link to here")

The standard search parameter `_text` is used to perform searches against the narrative content of a resource.

The `_text` search parameter is defined as a [special](#special) type parameter. While the actual format used by the parameter is implementation-dependant, the search is some form of text which can be used for advanced searching.

For example, the following search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Observation?\_text=cancer OR metastases OR tumor POST \[base\]/Observation/\_search Content-Type: application/x-www-form-urlencoded \_text=cancer OR metastases OR tumor is a search for Observations that contain any of the phrases "cancer", "metastases", and "tumor" in their narrative text (`Observation.text`). For example, the key words may appear in:

*   `Observation.text` - Text summary of the resource, for human interpretation
*   `Observation.code.text` - Type of observation, Plain text representation of the concept

The server may choose to search for related terms (e.g., "carcinoma", etc.), but is not required to do so.

More information can be found in the section [Text Search Parameters](#text).

##### 3.2.1.8.1.14 \_type[](search.html#_type "link to here")

The standard search parameter `_type` is used to allow filtering of types in searches that are performed across multiple resource types (e.g., searches across the server root).

The `_type` search parameter is defined as a [special](#special) type parameter, because there is no standard path expression for searching the types of resources. However, the parameter is a token parameter restricted to the [Resource Types](valueset-resource-types.html) Value Set.

For example, the following search: Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/?\_type=Observation,Condition&patient=Patient/123 POST \[base\]/\_search Content-Type: application/x-www-form-urlencoded \_type=Observation,Condition&patient=Patient/123 is a search for Observations and Conditions that are about the patient `Patient/123`. Since the `patient` search parameter is valid on both the Observation and Condition resources, the search is valid. The same search **would not** be valid if the `_type` parameter was excluded, since `patient` is not a search parameter that works across all resources.

More information can be found in the section [Searching Multiple Resource Types](#types).

#### 3.2.1.8.2 Parameters for each resource[](search.html#parameters "link to here")

In addition to the common parameters defined for all resources, each FHIR resource type defines its own set of search parameters with their names, types, and meanings. These search parameters are on the same page as the resource definitions, and are also published as part of the standard Capability statement ([XML](capabilitystatement-base.xml.html) or [JSON](capabilitystatement-base.json.html)).

In general, the defined search parameters correspond to a single element in the resource, but this is not required, and some search parameters refer to the same type of element in multiple places, or refer to derived values.

Some search parameters defined by resources are associated with more than one path in a resource. This means that the search parameter matches if any of the paths contain matching content. If a path matches, the whole resource is returned in the search results. The client may have to examine the resource to determine which path contains the match.

Servers are not required to implement any of the standard search parameters. However, servers SHOULD support the [`_id`](#id) parameter.[§101](#fcss101) Servers MAY also define their own search parameters.[§102](#fcss102)

Note that all search parameters defined in this specification, both common and resource-specific, are listed in the [search parameter registry](searchparameter-registry.html).

#### 3.2.1.8.3 Text Search Parameters[](search.html#content "link to here")

The special text search parameters `_text` and `_content` search on the narrative of the resource and the entire content of the resource respectively. Just like string parameters using the [`:text`](#modifiers) modifier, the `_text` and `_content` parameters SHOULD support a sophisticated search functionality of the type offered by typical text indexing services.[§103](#fcss103) The value of the parameter is a text-based search, which may involve searching multiple words with thesaurus and proximity considerations, and logical operations such as AND, OR etc. For example (note that the values are unescaped for clarity): Show as unencoded: [HTTP GET](#get) | [HTTP POST](#post) GET \[base\]/Condition?\_text=(bone OR liver) AND metastases POST \[base\]/Condition/\_search Content-Type: application/x-www-form-urlencoded \_text=(bone OR liver) AND metastases

This request returns all Condition resources with the word "metastases" and either "bone" or "liver" in the narrative. The server may choose to search for related words as well.

Implementers could consider using the rules specified by the [OData specification for the $`search` parameter ![icon](external.png)](http://docs.oasis-open.org/odata/odata/v4.0/cs01/part1-protocol/odata-v4.0-cs01-part1-protocol.html#_The_$search_System) . Typical implementations would use Lucene, Solr, an SQL-based full text search, or some similar indexing service.

### 3.2.1.9 Server Conformance[](search.html#conformance "link to here")

In order to allow the client to be confident about what search parameters were used as criteria by the server, the server SHALL return the parameters that were actually used to process the search.[§104](#fcss104) Applications processing search results SHALL check these returned values where necessary.[§105](#fcss105) For example, if the server did not support some of the filters specified in the search, a client might manually apply those filters to the retrieved result set, display a warning message to the user or take some other action.

In the case of a RESTful search, these parameters are encoded in the self link in the bundle that is returned: Show as: [FHIR+JSON](#json) | [FHIR+XML](#xml) "link": { "relation": "self", "url": "http://example.org/Patient?name=peter" } <link> <relation value="self"/> <url value="http://example.org/Patient?name=peter"/> </link>

In other respects, servers have considerable discretion with regards to supporting search:

*   Servers can choose which parameters to support (other than `_id` above).
*   Servers can choose when and where to implement parameter chaining, and when and where they support the `_include` parameter.
*   Servers can declare additional parameters in the profiles referenced from their Capability statements. Servers and IG authors SHOULD define custom search parameters with codes starting with a prefix defining the scope of the defining organization. Such a prefix SHALL NOT start with characters other than alphanumeric characters (e.g. '-', '~', etc.), as other characters either already have or may in the future have meaning within the search syntax. However, this is not sufficient to guarantee disambiguation - see the rules in [Search Tests](#matching-basics)[§106](#fcss106).
*   Servers are not required to enforce case sensitivity on parameter names, though the names are case sensitive (and URLs are generally case-sensitive).
*   Servers can choose how many results to return, though the client can use `_count` to limit.
*   Servers can choose how to sort the return results, though they SHOULD honor the `_sort` parameter.[§107](#fcss107)

### 3.2.1.10 Search Result Currency[](search.html#currency "link to here")

The results of a search operation are only guaranteed to be current at the instant the operation is executed. After the operation is executed, ongoing actions performed on the resources against which the search was executed will render the results increasingly stale. The significance of this depends on the nature of the search, and the kind of use that is being made of the results.

This is particularly relevant when the server is returning the results in a series of pages. It is at the discretion of the search engine of how to handle ongoing updates to the resources while the search is proceeding.

Performing a search operation does not change the set of resources on the server, with the possible exception of the creation of [Audit Event](auditevent.html) resources auditing the search itself.

### 3.2.1.11 Summary Tables[](search.html#table "link to here")

**Conformance Statement Summary**

*   [§0](#fcs0) Servers SHALL declare what features of search they require through their [CapabilityStatement search declarations](capabilitystatement.html#search-decl)
*   [§1](#fcs1) Implementers SHOULD review the [safety checklist](safety.html#search).
*   [§2](#fcs2) Clients and servers SHOULD be robust in their handling of URLs present in the content of FHIR bundles, such as in `Bundle.link.url`.
*   [§3](#fcs3) Clients SHOULD use correct case, and servers SHALL NOT define additional parameters with different meanings with names that only differ in case.
*   [§4](#fcs4) In the absence of any search filters, e.g., `GET [base]`, `GET [base]/Patient`, or `POST [base]/_search` or `POST [base]/Patient/_search` with no body, a server SHOULD return all records in scope of the search context.
*   [§5](#fcs5) Servers MAY reject a search as overly-broad, and SHOULD return an appropriate error in that situation (e.g., [too-costly](codesystem-issue-type.html#issue-type-too-costly)).
*   [§6](#fcs6) Note that if the `_type` parameter is included, all other search parameters SHALL be common to all provided types, and if `_type` is not included, all parameters SHALL be common to all resource types.
*   [§7](#fcs7) In order to allow the client to be confident about what search parameters were used as criteria by a server, servers SHALL return the parameters that were actually used to process a search.
*   [§8](#fcs8) Applications processing search results SHALL check these returned values where necessary.
*   [§9](#fcs9) Self links SHALL be expressed as an HTTP GET-based search with the relevant parameters included as query parameters, because of the semantics around the link types.
*   [§10](#fcs10) Self links MAY be returned as absolute URIs or URIs relative to the base URL of a server and MAY or MAY NOT be resolvable.
*   [§11](#fcs11) Clients SHALL review the returned parameters in the self link to ensure proper processing of results.
*   [§12](#fcs12) In those situations, clients SHOULD filter received records to account for additional data.
*   [§13](#fcs13) Servers MAY include such filters in the self link.
*   [§14](#fcs14) Clients SHOULD be able to handle search parameters in a self link that they did not request.
*   [§15](#fcs15) Servers SHOULD consider whether exposing search parameters in the self link would constitute inappropriate leakage of security information.
*   [§16](#fcs16) As with the self link, all relevant paging links SHALL be expressed as GET requests.
*   [§17](#fcs17) Servers SHOULD NOT include content that is considered sensitive in URLs, excluding the self link.
*   [§18](#fcs18) If a server is unable to execute a search request, it MAY either return an error for the request or return success with an `outcome` containing details of the error.
*   [§19](#fcs19) When the search fails, a server SHOULD return an [OperationOutcome](operationoutcome.html) detailing the cause of the failure.
*   [§20](#fcs20) Where the content of the parameter is syntactically incorrect, servers SHOULD return an error.
*   [§21](#fcs21) However, where the issue is a logical condition (e.g., unknown subject or code), the server SHOULD process the search, including processing the parameter - with the result of returning an empty search set, since the parameter cannot be satisfied.
*   [§22](#fcs22) In general, servers SHOULD ignore unknown or unsupported parameters for the following reasons:
*   [§23](#fcs23) Servers SHOULD honor the client's `prefer` request, but are not required to do so.
*   [§24](#fcs24) This SHOULD only occur in situations where it is known that some filters haven't been applied.
*   [§25](#fcs25) Servers MAY support [batching](http.html#transaction) multiple requests.
*   [§26](#fcs26) When requesting searches in a bundle, systems SHOULD accept searches via GET, even if they do not typically accept GET-based searches.
*   [§27](#fcs27) Servers receiving searches within a Bundle SHOULD NOT impose any GET-specific limitations on search (i.e., restrictions not imposed on POST-based searches) that they would impose if the GET-based search had been received outside a Bundle.
*   [§28](#fcs28) Servers MAY support FHIR [Messaging](messaging.html).
*   [§29](#fcs29) Individual search parameters MAY also allow "modifiers" that control their behavior.
*   [§30](#fcs30) A search parameter MAY instead explicitly choose elements by using an expression that instead points directly to the sub-elements.
*   [§31](#fcs31) Search parameter definitions MAY include a list of allowed modifiers in the `SearchParameter.modifier` element, which is bound to the [search-modifier-code](valueset-search-modifier-code.html) value set.
*   [§32](#fcs32) Servers SHALL support any modifiers present in search parameters the server advertises support for.
*   [§33](#fcs33) Servers MAY choose to support additional modifiers.
*   [§34](#fcs34) Note that servers MAY support modifiers on types not defined in this specification.
*   [§35](#fcs35) Since modifiers change the meaning of a search parameter, a server SHALL reject any search request that contains a search parameter with an unsupported modifier.
*   [§36](#fcs36) The definition for any search parameter of type "special" SHALL explicitly list any allowed modifiers; this list MAY include any value from the [search-modifier-code](codesystem-search-modifier-code.html) code system.
*   [§37](#fcs37) Version-related search criteria against resources with unknown versioning schemes SHALL be either ignored or rejected.
*   [§38](#fcs38) Version-related search criteria against resources with unknown versioning schemes SHALL be either ignored or rejected.
*   [§39](#fcs39) Implementers of the `text` modifier SHOULD support a sophisticated search functionality of the type offered by typical text indexing services.
*   [§40](#fcs40) Implementers of the `text-advanced` modifier SHOULD support a sophisticated search functionality of the type offered by typical text indexing services, but MAY support only basic search with minor additions (e.g., word-boundary recognition).
*   [§41](#fcs41) If a reference resolves to a type different than indicated or expected, a server SHOULD consider if it is appropriate to include the issue via an `OperationOutcome` in the response.
*   [§42](#fcs42) The date parameter format is `yyyy-mm-ddThh:mm:ss.ssss[Z|(+|-)hh:mm]` (the standard XML format). Note that fractional seconds MAY be ignored by servers.
*   [§43](#fcs43) Any degree of precision can be provided, but it SHALL be populated from the left (e.g., can't specify a month without a year), except that the minutes SHALL be present if an hour is present, and you SHOULD provide a timezone if the time part is present.
*   [§44](#fcs44) Note: Time can consist of hours and minutes with no seconds, unlike the XML Schema dateTime type. Some user agents may escape the `:` characters in the URL, and servers SHALL handle this correctly.
*   [§45](#fcs45) Where possible, the system SHOULD correct for timezones when performing queries.
*   [§46](#fcs46) Dates do not have timezones, and timezones SHOULD NOT be considered.
*   [§47](#fcs47) Where both search parameters and resource element date-times do not have timezones, the servers local timezone SHOULD be assumed.
*   [§48](#fcs48) If a reference value is a non-versioned relative reference (e.g., does not contain `[url]` or `_history/[version]` parts), the search SHOULD match instances that match the reference in it contains a versioned reference.
*   [§49](#fcs49) Servers SHOULD fail a search where the logical id in a reference refers to more than one matching resource across different types.
*   [§50](#fcs50) Punctuation and non-significant whitespace (e.g., repeated space characters, tab vs space) SHOULD also be ignored.
*   [§51](#fcs51) When a string type search parameter points to a complex or backbone element (an element that contains sub-elements), by default the search SHOULD be considered as a search against one or more string values in sub-elements, as selected by the implementation.
*   [§52](#fcs52) A search parameter MAY explicitly choose elements by using an expression that instead points directly to the sub-elements.
*   [§53](#fcs53) For robust search, servers SHOULD search the parts of a family name independently.
*   [§54](#fcs54) HL7 affiliates MAY make more specific recommendations about search expectations regarding names in their specific culture.
*   [§55](#fcs55) When in doubt, servers SHOULD treat tokens in a case-insensitive manner, on the grounds that including undesired data has less safety implications than excluding desired behavior.
*   [§56](#fcs56) Clients SHOULD always use the correct case when possible, and allow for the server to perform case-insensitive matching.
*   [§57](#fcs57) For token parameters on elements of type [id](datatypes.html#id), [ContactPoint](datatypes.html#ContactPoint), [uri](datatypes.html#uri), or [boolean](datatypes.html#boolean), the pipe symbol SHALL NOT be used - only the `[parameter]=[code]` form is allowed
*   [§58](#fcs58) Composite search parameters SHALL NOT declare components that reference other composite search parameters.
*   [§59](#fcs59) Where a search does not act on the referenced version, search results SHOULD contain a [OperationOutcome](operationoutcome.html) with a warning that indicates the discrepancy.
*   [§60](#fcs60) If a resource has a reference that is versioned and the resource is included (e.g., `_include`, `_revinclude`), the specified version SHOULD be provided.
*   [§61](#fcs61) For canonical references, servers SHOULD support searching by Canonical URLs, and SHOULD support automatically detecting a `|[version]` portion as part of the search parameter and interpreting that portion as a search on the business version of the target resource.
*   [§62](#fcs62) If a reference resolves to a type different than allowed via the reference, a server SHOULD consider if it is appropriate to include the issue via an `OperationOutcome` in the response.
*   [§63](#fcs63) Servers SHOULD fail a search where the logical id of a reference refers to more than one matching resource across different types.
*   [§64](#fcs64) If a request references multiple parameters that are not the same type (e.g., if one parameter is a `number` type and another is a `token` type, despite sharing the same name), this is an error and servers SHOULD return a `400` status.
*   [§65](#fcs65) Implementation Guides MAY set specific expectations about search behavior around implicit resources.
*   [§66](#fcs66) Named-query operations SHOULD NOT be invoked via any method other than search with the `_query` parameter (e.g., servers SHOULD NOT execute query operations via `[base]/$[operation code]` or `[base]/[type]/$[operation code]`, etc.).
*   [§67](#fcs67) Clients SHOULD NOT assume the availability of any search parameter that is not supported for general queries (i.e., listed in the server's [CapabilityStatement](capabilitystatement.html), either in [CapabilityStatement.rest.searchParam](capabilitystatement-definitions.html#CapabilityStatement.rest.searchParam) or [CapabilityStatement.rest.resource.searchParam](capabilitystatement-definitions.html#CapabilityStatement.rest.resource.searchParam)).
*   [§68](#fcs68) Servers MAY further document the search parameters they support for a specific `query` operation - either in text or by defining a constrained operation based on the original that explicitly enumerates all supported parameters.
*   [§69](#fcs69) In order to ensure consistent behavior, authors SHOULD include relevant search parameters in the named query definition (`OperationDefinition`).
*   [§70](#fcs70) Note that servers SHOULD accept query parameters that are not enumerated in named queries based on their standard support (e.g., parameters that are supported for a given resource type), but servers MAY choose to ignore parameters not specified by the OperationDefinition.
*   [§71](#fcs71) Note that query parameters are unordered, so parameters to a named query MAY appear in any location or sequence.
*   [§72](#fcs72) The specification (or Implementation Guide) that defines an additional resource **SHOULD** define a set of standard search parameters for that resource
*   [§73](#fcs73) Note that with the exception of `_include` and `_revinclude`, search result parameters SHOULD only appear once in a search. If such a parameter appears more than once, the behavior is undefined and a server MAY treat the situation as an error.
*   [§74](#fcs74) When sorting on string search parameters, sorting SHOULD be performed on a case-insensitive basis.
*   [§75](#fcs75) Servers MAY or MAY NOT support the `text` sort modifier behavior, and MAY fall back to token-based sorting or disregard the value.
*   [§76](#fcs76) Servers SHALL NOT return more resources in a single page than requested, even if they don't support paging, but MAY return less than the client requested.
*   [§77](#fcs77) NOTE: This means that all servers that support search or history SHALL support checking the `_count` parameter.
*   [§78](#fcs78) The server SHOULD ensure that any pages reachable via links (e.g., previous/next) respect the the original requested `_count` expectations (or a server-overridden max page size).
*   [§79](#fcs79) If `_count` has the value 0 (zero), this SHALL be treated the same as `_summary=count`: the server returns a bundle that reports the total number of resources that match in `Bundle.total`, but with no entries, and no previous/next/last links.
*   [§80](#fcs80) If supported, a server SHOULD NOT return more resources than requested.
*   [§81](#fcs81) As with other parameters, servers supporting \_maxresults SHALL include this parameter in the [Self Link](search.html#selflink) when returning results if it is used.
*   [§82](#fcs82) This subset SHOULD consist solely of all supported elements that are marked as "summary" in the base definition of the resource(s) (see [ElementDefinition.isSummary](elementdefinition-definitions.html#ElementDefinition.isSummary)).
*   [§83](#fcs83) Return only the `text`, `id`, `meta`, and top-level mandatory elements (these mandatory elements are included to ensure that the payload is valid FHIR; servers MAY omit elements within these sub-trees as long as they ensure that the payload is valid).
*   [§84](#fcs84) Servers MAY return extensions in summary mode, but clients SHOULD NOT rely on extensions being present and SHOULD use another search mode if data contained in extensions is required.
*   [§85](#fcs85) Servers SHOULD mark the by populating `meta.tag` resources with the code [`SUBSETTED` ![icon](external.png)](https://terminology.hl7.org/CodeSystem-v3-ObservationValue.html#v3-ObservationValue-SUBSETTED) to ensure that the incomplete resource is not accidentally used to overwrite a complete resource.
*   [§86](#fcs86) Note that the [self link](#selflink) in the search result Bundle is important for the interpretation of the result and SHALL always be returned, regardless of the type of `_summary`.
*   [§87](#fcs87) Each element name SHALL be the base element name without specifying `[x]` or one of the types (e.g., "value" is valid, while "value\[x\]" and "valueQuantity" are not).
*   [§88](#fcs88) Clients SHOULD list all mandatory and modifier elements in a resource as part of the list of elements.
*   [§89](#fcs89) Servers SHOULD return mandatory and modifier elements whether they are requested or not.
*   [§90](#fcs90) Servers SHOULD mark the resources with the tag [`SUBSETTED` ![icon](external.png)](https://terminology.hl7.org/CodeSystem-v3-ObservationValue.html#v3-ObservationValue-SUBSETTED) to ensure that the incomplete resource is not actually used to overwrite a complete resource.
*   [§91](#fcs91) If a client supplies `_elements` values prefixed with resource types, such as `_elements=Patient.gender`, then servers SHOULD apply these element-level restrictions to all resources in the returned bundle, rather than just to direct search matches (i.e., restrictions should be applied to bundle entries with any search.mode).
*   [§92](#fcs92) Note that implementers intending to support wildcard (`*`) segments SHOULD advertise them along with other resources and search parameters in their capability statement (e.g., in `CapabilityStatement.rest.resource.searchInclude`).
*   [§93](#fcs93) Servers SHOULD resolve `_include` and `_revinclude` requests for version-specific references by resolving the version named in the reference.
*   [§94](#fcs94) When search results are paged, each page of search results SHOULD include the matching includes for the resources in each page, so that each page stands alone as a coherent package.
*   [§95](#fcs95) Servers that support canonical resolution in includes SHOULD advertise this capability by listing the relevant search parameters in `CapabilityStatement.rest.resource.searchInclude` and `.revInclude`.
*   [§96](#fcs96) However, chained parameters SHOULD be evaluated inside contained resources.
*   [§97](#fcs97) When returning paged results for a search with `_include` resources, all `_include` resources that are related to the primary resources returned for the page SHOULD also be returned as part of that same page, even if some of those resource instances have previously been returned on previous pages.
*   [§98](#fcs98) If an implementation wishes to restrict which modifiers they support, the implementation must define their own `SearchParameter` instance, which SHOULD inherit from the original definition.
*   [§99](#fcs99) The definition of any locally-defined search parameters SHALL be available to clients, either as resources at the `[base]/SearchParameter` endpoint or as [contained](references.html#contained) resources in the [CapabilityStatement](capabilitystatement.html).
*   [§100](#fcs100) Servers processing search requests SHALL refuse to process a search request if they do not recognize the `_query` parameter value.
*   [§101](#fcs101) However, servers SHOULD support the [`_id`](#id) parameter.
*   [§102](#fcs102) Servers MAY also define their own search parameters.
*   [§103](#fcs103) Just like string parameters using the [`:text`](#modifiers) modifier, the `_text` and `_content` parameters SHOULD support a sophisticated search functionality of the type offered by typical text indexing services.
*   [§104](#fcs104) In order to allow the client to be confident about what search parameters were used as criteria by the server, the server SHALL return the parameters that were actually used to process the search.
*   [§105](#fcs105) Applications processing search results SHALL check these returned values where necessary.
*   [§106](#fcs106) Servers and IG authors SHOULD define custom search parameters with codes starting with a prefix defining the scope of the defining organization. Such a prefix SHALL NOT start with characters other than alphanumeric characters (e.g. '-', '~', etc.), as other characters either already have or may in the future have meaning within the search syntax. However, this is not sufficient to guarantee disambiguation - see the rules in [Search Tests](#matching-basics)
*   [§107](#fcs107) Servers can choose how to sort the return results, though they SHOULD honor the `_sort` parameter.

Common Parameters defined for all resources:

**Name**

**Type**

**Description**

**Paths**

[`_content`](#_content)

[string](#string)

Text search against the entire resource

[`_filter`](#_filter)

[special](#special)

Filter search parameter which supports a more sophisticated grammar for searching

[`_has`](#_has)

[special](#special)

Provides limited support for reverse chaining

[`_id`](#_id)

[token](#token)

Resource id (not a full URL)

Resource.id

[`_in`](#_in)

[reference](#reference)

Group, List, or CareTeam membership

Resource.id

[`_language`](#_language)

[token](#token)

Language of the resource content

Resource.language

[`_lastUpdated`](#_lastUpdated)

[date](#date)

Date last updated. Server has discretion on the boundary precision

Resource.meta.lastUpdated

[`_list`](#_list)

[string](#string)

All resources in [nominated list](lifecycle.html#current) (by id, not a full URL)

[`_profile`](#_profile)

[reference](#reference)

Search for all resources tagged with a profile

Resource.meta.profile

[`_query`](#_query)

[string](#string)

Custom named query

[`_security`](#_security)

[token](#token)

Search by a security label

Resource.meta.security

[`_source`](#_source)

[uri](#uri)

Search by where the resource comes from

Resource.meta.source

[`_tag`](#_tag)

[token](#token)

Search by a resource tag

Resource.meta.tag

[`_text`](#_text)

[string](#string)

Text search against the narrative

Search Control Parameters:

**Name**

**Type**

**Description**

**Allowable Content**

[`_contained`](#_contained)

[string](#string)

Whether to return resources contained in other resources in the search matches

`true` | `false` | `both` (`false` is default)

[`_containedType`](#_containedType)

[string](#string)

If returning contained resources, whether to return the contained or container resources

`container` | `contained`

[`_count`](#_count)

[number](#number)

Number of results per page

Whole number

[`_elements`](#_elements)

[token](#token)

Request that only a specific set of elements be returned for resources

[`_graph`](#_graph)

[reference](#reference)

Include related resources according to a `GraphDefinition`

[`_include`](#_include)

[string](#string)

Other resources to include in the search results that search matches point to

SourceType:searchParam(:targetType)

[`_maxresults`](#_maxresults)

[number](#number)

Hint to a server that only the first 'n' results will ever be processed

Whole number

[`_revinclude`](#_revinclude)

[string](#string)

Other resources to include in the search results when they refer to search matches

SourceType:searchParam(:targetType)

[`_score`](#_score)

[token](#token)

Request match relevance in results

`true` | `false`

[`_sort`](#_sort)

[string](#string)

Order to sort results in (can repeat for inner sort orders)

Name of a valid search parameter

[`_summary`](#_summary)

[string](#string)

Just return the summary elements (for resources where this is defined)

`true` | `false` (`false` is default)

[`_total`](#_total)

[token](#token)

Request a precision of the total number of results for a request

`none` | `estimate` | `accurate`

Cross-map between search parameter types and Datatypes:

**Datatype**

**[number](#number)**

**[date](#date)**

**[reference](#reference)**

**[quantity](#quantity)**

**[uri](#uri)**

**[string](#string)**

**[token](#token)**

**Primitive Types**

[base64Binary](datatypes.html#base64Binary)

Not used in search

[boolean](datatypes.html#boolean)

N

N

N

N

N

N

Y . true|false (System = http://terminology.hl7.org/CodeSystem/special-values but not usually needed)

[canonical](datatypes.html#canonical)

N

N

Y

N

Y

N

Y

[code](datatypes.html#code)

N

N

N

N

N

N

Y . (System, if desired, is defined in the underlying value set for each code)

[date](datatypes.html#date)

N

Y

N

N

N

N

N

[dateTime](datatypes.html#dateTime)

N

Y

N

N

N

N

N

[decimal](datatypes.html#decimal)

Y

N

N

N

N

N

N

[id](datatypes.html#id)

N

N

N

N

N

N

Y

[instant](datatypes.html#instant)

N

Y

N

N

N

N

N

[integer](datatypes.html#integer)

Y

N

N

N

N

N

N

[markdown](datatypes.html#markdown)

Not used in search

[oid](datatypes.html#oid)

Not used in search (but see uri)

[positiveInt](datatypes.html#positiveInt)

Not used in search (but see integer)

[string](datatypes.html#string)

N

N

N

N

N

Y

Y

[time](datatypes.html#time)

Not used in search

[unsignedInt](datatypes.html#unsignedInt)

Not used in search (but see integer)

[uri](datatypes.html#uri)

N

N

Y

N

Y

N

N

[url](datatypes.html#url)

Not used in search (but see uri)

[uuid](datatypes.html#uuid)

Not used in search (but see uri)

**Datatypes**

[Address](datatypes.html#Address)

N

N

N

N

N

Y search on any string element in the address

N

[Age](datatypes.html#Age)

N

N

N

Y

N

N

N

[Annotation](datatypes.html#Annotation)

Not used in search

[Attachment](datatypes.html#Annotation)

Not used in search

[CodeableConcept](datatypes.html#CodeableConcept)

N

N

N

N

N

N

Y

[CodeableReference](datatypes.html#CodeableReference)

Not used in search (searches either refer to .concept or .reference)

[Coding](datatypes.html#Coding)

N

N

N

N

N

N

Y

[Count](datatypes.html#Count)

Not used in search

[ContactPoint](datatypes.html#ContactPoint)

N

N

N

N

N

N

Y

[Distance](datatypes.html#Distance)

Not used in search

[Duration](datatypes.html#Duration)

N

N

N

Y

N

N

N

[HumanName](datatypes.html#HumanName)

N

N

N

N

N

Y Search on any string element in the name

N

[Identifier](datatypes.html#Identifier)

N

N

N

N

N

N

Y

[Money](datatypes.html#Money)

N

N

N

Y

N

N

N

[Period](datatypes.html#Period)

N

Y

N

N

N

N

N

[Quantity](datatypes.html#Quantity)

N

N

N

Y

N

N

N

[Range](datatypes.html#Range)

N

N

N

Y

N

N

N

[Ratio](datatypes.html#Ratio)

Not used in search

[Reference](datatypes.html#Reference)

N

N

Y

N

N

N

N

[SampledData](datatypes.html#base64Binary)

Not used in search

[Signature](datatypes.html#Signature)

Not used in search

[Timing](datatypes.html#Timing)

N

Y

N

N

N

N

N

FHIR ®© HL7.org 2011+. FHIR R6 hl7.fhir.core#6.0.0-ballot3 generated on Wed, Dec 17, 2025 09:55+0000.  
Links: [Search ![icon](external.png)](http://hl7.org/fhir/search.cfm) | [Version History](history.html) | [Contents](toc.html) | [Glossary](help.html) | [QA](qa.html) | [Compare to R4 ![icon](external.png)](https://www.fhir.org/perl/htmldiff.pl?oldfile=http%3A%2F%2Fhl7.org%2Ffhir%2FR4%2Fsearch.html&newfile=http%3A%2F%2Fbuild.fhir.org%2Fsearch.html) | [Compare to R5 ![icon](external.png)](https://www.fhir.org/perl/htmldiff.pl?oldfile=http%3A%2F%2Fhl7.org%2Ffhir%2FR5%2Fsearch.html&newfile=http%3A%2F%2Fbuild.fhir.org%2Fsearch.html) | [Compare to Last Ballot ![icon](external.png)](https://www.fhir.org/perl/htmldiff.pl?oldfile=http%3A%2F%2Fhl7.org%2Ffhir%2F6.0.0-ballot3%2Fsearch.html&newfile=http%3A%2F%2Fbuild.fhir.org%2Fsearch.html) | [![CC0](cc0.png)](license.html) | [Propose a change ![icon](external.png)](https://jira.hl7.org/secure/CreateIssueDetails!init.jspa?pid=10405&issuetype=10600&customfield_11302=FHIR-core&customfield_11808=R5&customfield_10612=http://build.fhir.org/search.html) 

$(function() { try { var httpVerbSelector = sessionStorage.getItem('fhir-http-verb-selector'); } catch(exception){ } if (!httpVerbSelector) { httpVerbSelector = 'get'; } $(\`.http-verb-display-${httpVerbSelector}\`).show().siblings().hide(); $('.http-verb-selector a').on('click', function(e) { e.preventDefault(); httpVerbSelector = $(this).attr('href').substr(1); $(\`.http-verb-display-${httpVerbSelector}\`).show().siblings().hide(); try { sessionStorage.setItem('fhir-http-verb-selector', httpVerbSelector); } catch(exception){ } }); try { var exampleFormatSelector = sessionStorage.getItem('fhir-example-format-selector'); } catch(exception){ } if (!exampleFormatSelector) { exampleFormatSelector = 'json'; } $(\`.example-format-display-${exampleFormatSelector}\`).show().siblings().hide(); $('.example-format-selector a').on('click', function(e) { e.preventDefault(); exampleFormatSelector = $(this).attr('href').substr(1); $(\`.example-format-display-${exampleFormatSelector}\`).show().siblings().hide(); try { sessionStorage.setItem('fhir-example-format-selector', exampleFormatSelector); } catch(exception){ } }); })