---
sidebar_position: 9
sidebar_label: Synchronization
description: Synchronize Pathling server with other FHIR servers using subscriptions or bulk data export.
---

# Synchronization with other FHIR servers

## FHIR subscriptions

If the other FHIR server
supports [FHIR subscriptions](https://www.hl7.org/fhir/subscription.html), then
you can configure it to send resource updates to Pathling.

The first task is to enable subscription support, which may be disabled by
default. This process will vary depending on the FHIR server implementation.

Subscriptions can be enabled within
the [HAPI FHIR JPA server](https://github.com/hapifhir/hapi-fhir-jpaserver-starter)
by setting the `hapi.fhir.subscription.resthook_enabled` property to `true`.

To create a subscription, you can send a `POST` request to
the `/fhir/Subscription` endpoint on the source server. The following example
shows how to instruct the server to send all updates regarding
the `Questionnaire` resource.

```json
{
    "resourceType": "Subscription",
    "channel": {
        "type": "rest-hook",
        "endpoint": "https://[your Pathling server]/fhir",
        "payload": "application/fhir+json"
    },
    "status": "requested",
    "criteria": "Questionnaire?"
}
```

Whenever a resource matching the criteria is created or updated, the source
server will invoke the [update](/docs/7.2.0/server/operations/update) operation
on
your Pathling server.

## FHIR bulk data export

Another method of synchronizing with another FHIR server is to use the FHIR bulk
data export API. If the source server supports this, it can provide a way of
exporting the full contents of the server, or changes since a certain time, in
NDJSON format. This can then be imported into Pathling using
the [import](/docs/7.2.0/server/operations/import) operation.

A library has been created to assist with the task of exporting from another
FHIR server and importing into Pathling -
see [pathling-import](/docs/7.2.0/libraries/javascript/pathling-import) for more
details.
