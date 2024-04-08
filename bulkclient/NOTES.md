Notes
-----



### References on Bulk Export
 
- Bulk Export Specification: https://build.fhir.org/ig/HL7/bulk-data/export.html
- Bulk Data Demo Server: https://bulk-data.smarthealthit.org/
- Bulk Data Client JS (https://github.com/smart-on-fhir/bulk-data-client)



### Authentication

- Backend Services: https://www.hl7.org/fhir/smart-app-launch/backend-services.html
- Asymetric authentication: https://www.hl7.org/fhir/smart-app-launch/client-confidential-asymmetric.html#authenticating-to-the-token-endpoint
- STU3 js-downloaded: https://github.com/smart-on-fhir/sample-apps-stu3/tree/master/fhir-downloader
- jwt.io: https://jwt.io/
- Bulk Data Client: https://github.com/smart-on-fhir/bulk-data-client/
- Java Key Store: https://www.baeldung.com/java-keystore
- JWKS Java: https://medium.com/trabe/validate-jwt-tokens-using-jwks-in-java-214f7014b5cf
- JWT, JWK, JWKS: https://auth0.com/docs/secure/tokens/json-web-tokens/json-web-key-sets
- java.jwt: https://github.com/auth0/java-jwt
- Sign and valdate: https://github.com/stevenschwenke/JWTWithJava/blob/main/src/test/java/JWTExperiments.java
- Read PKCS: https://gist.github.com/simon04/b30c8400f09648b794c25a1f3f1edb32
- Java JWK: https://www.javadoc.io/doc/com.nimbusds/nimbus-jose-jwt/latest/com/nimbusds/jose/jwk/AsymmetricJWK.html
- HTTP Client OAuth example: https://github.com/mojodna/httpclient-oauth/blob/master/src/examples/org/apache/http/examples/client/ClientOAuthAuthentication.java


### Fhir Servers to try

- HAPI: https://hapi.fhir.org/ (Open endpoint: http://hapi.fhir.org/baseR4)  
- Cerner: https://code-console.cerner.com/  (smart-services-auth)  endpoint is: https://fhir-ehr-code.cerner.com/r4/8c040c3c-898b-44ce-9c8a-7840e8793050
- firely.io (TBD)
- Bulk Data Demo Server: https://bulk-data.smarthealthit.org/



#### Cerner resources

Documentation: https://fhir.cerner.com/millennium/bulk-data/#authorization
Endpoint URL: https://fhir-ehr-code.cerner.com/r4/a658a26c-c581-488c-b301-370b5a0776f7
token_endpoint: https://authorization.cerner.com/tenants/a658a26c-c581-488c-b301-370b5a0776f7/protocols/oauth2/profiles/smart-v1/token


    AllergyIntolerance
    Binary
    CarePlan
    CareTeam
    Condition
    Device
    DiagnosticReport
    DocumentReference
    Encounter
    Goal
    Immunization
    Location
    MedicationRequest
    Observation
    Organization
    Patient
    Practitioner
    Procedure
    Provenance


Sandbox:

https://fhir-ehr-code.cerner.com/r4/ec2458f2-1e24-41c8-b71b-0e701af7583d




In order to call bulk data endpoints in sandbox, you will need to kick off a group export using one of the group ids listed below and then perform the subsequent requests.

Group with 3 patients: 11ec-d16a-c763b73e-98e8-a31715e6a2bf
Group with 10 patients: 11ec-d16a-b40370f8-9d31-577f11a339c5
Example
GET https://fhir-ehr-code.cerner.com/r4/ec2458f2-1e24-41c8-b71b-0e701af7583d/Group/11ec-d16a-c763b73e-98e8-a31715e6a2bf/$export?_type=Patient


Implemented Exports
Cerner supports only the Group Export operation.

Group Export
