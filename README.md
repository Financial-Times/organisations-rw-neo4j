# Roles Reader/Writer for Neo4j (roles-rw-neo4j)

__An API for reading/writing roles into Neo4j. Expects the roles json supplied to be in the format that comes out of the roles transformer.__

## Installation

For the first time:

`go get github.com/Financial-Times/roles-rw-neo4j`

or update:

`go get -u github.com/Financial-Times/roles-rw-neo4j`

## Running

`$GOPATH/bin/roles-rw-neo4j --neo-url={neo4jUrl} --port={port} --batchSize=50 --graphiteTCPAddress=graphite.ft.com:2003 --graphitePrefix=content.{env}.roles.rw.neo4j.{hostname} --logMetrics=false

All arguments are optional, they default to a local Neo4j install on the default port (7474), application running on port 8080, batchSize of 1024, graphiteTCPAddress of "" (meaning metrics won't be written to Graphite), graphitePrefix of "" and logMetrics false.

NB: the default batchSize is much higher than the throughput the instance data ingester currently can cope with.

## Updating the model
We use the transformer to get the information to write and from that we establish the json for the request. This representation is held in the model.go in a struct called role.

Use gojson against a transformer endpoint to create a role struct and update the role/model.go file. NB: we DO need a separate identifier struct

Please note that the transformer for roles is actually within the membership transformer. Currently there are only approx 50 roles and this is a controlled list e.g CEO

`curl http://ftaps50683-law1a-eu-p:8080/transformers/roles/344fdb1d-0585-31f7-814f-b478e54dbe1f | gojson -name=person`

## Building

This service is built and deployed via Jenkins.

<a href="http://ftjen10085-lvpr-uk-p:8181/view/JOBS-roles-rw-neo4j/job/roles-rw-neo4j-build/">Build job</a>
<a href="http://ftjen10085-lvpr-uk-p:8181/view/JOBS-roles-rw-neo4j/job/roles-rw-neo4j-deploy-test/">Deploy job to Test</a>
<a href="http://ftjen10085-lvpr-uk-p:8181/view/JOBS-roles-rw-neo4j/job/roles-rw-neo4j-deploy-prod/">Deploy job to Prod</a>

The build works via git tags. To prepare a new release
- update the version in /puppet/ft-roles_rw_neo4j/Modulefile, e.g. to 0.0.12
- git tag that commit using `git tag 0.0.12`
- `git push --tags`

The deploy also works via git tag and you can also select the environment to deploy to.

## Endpoints
/roles/{uuid}
### PUT
The only mandatory fields are the uuid, isBoardRole, the prefLabel and the alternativeIdentifier uuids (because the uuid is also listed in the alternativeIdentifier uuids list), and the uuid in the body must match the one used on the path.

Every request results in an attempt to update that person: unlike with GraphDB there is no check on whether the role already exists and whether there are any changes between what's there and what's being written. We just do a MERGE which is Neo4j for create if not there, update if it is there.

A successful PUT results in 200.

We run queries in batches. If a batch fails, all failing requests will get a 500 server error response.

Invalid json body input, or uuids that don't match between the path and the body will result in a 400 bad request response.

Example:
`curl -XPUT -H "X-Request-Id: 123" -H "Content-Type: application/json" localhost:8080/roles/3fa70485-3a57-3b9b-9449-774b001cd965 --data '{"uuid": "3fa70485-3a57-3b9b-9449-774b001cd965", "isBoardRole": true, "prefLabel": "Director/Board Member", "alternativeIdentifiers":{"uuids": ["3fa70485-3a57-3b9b-9449-774b001cd965","6a2a0170-6afa-4bcc-b427-430268d2ac50"], "factsetIdentifier":"BRD"},"type":"Role"}'`

The type field is not currently validated - instead, the Roles Writer writes type Role as label for the Role.

### GET
Thie internal read should return what got written (i.e., there isn't a public read for roles and this is not intended to ever be public either)

If not found, you'll get a 404 response.

Empty fields are omitted from the response.
`curl -H "X-Request-Id: 123" localhost:8080/roles/344fdb1d-0585-31f7-814f-b478e54dbe1f`

### DELETE
Will return 204 if successful, 404 if not found
`curl -XDELETE -H "X-Request-Id: 123" localhost:8080/roles/344fdb1d-0585-31f7-814f-b478e54dbe1f`

### Admin endpoints
Healthchecks: [http://localhost:8080/__health](http://localhost:8080/__health)

Ping: [http://localhost:8080/ping](http://localhost:8080/ping) or [http://localhost:8080/__ping](http://localhost:8080/__ping)

### Logging
 the application uses logrus, the logfile is initilaised in main.go.
 logging requires an env app parameter, for all enviromets  other than local logs are written to file
 when running locally logging is written to console (if you want to log locally to file you need to pass in an env parameter that is != local)
 
