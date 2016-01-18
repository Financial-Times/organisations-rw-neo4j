# Organisations Reader/Writer for Neo4j (organisations-rw-neo4j)

__An API for reading/writing organisations into Neo4j. Expects the organisations json supplied to be in the format that comes out of the organisations transformer.__

## Installation

For the first time:

`go get github.com/Financial-Times/organisations-rw-neo4j`

or update:

`go get -u github.com/Financial-Times/organisations-rw-neo4j`

## Running

`$GOPATH/bin/organisations-rw-neo4j --neo-url={neo4jUrl} --port={port} --batchSize=50 --graphiteTCPAddress=graphite.ft.com:2003 --graphitePrefix=content.{env}.organisations.rw.neo4j.{hostname} --logMetrics=false

All arguments are optional, they default to a local Neo4j install on the default port (7474), application running on port 8080, batchSize of 1024, graphiteTCPAddress of "" (meaning metrics won't be written to Graphite), graphitePrefix of "" and logMetrics false.

NB: the default batchSize is much higher than the throughput the instance data ingester currently can cope with.

## Updating the model
Use gojson against a transformer endpoint to create a role struct and update the role/model.go file. NB: we DO need a separate identifier struct

`curl http://<TODO>:8080/transformers/organisations/344fdb1d-0585-31f7-814f-b478e54dbe1f | gojson -name=organisation`

## Building

This service is built and deployed via Jenkins.

<a href="<TODO>">Build job</a>
<a href="<TODO>">Deploy job</a>

The build works via git tags. To prepare a new release
- update the version in /puppet/ft-organisations_rw_neo4j/Modulefile, e.g. to 0.0.12
- git tag that commit using `git tag 0.0.12`
- `git push --tags`

The deploy also works via git tag and you can also select the environment to deploy to.

## Endpoints
/people/{uuid}
### PUT
The only mandatory field is the uuid, and the uuid in the body must match the one used on the path.

Every request results in an attempt to update that organisation: unlike with GraphDB there is no check on whether the role already exists and whether there are any changes between what's there and what's being written. We just do a MERGE which is Neo4j for create if not there, update if it is there.

A successful PUT results in 200.

We run queries in batches. If a batch fails, all failing requests will get a 500 server error response.

Invalid json body input, or uuids that don't match between the path and the body will result in a 400 bad request response.

Example:
`curl -XPUT -H "X-Request-Id: 123" -H "Content-Type: application/json" localhost:8080/organisations/3fa70485-3a57-3b9b-9449-774b001cd965 --data '{"uuid": "344fdb1d-0585-31f7-814f-b478e54dbe1f", "isBoardRole": true, "prefLabel": "Director/Board Member", "identifiers": [{"authority": "http://api.ft.com/system/FACTSET","identifierValue": "BRD"}]}'`

### GET
Thie internal read should return what got written (i.e., there isn't a public read for organisations and this is not intended to ever be public either)

If not found, you'll get a 404 response.

Empty fields are omitted from the response.
`curl -H "X-Request-Id: 123" localhost:8080/organisations/344fdb1d-0585-31f7-814f-b478e54dbe1f`

### DELETE
Will return 204 if successful, 404 if not found
`curl -XDELETE -H "X-Request-Id: 123" localhost:8080/organisations/344fdb1d-0585-31f7-814f-b478e54dbe1f`

### Admin endpoints
Healthchecks: [http://localhost:8080/__health](http://localhost:8080/__health)

Ping: [http://localhost:8080/ping](http://localhost:8080/ping) or [http://localhost:8080/__ping](http://localhost:8080/__ping)
