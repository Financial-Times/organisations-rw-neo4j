*DECOMISSIONED*
See [Concepts RW Neo4j](https://github.com/Financial-Times/concepts-rw-neo4j) instead

# Organisations Reader/Writer for Neo4j (organisations-rw-neo4j)
[![Circle CI](https://circleci.com/gh/Financial-Times/organisations-rw-neo4j.svg?style=shield)](https://circleci.com/gh/Financial-Times/organisations-rw-neo4j)[![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/organisations-rw-neo4j)](https://goreportcard.com/report/github.com/Financial-Times/organisations-rw-neo4j) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/organisations-rw-neo4j/badge.svg)](https://coveralls.io/github/Financial-Times/organisations-rw-neo4j)

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

We use the transformer to get the information to write and from that we establish the json for the request. This representation is held in the model.go in a struct called organisation.

Use gojson against a transformer endpoint to create a organisation struct and update the organisation/model.go file. NB: we DO need a separate identifier struct

`curl http://ftaps39403-law1a-eu-t:8080/transformers/organisations/344fdb1d-0585-31f7-814f-b478e54dbe1f | gojson -name=organisation`

## Endpoints
/organisations/{uuid}

### PUT
The only mandatory field is the uuid, and the uuid in the body must match the one used on the path.

Every request results in an attempt to update that organisation: unlike with GraphDB there is no check on whether the role already exists and whether there are any changes between what's there and what's being written. We just do a MERGE which is Neo4j for create if not there, update if it is there.

A successful PUT results in 200.

We run queries in batches. If a batch fails, all failing requests will get a 500 server error response.

Invalid json body input, or uuids that don't match between the path and the body will result in a 400 bad request response.

Example1: `curl -XPUT -H "X-Request-Id: 123" -H "Content-Type: application/json" localhost:8080/organisations/3fa70485-3a57-3b9b-9449-774b001cd965 --data '{"uuid": "3fa70485-3a57-3b9b-9449-774b001cd965", "type": "PublicCompany", "properName": "The E.W. Scripps Co.","prefLabel": "EW Scripps", "legalName": "The E. W. Scripps Company", "shortName": "The EW Scripps", "hiddenLabel": "EW SCRIPPS CO", "alternativeIdentifiers": { "leiCode": "549300U1OW41QPKYW028", "uuids":["3fa70485-3a57-3b9b-9449-774b001cd965"], "factsetIdentifier":"FACTSET ID", "TME":["tme1","tme2"]}, "aliases": [ "EW Scripps Company", "E.W. Scripps", "Scripps", "EW Scripps Co", "E.W. Scripps Company", "Scripps Company", "EW Scripps", "The E.W. Scripps Company", "Scripps EW", "E.W. Scripps Co" ], "industryClassification": "3c980022-6253-324d-ba9f-abfb71e39bf3", "parentOrganisation":"parentOrgUUID" }'`

  Note: inserting the above organisation results in:       
    - writing an organisation node with the above properties and relationships in neo4j (normal behaviour)       
    - writing the IDENTIFIES relationship for:           
        * all the above mentioned identifier nodes           
        * IMPORTANT: an identifier node corresponding to the organisation itself ("3fa70485-3a57-3b9b-9449-774b001cd965") should be present in the alternativeIdentifiers.uuids list     

 Example2:   `curl -XPUT -H "X-Request-Id: 123" -H "Content-Type: application/json" localhost:8080/organisations/3fa70485-3a57-3b9b-9449-774b001cd965 --data '{"uuid": "3fa70485-3a57-3b9b-9449-774b001cd965", "type": "PublicCompany", "properName": "The E.W. Scripps Co.","prefLabel": "EW Scripps", "legalName": "The E. W. Scripps Company", "shortName": "The EW Scripps", "hiddenLabel": "EW SCRIPPS CO", "alternativeIdentifiers": { "leiCode": "549300U1OW41QPKYW028", "uuids":["3fa70485-3a57-3b9b-9449-774b001cd965","857cfe0f-82aa-429a-ab80-854c93e4111b"], "factsetIdentifier":"FACTSET ID", "TME":["tme1","tme2"]}, "aliases": [ "EW Scripps Company", "E.W. Scripps", "Scripps", "EW Scripps Co", "E.W. Scripps Company", "Scripps Company", "EW Scripps", "The E.W. Scripps Company", "Scripps EW", "E.W. Scripps Co" ], "industryClassification": "3c980022-6253-324d-ba9f-abfb71e39bf3", "parentOrganisation":"parentOrgUUID" }'`

Note: if there are identifiers alternativeIdentifiers.uuids list (other then the node's uuid itself):  
    - besides the props and relationships above, the organisation node corresponding to the identifier value (here the node with `857cfe0f-82aa-429a-ab80-854c93e4111b` - if exists) should be deleted, and all its relationships should be transferred to the newly created organisation (the one with canonical uuid, here: `3fa70485-3a57-3b9b-9449-774b001cd965`)  

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


### Logging
 the application uses logrus, the logfile is initialised in main.go.
 logging requires an env app parameter, for all environments  other than local logs are written to file
 when running locally logging is written to console (if you want to log locally to file you need to pass in an env parameter that is != local)

