package organisations

import (
	"fmt"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func getDatabaseConnection(assert *assert.Assertions) neoutils.NeoConnection {
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}

	conf := neoutils.DefaultConnectionConfig()
	conf.Transactional = false
	db, err := neoutils.Connect(url, conf)
	assert.NoError(err, "Failed to connect to Neo4j")
	return db
}

func getCypherDriver(db neoutils.NeoConnection) service {
	cr := NewCypherOrganisationService(db)
	cr.Initialise()
	return cr
}

func getDatabaseConnectionAndCheckClean(t *testing.T, assert *assert.Assertions, uuidsToClean []string) neoutils.NeoConnection {
	db := getDatabaseConnection(assert)
	cleanDB(db, t, assert, uuidsToClean)
	checkDbClean(db, t, uuidsToClean)
	return db
}

func checkDbClean(db neoutils.CypherRunner, t *testing.T, uuidsToClean []string) {
	assert := assert.New(t)

	result := []struct {
		UUID string `json:"org.uuid"`
	}{}

	checkGraph := neoism.CypherQuery{
		Statement: `
			MATCH (org:Thing) WHERE org.uuid in {uuids} RETURN org.uuid
		`,
		Parameters: neoism.Props{
			"uuids": uuidsToClean,
		},
		Result: &result,
	}
	err := db.CypherBatch([]*neoism.CypherQuery{&checkGraph})
	assert.NoError(err)
	assert.Empty(result)
}

func cleanDB(db neoutils.CypherRunner, t *testing.T, assert *assert.Assertions, uuidsToClean []string) {
	qs := []*neoism.CypherQuery{}

	for _, uuid := range uuidsToClean {
		qs = append(qs, &neoism.CypherQuery{Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'})<-[:IDENTIFIES*0..]-(i:Identifier) DETACH DELETE org, i", uuid)})
		qs = append(qs, &neoism.CypherQuery{Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'}) DETACH DELETE org", uuid)})
	}

	err := db.CypherBatch(qs)
	assert.NoError(err)
}

func contains(rels relationships, rel string) bool {
	for _, foundRel := range rels {
		if foundRel.RelationshipType == rel {
			return true
		}
	}
	return false
}
