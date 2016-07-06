package organisations

import (
	"fmt"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

const (
	testRelationshipLeftToRight     = "TEST_RELATIONSHIP_1"
	testRelationshipRightToLeft     = "TEST_RELATIONSHIP_2"
	relationShipTransferContentUUID = "d3dbe29e-5f6f-456f-a245-9c4d70846e11"
	transferOrg1UUID                = "10c547d2-6383-41e1-9430-2f543321587f"
	transferOrg2UUID                = "3977bc1c-1026-45f0-b7db-d91ff25770fb"
	fromUUID                        = "3d91a94c-6ce6-4ec9-a16b-8b89be574ecc"
	toUUID                          = "ecd7319d-92f1-3c0a-9912-0b91186bf555"
	fsTransferOrg1Identifier        = "org identifier 1"
	fsTransferOrg2Identifier        = "org identifier 2"
)

var transferOrg1 = organisation{
	UUID: transferOrg1UUID,
	Type: Organisation,
	AlternativeIdentifiers: alternativeIdentifiers{
		FactsetIdentifier: fsTransferOrg1Identifier,
		UUIDS:             []string{transferOrg1UUID},
		TME:               []string{},
	},
	ProperName: "Org Proper Name 1",
}

var transferOrg2 = organisation{
	UUID: transferOrg2UUID,
	Type: Organisation,
	AlternativeIdentifiers: alternativeIdentifiers{
		FactsetIdentifier: fsTransferOrg2Identifier,
		UUIDS:             []string{transferOrg2UUID},
		TME:               []string{},
	},
	ProperName: "Org Proper Name 2",
}

var transferUUIDsToClean = []string{relationShipTransferContentUUID, transferOrg1UUID, transferOrg2UUID}

func TestConstructTransferRelationshipsFromNodeQuery(t *testing.T) {
	var tests = []struct {
		fromUUID         string
		toUUID           string
		predicate        string
		constructedQuery *neoism.CypherQuery
	}{
		{
			fromUUID,
			toUUID,
			testRelationshipLeftToRight,
			&neoism.CypherQuery{
				Statement: `MATCH (oldNode:Thing {uuid:{fromUUID}})
				MATCH (newNode:Thing {uuid:{toUUID}})
				MATCH (oldNode)-[oldRel:` + testRelationshipLeftToRight + `]->(p)
				FOREACH (ignoreMe IN CASE WHEN (EXISTS (oldRel.platformVersion)) THEN [1] ELSE [] END |
					MERGE (newNode)-[newRel:` + testRelationshipLeftToRight + `{platformVersion:oldRel.platformVersion}]->(p)
					SET newRel = oldRel
				)
				FOREACH (ignoreMe IN CASE WHEN NOT (EXISTS (oldRel.platformVersion)) THEN [1] ELSE [] END |
					MERGE (newNode)-[newRel:` + testRelationshipLeftToRight + `]->(p)
					SET newRel = oldRel
				)
				DELETE oldRel`,
				Parameters: map[string]interface{}{
					"fromUUID": fromUUID,
					"toUUID":   toUUID,
				},
			},
		},
	}

	for _, test := range tests {
		resultingQuery := constructTransferRelationshipsFromNodeQuery(test.fromUUID, test.toUUID, test.predicate)
		if strings.Replace(resultingQuery.Statement, "\t", "", -1) != strings.Replace(test.constructedQuery.Statement, "\t", "", -1) {
			t.Errorf("Expected statement: msgs: %v \nActual statement: msgs: %v.",
				test.constructedQuery.Statement, resultingQuery.Statement)
		}
		for key, value := range test.constructedQuery.Parameters {
			if resultingQuery.Parameters[key] != value {
				t.Errorf("Expected parameter %s with value: %s, but found %s.",
					key, value, resultingQuery.Parameters[key])
			}
		}
	}
}

func TestConstructTransferRelationshipsToNodeQuery(t *testing.T) {
	var tests = []struct {
		fromUUID         string
		toUUID           string
		predicate        string
		constructedQuery *neoism.CypherQuery
	}{
		{
			fromUUID,
			toUUID,
			testRelationshipRightToLeft,
			&neoism.CypherQuery{
				Statement: `MATCH (oldNode:Thing {uuid:{fromUUID}})
				MATCH (newNode:Thing {uuid:{toUUID}})
				MATCH (oldNode)<-[oldRel:` + testRelationshipRightToLeft + `]-(p)
				FOREACH (ignoreMe IN CASE WHEN (EXISTS (oldRel.platformVersion)) THEN [1] ELSE [] END |
					MERGE (newNode)<-[newRel:` + testRelationshipRightToLeft + `{platformVersion:oldRel.platformVersion}]-(p)
					SET newRel = oldRel
				)
				FOREACH (ignoreMe IN CASE WHEN NOT (EXISTS (oldRel.platformVersion)) THEN [1] ELSE [] END |
					MERGE (newNode)<-[newRel:` + testRelationshipRightToLeft + `]-(p)
					SET newRel = oldRel
				)
				DELETE oldRel`,
				Parameters: map[string]interface{}{
					"fromUUID": fromUUID,
					"toUUID":   toUUID,
				},
			},
		},
	}

	for _, test := range tests {
		resultingQuery := constructTransferRelationshipsToNodeQuery(test.fromUUID, test.toUUID, test.predicate)
		if strings.Replace(resultingQuery.Statement, "\t", "", -1) != strings.Replace(test.constructedQuery.Statement, "\t", "", -1) {
			t.Errorf("Expected statement: msgs: %v \nActual statement: msgs: %v.",
				test.constructedQuery.Statement, resultingQuery.Statement)
		}
		for key, value := range test.constructedQuery.Parameters {
			if resultingQuery.Parameters[key] != value {
				t.Errorf("Expected parameter %s with value: %s, but found %s.",
					key, value, resultingQuery.Parameters[key])
			}
		}
	}
}

func TestGetNodeRelationshipNames(t *testing.T) {
	assert := assert.New(t)
	db := getDatabaseConnectionAndCheckClean(t, assert, transferUUIDsToClean)
	cypherDriver := getCypherDriver(db)
	defer cleanRelationshipDB(db, t, assert, transferUUIDsToClean)

	addMentionsQuery := &neoism.CypherQuery{
		Statement: `MATCH (c:Thing{uuid:{uuid}})
			    CREATE (co:Content{uuid:{cuuid}})
			    CREATE (co)-[:` + testRelationshipLeftToRight + `{someProperty:"someValue"}]->(c)
			    CREATE (co)<-[:` + testRelationshipRightToLeft + `]-(c)`,
		Parameters: map[string]interface{}{
			"cuuid": relationShipTransferContentUUID,
			"uuid":  transferOrg1UUID,
		},
	}

	assert.NoError(cypherDriver.Write(transferOrg1))
	assert.NoError(cypherDriver.cypherRunner.CypherBatch([]*neoism.CypherQuery{addMentionsQuery}))

	relationshipsFromNodeWithUUID, relationshipsToNodeWithUUID, err := getNodeRelationshipNames(cypherDriver.cypherRunner, transferOrg1UUID)

	assert.NoError(err)
	assert.True(len(relationshipsFromNodeWithUUID) >= 1, "Expected -> relationship length differs from actual length")
	assert.True(len(relationshipsToNodeWithUUID) >= 1, "Expected <- relationship length differs from actual length")

	assert.True(contains(relationshipsFromNodeWithUUID, testRelationshipRightToLeft))
	assert.True(contains(relationshipsToNodeWithUUID, testRelationshipLeftToRight))
}

func TestTransferRelationships(t *testing.T) {
	assert := assert.New(t)
	db := getDatabaseConnectionAndCheckClean(t, assert, transferUUIDsToClean)
	cypherDriver := getCypherDriver(db)
	defer cleanRelationshipDB(db, t, assert, transferUUIDsToClean)

	addMentionsQuery := &neoism.CypherQuery{
		Statement: `MATCH (c:Thing{uuid:{uuid}})
			    CREATE (co:Content{uuid:{cuuid}})
			    CREATE (co)-[:` + testRelationshipLeftToRight + `{someProperty:"someValue"}]->(c)
			    CREATE (co)<-[:` + testRelationshipRightToLeft + `]-(c)`,
		Parameters: map[string]interface{}{
			"cuuid": relationShipTransferContentUUID,
			"uuid":  transferOrg1UUID,
		},
	}
	assert.NoError(cypherDriver.Write(transferOrg1))
	assert.NoError(cypherDriver.cypherRunner.CypherBatch([]*neoism.CypherQuery{addMentionsQuery}))

	//write new node and test that it doesn't yet have the relationships
	assert.NoError(cypherDriver.Write(transferOrg2))
	relationshipsFromNewNode, relationshipsToNewNode, err := getNodeRelationshipNames(cypherDriver.cypherRunner, transferOrg2UUID)
	assert.NoError(err)
	assert.False(contains(relationshipsFromNewNode, testRelationshipRightToLeft))
	assert.False(contains(relationshipsToNewNode, testRelationshipRightToLeft))

	//transfer relationships from the one above to the on other uuid
	transferQuery, err := CreateTransferRelationshipsQueries(cypherDriver.cypherRunner, transferOrg2UUID, transferOrg1UUID)
	assert.NoError(err)
	assert.NoError(cypherDriver.cypherRunner.CypherBatch(transferQuery))

	//verify that the relationships has been transferred
	relationshipsFromOldNode, relationshipsToOldNode, err := getNodeRelationshipNames(cypherDriver.cypherRunner, transferOrg1UUID)
	assert.NoError(err)
	relationshipsFromNewNode, relationshipsToNewNode, err = getNodeRelationshipNames(cypherDriver.cypherRunner, transferOrg2UUID)
	assert.NoError(err)

	//no relationships for the old node
	assert.Equal(0, len(relationshipsFromOldNode))
	assert.Equal(0, len(relationshipsToOldNode))

	//new relationships for the new node
	assert.True(contains(relationshipsFromNewNode, testRelationshipRightToLeft))
	assert.True(contains(relationshipsToNewNode, testRelationshipLeftToRight))

	//verify that properties has been transferred
	type property []struct {
		Value string `json:"r.someProperty"`
	}

	transferredProperty := property{}
	readRelationshipPropertyQuery := &neoism.CypherQuery{
		Statement: `match (co:Content{uuid:{cuuid}})-[r:` + testRelationshipLeftToRight + `]->(c:Thing{uuid:{uuid}})
 				return r.someProperty`,
		Parameters: map[string]interface{}{
			"cuuid": relationShipTransferContentUUID,
			"uuid":  transferOrg2UUID,
		},
		Result: &transferredProperty,
	}
	assert.NoError(cypherDriver.cypherRunner.CypherBatch([]*neoism.CypherQuery{readRelationshipPropertyQuery}))
	assert.Equal(1, len(transferredProperty))
	assert.Equal("someValue", transferredProperty[0].Value)
}

func cleanRelationshipDB(db *neoism.Database, t *testing.T, assert *assert.Assertions, uuidsToClean []string) {
	cleanDB(db, t, assert, uuidsToClean)

	qs := []*neoism.CypherQuery{
		{
			Statement: fmt.Sprintf("MATCH (c:Content {uuid: '%v'})-[rel]-(o) DELETE c, rel ", relationShipTransferContentUUID),
		},
		{
			Statement: fmt.Sprintf("MATCH (c:Content {uuid: '%v'}) DELETE c ", relationShipTransferContentUUID),
		},
	}

	err := db.CypherBatch(qs)
	assert.NoError(err)
}
