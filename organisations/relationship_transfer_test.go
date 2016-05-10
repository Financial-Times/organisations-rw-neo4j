package organisations

import (
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

const TEST_RELATIONSHIP_LEFT_TO_RIGHT = "TEST_RELATIONSHIP_1"
const TEST_RELATIONSHIP_RIGHT_TO_LEFT = "TEST_RELATIONSHIP_2"

func TestConstructTransferRelationshipsFromNodeQuery(t *testing.T) {
	var tests = []struct {
		fromUUID         string
		toUUID           string
		predicate        string
		constructedQuery *neoism.CypherQuery
	}{
		{
			"3d91a94c-6ce6-4ec9-a16b-8b89be574ecc",
			"ecd7319d-92f1-3c0a-9912-0b91186bf555",
			TEST_RELATIONSHIP_LEFT_TO_RIGHT,
			&neoism.CypherQuery{
				Statement: `MATCH (oldNode:Thing {uuid:{fromUUID}})
				MATCH (newNode:Thing {uuid:{toUUID}})
				MATCH (oldNode)-[oldRel:`+TEST_RELATIONSHIP_LEFT_TO_RIGHT+`]->(p)
				MERGE (newNode)-[newRel:`+TEST_RELATIONSHIP_LEFT_TO_RIGHT+`]->(p)
				SET newRel = oldRel
				DELETE oldRel`,
				Parameters: map[string]interface{}{
					"fromUUID": "3d91a94c-6ce6-4ec9-a16b-8b89be574ecc",
					"toUUID":   "ecd7319d-92f1-3c0a-9912-0b91186bf555",
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
			"3d91a94c-6ce6-4ec9-a16b-8b89be574ecc",
			"ecd7319d-92f1-3c0a-9912-0b91186bf555",
			TEST_RELATIONSHIP_RIGHT_TO_LEFT,
			&neoism.CypherQuery{
				Statement: `MATCH (oldNode:Thing {uuid:{fromUUID}})
				MATCH (newNode:Thing {uuid:{toUUID}})
				MATCH (oldNode)<-[oldRel:`+TEST_RELATIONSHIP_RIGHT_TO_LEFT+`]-(p)
				MERGE (newNode)<-[newRel:`+TEST_RELATIONSHIP_RIGHT_TO_LEFT+`]-(p)
				SET newRel = oldRel
				DELETE oldRel`,
				Parameters: map[string]interface{}{
					"fromUUID": "3d91a94c-6ce6-4ec9-a16b-8b89be574ecc",
					"toUUID":   "ecd7319d-92f1-3c0a-9912-0b91186bf555",
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
	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	addMentionsQuery := &neoism.CypherQuery{
		Statement: `MATCH (c:Thing{uuid:{uuid}})
			    CREATE (co:Content{uuid:{cuuid}})
			    CREATE (co)-[:`+TEST_RELATIONSHIP_LEFT_TO_RIGHT+`{someProperty:"someValue"}]->(c)
			    CREATE (co)<-[:`+TEST_RELATIONSHIP_RIGHT_TO_LEFT+`]-(c)`,
		Parameters: map[string]interface{}{
			"cuuid": contentUUID,
			"uuid":  minimalOrgUUID,
		},
	}

	assert.NoError(cypherDriver.Write(minimalOrg))
	assert.NoError(cypherDriver.cypherRunner.CypherBatch([]*neoism.CypherQuery{addMentionsQuery}))

	relationshipsFromNodeWithUUID, relationshipsToNodeWithUUID, err := getNodeRelationshipNames(cypherDriver.cypherRunner, minimalOrgUUID)

	assert.NoError(err)
	assert.True(len(relationshipsFromNodeWithUUID) >= 1, "Expected -> relationship length differs from actual length")
	assert.True(len(relationshipsToNodeWithUUID) >= 1, "Expected <- relationship length differs from actual length")

	assert.True(contains(relationshipsFromNodeWithUUID, TEST_RELATIONSHIP_RIGHT_TO_LEFT))
	assert.True(contains(relationshipsToNodeWithUUID, TEST_RELATIONSHIP_LEFT_TO_RIGHT))
}

func TestTransferRelationships(t *testing.T) {
	assert := assert.New(t)
	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	addMentionsQuery := &neoism.CypherQuery{
		Statement: `MATCH (c:Thing{uuid:{uuid}})
			    CREATE (co:Content{uuid:{cuuid}})
			    CREATE (co)-[:`+TEST_RELATIONSHIP_LEFT_TO_RIGHT+`{someProperty:"someValue"}]->(c)
			    CREATE (co)<-[:`+TEST_RELATIONSHIP_RIGHT_TO_LEFT+`]-(c)`,
		Parameters: map[string]interface{}{
			"cuuid": contentUUID,
			"uuid":  minimalOrgUUID,
		},
	}
	assert.NoError(cypherDriver.Write(minimalOrg))
	assert.NoError(cypherDriver.cypherRunner.CypherBatch([]*neoism.CypherQuery{addMentionsQuery}))

	//write new node and test that it doesn't yet have the relationships
	assert.NoError(cypherDriver.Write(fullOrg))
	relationshipsFromNewNode, relationshipsToNewNode, err := getNodeRelationshipNames(cypherDriver.cypherRunner, fullOrgUUID)
	assert.NoError(err)
	assert.False(contains(relationshipsFromNewNode, TEST_RELATIONSHIP_RIGHT_TO_LEFT))
	assert.False(contains(relationshipsToNewNode, TEST_RELATIONSHIP_RIGHT_TO_LEFT))

	//transfer relationships from the one above to the on other uuid
	transferQuery, err := TransferRelationships(cypherDriver.cypherRunner, fullOrgUUID, minimalOrgUUID)
	assert.NoError(err)
	assert.NoError(cypherDriver.cypherRunner.CypherBatch(transferQuery))

	//verify that the relationships has been transferred
	relationshipsFromOldNode, relationshipsToOldNode, err := getNodeRelationshipNames(cypherDriver.cypherRunner, minimalOrgUUID)
	assert.NoError(err)
	relationshipsFromNewNode, relationshipsToNewNode, err = getNodeRelationshipNames(cypherDriver.cypherRunner, fullOrgUUID)
	assert.NoError(err)

	//no relationships for the old node
	assert.Equal(0, len(relationshipsFromOldNode))
	assert.Equal(0, len(relationshipsToOldNode))

	//new relationships for the new node
	assert.True(contains(relationshipsFromNewNode, TEST_RELATIONSHIP_RIGHT_TO_LEFT))
	assert.True(contains(relationshipsToNewNode, TEST_RELATIONSHIP_LEFT_TO_RIGHT))

	//verify that properties has been trasnferred
	type property []struct {
		Value string `json:"r.someProperty"`
	}

	transferredProperty := property{}
	readRelationshipPropertyQuery := &neoism.CypherQuery{
		Statement: `match (co:Content{uuid:{cuuid}})-[r:`+TEST_RELATIONSHIP_LEFT_TO_RIGHT+`]->(c:Thing{uuid:{uuid}})
 				return r.someProperty`,
		Parameters: map[string]interface{}{
			"cuuid": contentUUID,
			"uuid":  fullOrgUUID,
		},
		Result: &transferredProperty,
	}
	assert.NoError(cypherDriver.cypherRunner.CypherBatch([]*neoism.CypherQuery{readRelationshipPropertyQuery}))
	assert.Equal(1, len(transferredProperty))
	assert.Equal("someValue", transferredProperty[0].Value)
}

func contains(rels relationships, rel string) bool {
	for _, foundRel := range rels {
		if foundRel.RelationshipType == rel {
			return true
		}
	}
	return false
}
