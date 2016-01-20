package organisations

import (
	"os"
	"testing"

	"github.com/Financial-Times/neo-utils-go"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
)

const (
	fullOrgUuid    = "4e484678-cf47-4168-b844-6adb47f8eb58"
	minimalOrgUuid = "33f93f25-3301-417e-9b20-50b27d215617"
)

var fsIdentifier = identifier{
	Authority:       fsAuthority,
	IdentifierValue: "identifierValue",
}

var leiCodeIdentifier = identifier{
	Authority:       leiIdentifier,
	IdentifierValue: "leiCodeIdentifier",
}

var fullOrg = organisation{
	UUID:                   fullOrgUuid,
	Type:                   Organisation,
	Identifiers:            []identifier{fsIdentifier, leiCodeIdentifier},
	ProperName:             "Proper Name",
	LegalName:              "Legal Name",
	ShortName:              "Short Name",
	HiddenLabel:            "Hidden Label",
	FormerNames:            []string{"Old Name, inc.", "Older Name, inc."},
	TradeNames:             []string{"Old Trade Name, inc.", "Older Trade Name, inc."},
	LocalNames:             []string{"Oldé Name, inc.", "Tradé Name"},
	TmeLabels:              []string{"tmeLabel1", "tmeLabel2", "tmeLabel3"},
	ParentOrganisation:     "de38231e-e481-4958-b470-e124b2ef5a34",
	IndustryClassification: "c3d17865-f9d1-42f2-9ca2-4801cb5aacc0",
}

var minimalOrg = organisation{
	UUID:        minimalOrgUuid,
	Type:        Organisation,
	Identifiers: []identifier{fsIdentifier},
	ProperName:  "Proper Name",
}

func TestWrite(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnection(t)
	cleanDB(db, t)
	checkDbClean(db, t)
	cypherDriver := getCypherDriver(db)

	assert.NoError(cypherDriver.Write(fullOrg))
	cleanDB(db, t)
}

func TestRead(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnection(t)
	cleanDB(db, t)
	checkDbClean(db, t)
	cypherDriver := getCypherDriver(db)

	assert.NoError(cypherDriver.Write(fullOrg))

	storedOrg, found, err := cypherDriver.Read(fullOrgUuid)

	assert.NoError(err, "Error finding organisation for uuid %s", fullOrgUuid)
	assert.True(found, "Didn't find organisation for uuid %s", fullOrgUuid)
	// assert.Equal(fullOrg, storedOrg, "organisations should be the same")
}

// func TestDeleteWithRelationships(t *testing.T) {
// 	assert := assert.New(t)
//
// 	db := getDatabaseConnection(t)
// 	cleanDB(db, t)
// 	checkDbClean(db, t)
// 	cypherDriver := getCypherDriver(db)
//
// 	cypherDriver.Write(fullOrg)
// 	cypherDriver.Delete(fullOrgUuid)
//
// 	result := []struct {
// 		Uuid string `json:"t.uuid"`
// 	}{}
//
// 	getOrg := neoism.CypherQuery{
// 		Statement: `
// 			MATCH (t:Thing {uuid:"4e484678-cf47-4168-b844-6adb47f8eb58"}) RETURN t.uuid
// 			`,
// 		Result: &result,
// 	}
//
// 	err := db.Cypher(&getOrg)
// 	assert.NoError(err)
// 	assert.NotEmpty(result)
// 	cleanDB(db, t)
// }
//
// func TestDeleteNoRelationships(t *testing.T) {
// 	assert := assert.New(t)
//
// 	db := getDatabaseConnection(t)
// 	cleanDB(db, t)
// 	checkDbClean(db, t)
// 	cypherDriver := getCypherDriver(db)
//
// 	cypherDriver.Write(minimalOrg)
// 	cypherDriver.Delete(minimalOrgUuid)
//
// 	result := []struct {
// 		Uuid string `json:"t.uuid"`
// 	}{}
//
// 	getOrg := neoism.CypherQuery{
// 		Statement: `
// 			MATCH (t:Thing {uuid:"33f93f25-3301-417e-9b20-50b27d215617"}) RETURN t.uuid
// 			`,
// 		Result: &result,
// 	}
//
// 	err := db.Cypher(&getOrg)
// 	assert.NoError(err)
// 	// TODO: result should be empty
// 	assert.NotEmpty(result)
// 	cleanDB(db, t)
// }

func checkDbClean(db *neoism.Database, t *testing.T) {
	assert := assert.New(t)

	result := []struct {
		Uuid string `json:"org.uuid"`
	}{}

	checkGraph := neoism.CypherQuery{
		Statement: `
			MATCH (org:Thing {uuid: {uuid}}) RETURN org.uuid
		`,
		Parameters: map[string]interface{}{
			"uuid": "4e484678-cf47-4168-b844-6adb47f8eb58",
		},
		Result: &result,
	}
	err := db.Cypher(&checkGraph)
	assert.NoError(err)
	assert.Empty(result)
}

func getDatabaseConnection(t *testing.T) *neoism.Database {
	assert := assert.New(t)
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}

	db, err := neoism.Connect(url)
	assert.NoError(err, "Failed to connect to Neo4j")
	return db
}

func cleanDB(db *neoism.Database, t *testing.T) {
	assert := assert.New(t)

	deleteFullOrg := neoism.CypherQuery{
		Statement: `
		MATCH (forg:Thing {uuid: '4e484678-cf47-4168-b844-6adb47f8eb58'}) DETACH DELETE forg
	`}

	deleteMinimalOrg := neoism.CypherQuery{
		Statement: `
		MATCH (morg:Thing {uuid: '33f93f25-3301-417e-9b20-50b27d215617'}) DETACH DELETE morg
	`}

	deletePar := neoism.CypherQuery{
		Statement: `
		MATCH (p:Thing {uuid: 'de38231e-e481-4958-b470-e124b2ef5a34'}) DETACH DELETE p
	`}

	deleteInd := neoism.CypherQuery{
		Statement: `
		MATCH (ind:Thing {uuid: 'c3d17865-f9d1-42f2-9ca2-4801cb5aacc0'}) DETACH DELETE ind
	`}
	deletes := []*neoism.CypherQuery{&deleteFullOrg, &deleteMinimalOrg, &deletePar, &deleteInd}
	err := db.CypherBatch(deletes)
	assert.NoError(err)
}

func getCypherDriver(db *neoism.Database) CypherDriver {
	return NewCypherDriver(neoutils.StringerDb{db}, db)
}
