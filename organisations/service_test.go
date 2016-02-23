package organisations

import (
	"fmt"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
	"os"
	"reflect"
	"testing"
)

const (
	fullOrgUuid    = "4e484678-cf47-4168-b844-6adb47f8eb58"
	minimalOrgUuid = "33f93f25-3301-417e-9b20-50b27d215617"
	oddCharOrgUuid = "161403e2-074f-3c82-9328-0337e909ac8c"
)

var fsIdentifier = identifier{
	Authority:       fsAuthority,
	IdentifierValue: "identifierValue",
}

var leiCodeIdentifier = identifier{
	Authority:       leiAuthority,
	IdentifierValue: "leiCodeIdentifier",
}

var tmeIdentifier = identifier{
	Authority:       tmeAuthority,
	IdentifierValue: "tmeIdentifier",
}

var fullOrg = organisation{
	UUID: fullOrgUuid,
	Type: PublicCompany,
	//identifiers are in the expected read order
	Identifiers:            []identifier{fsIdentifier, tmeIdentifier, leiCodeIdentifier},
	ProperName:             "Proper Name",
	LegalName:              "Legal Name",
	ShortName:              "Short Name",
	HiddenLabel:            "Hidden Label",
	FormerNames:            []string{"Old Name, inc.", "Older Name, inc."},
	TradeNames:             []string{"Old Trade Name, inc.", "Older Trade Name, inc."},
	LocalNames:             []string{"Oldé Name, inc.", "Tradé Name"},
	Aliases:                []string{"alias1", "alias2", "alias3"},
	ParentOrganisation:     "de38231e-e481-4958-b470-e124b2ef5a34",
	IndustryClassification: "c3d17865-f9d1-42f2-9ca2-4801cb5aacc0",
}

var minimalOrg = organisation{
	UUID:        minimalOrgUuid,
	Type:        Organisation,
	Identifiers: []identifier{fsIdentifier},
	ProperName:  "Proper Name",
}

func TestWriteNewOrganisation(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	assert.NoError(cypherDriver.Write(fullOrg))

	storedOrg, _, err := cypherDriver.Read(fullOrgUuid)

	assert.NoError(err)
	assert.NotEmpty(storedOrg)
}

func TestWriteNewOrganisationAuthorityNotSupported(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	var unsupporterIdentifier = identifier{
		Authority:       "unsupported",
		IdentifierValue: "leiCodeIdentifier",
	}
	var testOrg = organisation{
		UUID:        "3166b06b-a7a7-40f7-bcb1-a13dc3e478dc",
		Type:        Organisation,
		Identifiers: []identifier{fsIdentifier, unsupporterIdentifier},
		ProperName:  "Proper Name",
	}

	assert.Error(cypherDriver.Write(testOrg))

	storedOrg, _, err := cypherDriver.Read("3166b06b-a7a7-40f7-bcb1-a13dc3e478dc")

	assert.NoError(err)
	assert.Equal(storedOrg, organisation{})

}

func TestWriteWillUpdateOrg(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	assert.NoError(cypherDriver.Write(minimalOrg))

	storedOrg, _, _ := cypherDriver.Read(minimalOrgUuid)

	assert.Empty(storedOrg.(organisation).HiddenLabel, "Minimal org should not have a hidden label value.")

	updatedOrg := organisation{
		UUID:        minimalOrgUuid,
		Type:        Organisation,
		Identifiers: []identifier{fsIdentifier},
		ProperName:  "Updated Name",
		HiddenLabel: "No longer hidden",
	}

	assert.NoError(cypherDriver.Write(updatedOrg))

	storedUpdatedOrg, _, _ := cypherDriver.Read(minimalOrgUuid)

	assert.Equal(updatedOrg, storedUpdatedOrg, "org should have been updated")
	assert.NotEmpty(storedUpdatedOrg.(organisation).HiddenLabel, "Updated org should have a hidden label value")
}

func TestWritesOrgsWithEscapedCharactersInfields(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	var oddCharOrg = organisation{
		UUID:               oddCharOrgUuid,
		Type:               Company,
		ProperName:         "TBWA\\Paling Walters Ltd.",
		Identifiers:        []identifier{fsIdentifier, leiCodeIdentifier},
		ParentOrganisation: "5852ca0f-f254-3002-b05c-d64a354a661e",
		ShortName:          "TBWA\\Paling Walters",
		FormerNames:        []string{"Paling Elli$ Cognis Ltd.", "Paling Ellis\\/ Ltd.", "Paling Walters Ltd.", "Paling Walter/'s Targis Ltd."},
		HiddenLabel:        "TBWA PALING WALTERS LTD",
	}

	assert.NoError(cypherDriver.Write(oddCharOrg))

	storedOrg, found, err := cypherDriver.Read(oddCharOrgUuid)

	assert.NoError(err, "Error finding organisation for uuid %s", oddCharOrgUuid)
	assert.True(found, "Didn't find organisation for uuid %s", oddCharOrgUuid)
	assert.True(reflect.DeepEqual(oddCharOrg, storedOrg), fmt.Sprintf("organisations should be the same \n EXPECTED  %+v \n ACTUAL  %+v", oddCharOrg, storedOrg))
}

func TestReadOrganisation(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	assert.NoError(cypherDriver.Write(fullOrg))

	storedOrg, found, err := cypherDriver.Read(fullOrgUuid)

	assert.NoError(err, "Error finding organisation for uuid %s", fullOrgUuid)
	assert.True(found, "Didn't find organisation for uuid %s", fullOrgUuid)
	assert.True(reflect.DeepEqual(fullOrg, storedOrg), fmt.Sprintf("organisations should be the same \n EXPECTED  %+v \n ACTUAL  %+v", fullOrg, storedOrg))
}

func TestDeleteNothing(t *testing.T) {
	assert := assert.New(t)
	db := getDatabaseConnectionAndCheckClean(t, assert)
	defer cleanDB(db, t, assert)

	cypherDriver := getCypherDriver(db)
	res, err := cypherDriver.Delete("4e484678-cf47-4168-b844-6adb47f8eb58")

	assert.NoError(err)
	assert.False(res)
}

func TestDeleteWithRelationships(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	cypherDriver.Write(fullOrg)
	cypherDriver.Delete(fullOrgUuid)

	storedOrg, _, err := cypherDriver.Read(fullOrgUuid)

	assert.NoError(err)
	assert.NotEmpty(storedOrg)
}

func TestDeleteNoRelationships(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	cypherDriver.Write(minimalOrg)
	cypherDriver.Delete(minimalOrgUuid)

	result := []struct {
		Uuid string `json:"t.uuid"`
	}{}

	getOrg := neoism.CypherQuery{
		Statement: `
			MATCH (t:Thing {uuid:"33f93f25-3301-417e-9b20-50b27d215617"}) RETURN t.uuid
			`,
		Result: &result,
	}

	err := db.Cypher(&getOrg)
	assert.NoError(err)
	assert.Empty(result)
}

func TestCount(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	cypherDriver.Write(minimalOrg)
	cypherDriver.Write(fullOrg)

	count, err := cypherDriver.Count()
	assert.NoError(err)
	assert.Equal(2, count)
}

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

func getDatabaseConnectionAndCheckClean(t *testing.T, assert *assert.Assertions) *neoism.Database {
	db := getDatabaseConnection(t, assert)
	cleanDB(db, t, assert)
	checkDbClean(db, t)
	return db
}

func getDatabaseConnection(t *testing.T, assert *assert.Assertions) *neoism.Database {
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}

	db, err := neoism.Connect(url)
	assert.NoError(err, "Failed to connect to Neo4j")
	return db
}

func cleanDB(db *neoism.Database, t *testing.T, assert *assert.Assertions) {
	qs := []*neoism.CypherQuery{
		{
			Statement: `
		MATCH (org:Thing {uuid: '4e484678-cf47-4168-b844-6adb47f8eb58'}) DETACH DELETE org
	`},
		{
			Statement: `
		MATCH (p:Thing {uuid: 'de38231e-e481-4958-b470-e124b2ef5a34'}) DETACH DELETE p
	`},
		{
			Statement: `
		MATCH (ind:Thing {uuid: 'c3d17865-f9d1-42f2-9ca2-4801cb5aacc0'}) DETACH DELETE ind
	`},
		{
			Statement: `
		MATCH (morg:Thing {uuid: '33f93f25-3301-417e-9b20-50b27d215617'}) DETACH DELETE morg
	`},
		{
			Statement: `
		MATCH (morg:Thing {uuid: '3166b06b-a7a7-40f7-bcb1-a13dc3e478dc'}) DETACH DELETE morg
	`},
		{
			Statement: `
		MATCH (morg:Thing {uuid: '161403e2-074f-3c82-9328-0337e909ac8c'}) DETACH DELETE morg
	`},
	}

	err := db.CypherBatch(qs)
	assert.NoError(err)
}

func getCypherDriver(db *neoism.Database) service {
	return NewCypherOrganisationService(neoutils.StringerDb{db}, db)
}