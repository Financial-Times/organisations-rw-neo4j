package organisations

import (
	"github.com/Financial-Times/neo-utils-go"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestWriteNewOrganisation(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	assert.NoError(cypherDriver.Write(FullOrg))

	storedOrg, _, err := cypherDriver.Read(FullOrgUuid)

	assert.NoError(err)
	assert.NotEmpty(storedOrg)
}

func TestWriteWillUpdateOrg(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	assert.NoError(cypherDriver.Write(MinimalOrg))

	storedOrg, _, _ := cypherDriver.Read(MinimalOrgUuid)

	assert.Empty(storedOrg.(organisation).HiddenLabel, "Minimal org should not have a hidden label value.")

	updatedOrg := organisation{
		UUID:        MinimalOrgUuid,
		Type:        TypeOrganisation,
		Identifiers: []identifier{FsIdentifier},
		ProperName:  "Updated Name",
		HiddenLabel: "No longer hidden",
	}

	assert.NoError(cypherDriver.Write(updatedOrg))

	storedUpdatedOrg, _, _ := cypherDriver.Read(MinimalOrgUuid)

	assert.Equal(updatedOrg, storedUpdatedOrg, "org should have been updated")
	assert.NotEmpty(storedUpdatedOrg.(organisation).HiddenLabel, "Updated org should have a hidden label value")
}

func TestWritesOrgsWithEscapedCharactersInfields(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	var oddCharOrg = organisation{
		UUID:               OddCharOrgUuid,
		Type:               TypeCompany,
		ProperName:         "TBWA\\Paling Walters Ltd.",
		Identifiers:        []identifier{FsIdentifier, LeiCodeIdentifier},
		ParentOrganisation: "5852ca0f-f254-3002-b05c-d64a354a661e",
		ShortName:          "TBWA\\Paling Walters",
		FormerNames:        []string{"Paling Elli$ Cognis Ltd.", "Paling Ellis\\/ Ltd.", "Paling Walters Ltd.", "Paling Walter/'s Targis Ltd."},
		HiddenLabel:        "TBWA PALING WALTERS LTD",
	}

	assert.NoError(cypherDriver.Write(oddCharOrg))

	storedOrg, found, err := cypherDriver.Read(OddCharOrgUuid)

	assert.NoError(err, "Error finding organisation for uuid %s", OddCharOrgUuid)
	assert.True(found, "Didn't find organisation for uuid %s", OddCharOrgUuid)
	assert.Equal(oddCharOrg, storedOrg, "organisations should be the same")
}

func TestReadOrganisation(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	assert.NoError(cypherDriver.Write(FullOrg))

	storedOrg, found, err := cypherDriver.Read(FullOrgUuid)

	assert.NoError(err, "Error finding organisation for uuid %s", FullOrgUuid)
	assert.True(found, "Didn't find organisation for uuid %s", FullOrgUuid)
	assert.Equal(FullOrg, storedOrg, "organisations should be the same")
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

	cypherDriver.Write(FullOrg)
	cypherDriver.Delete(FullOrgUuid)

	storedOrg, _, err := cypherDriver.Read(FullOrgUuid)

	assert.NoError(err)
	assert.NotEmpty(storedOrg)
}

func TestDeleteNoRelationships(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	cypherDriver.Write(MinimalOrg)
	cypherDriver.Delete(MinimalOrgUuid)

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

	cypherDriver.Write(MinimalOrg)
	cypherDriver.Write(FullOrg)

	count, err := cypherDriver.Count()
	assert.NoError(err)
	assert.Equal(3, count) // Three as full org has a parent org
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
	}

	err := db.CypherBatch(qs)
	assert.NoError(err)
}

func getCypherDriver(db *neoism.Database) service {
	return NewCypherOrganisationService(neoutils.StringerDb{db}, db)
}
