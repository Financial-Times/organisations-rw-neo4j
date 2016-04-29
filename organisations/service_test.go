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
	fullOrgUuid                  = "4e484678-cf47-4168-b844-6adb47f8eb58"
	minimalOrgUuid               = "33f93f25-3301-417e-9b20-50b27d215617"
	oddCharOrgUuid               = "5bb679d7-334e-4d51-a676-b1a10daaab38"
	canonicalOrgUuid             = "3f646c05-3e20-420a-b0e4-6fc1c9fb3a02"
	contentUuid		     = "c3bce4dc-c857-4fe6-8277-61c0294d9187"
	dupeIdentifierOrgUuid        = "fbe74159-f4a0-4aa0-9cca-c2bbb9e8bffe"
	parentOrgUuid                = "de38231e-e481-4958-b470-e124b2ef5a34"
	industryClassificationUuid   = "c3d17865-f9d1-42f2-9ca2-4801cb5aacc0"
	authorityNotSupportedOrgUuid = "3166b06b-a7a7-40f7-bcb1-a13dc3e478dc"
)

var fsIdentifier = identifier{
	Authority:       fsAuthority,
	IdentifierValue: "identifierValue",
}

var fsIdentifierOther = identifier{
	Authority:       fsAuthority,
	IdentifierValue: "identifierOtherValue",
}

var fsIdentifierMinimal = identifier{
	Authority:       fsAuthority,
	IdentifierValue: "identifierMinimalValue",
}

var leiCodeIdentifier = identifier{
	Authority:       leiAuthority,
	IdentifierValue: "leiCodeIdentifier",
}

var tmeIdentifier = identifier{
	Authority:       tmeAuthority,
	IdentifierValue: "tmeIdentifier",
}

var uppIdentifier = identifier{
	Authority:       uppAuthority,
	IdentifierValue: minimalOrgUuid,
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
	ParentOrganisation:     parentOrgUuid,
	IndustryClassification: industryClassificationUuid,
}

var minimalOrg = organisation{
	UUID:        minimalOrgUuid,
	Type:        Organisation,
	Identifiers: []identifier{fsIdentifierMinimal},
	ProperName:  "Minimal Org Proper Name",
}

var dupeIdentifierOrg = organisation{
	UUID:        dupeIdentifierOrgUuid,
	Type:        Company,
	Identifiers: []identifier{fsIdentifierOther, leiCodeIdentifier},
	ProperName:  "Dupe Identifier Proper Name",
}
var oddCharOrg = organisation{
	UUID:               oddCharOrgUuid,
	Type:               Company,
	ProperName:         "TBWA\\Paling Walters Ltd.",
	Identifiers:        []identifier{fsIdentifier, leiCodeIdentifier},
	ParentOrganisation: parentOrgUuid,
	ShortName:          "TBWA\\Paling Walters",
	FormerNames:        []string{"Paling Elli$ Cognis Ltd.", "Paling Ellis\\/ Ltd.", "Paling Walters Ltd.", "Paling Walter/'s Targis Ltd."},
	HiddenLabel:        "TBWA PALING WALTERS LTD",
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
		UUID:        authorityNotSupportedOrgUuid,
		Type:        Organisation,
		Identifiers: []identifier{fsIdentifier, unsupporterIdentifier},
		ProperName:  "Proper Name",
	}

	assert.Error(cypherDriver.Write(testOrg))

	storedOrg, _, err := cypherDriver.Read(authorityNotSupportedOrgUuid)

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

func TestWriteWillWriteCanonicalOrgAndDeleteAlternativeNodes(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	updatedOrg := organisation{
		UUID:        canonicalOrgUuid,
		Type:        Organisation,
		Identifiers: []identifier{fsIdentifier, uppIdentifier},
		ProperName:  "Updated Name",
		HiddenLabel: "No longer hidden",
	}

	assert.NoError(cypherDriver.Write(minimalOrg))
	assert.NoError(cypherDriver.Write(updatedOrg))

	storedMinimalOrg, _, _ := cypherDriver.Read(minimalOrgUuid)
	storedUpdatedOrg, _, _ := cypherDriver.Read(canonicalOrgUuid)

	assert.Equal(organisation{}, storedMinimalOrg, "org should have been deleted")
	assert.Equal(updatedOrg, storedUpdatedOrg, "org should have been updated")
	assert.NotEmpty(storedUpdatedOrg.(organisation).HiddenLabel, "Updated org should have a hidden label value")
}

func TestWriteWillWriteCanonicalOrgAndDeleteAlternativeNodesWithRelationshipTransfer(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	updatedOrg := organisation{
		UUID:        canonicalOrgUuid,
		Type:        Organisation,
		Identifiers: []identifier{fsIdentifier, uppIdentifier},
		ProperName:  "Updated Name",
		HiddenLabel: "No longer hidden",
	}

	//add MENTIONS relationship with platformVersion property
	addMentionsQuery := &neoism.CypherQuery{
		Statement: `MATCH (c:Thing{uuid:{uuid}})
			    CREATE (co:Content{uuid:{cuuid}})
			    CREATE (co)-[r:MENTIONS{platformVersion:"v2"}]->(c)`,
		Parameters: map[string]interface{}{
			"cuuid": contentUuid,
			"uuid": minimalOrgUuid,
		},
	}

	assert.NoError(cypherDriver.Write(minimalOrg))
	assert.NoError(cypherDriver.cypherRunner.CypherBatch([]*neoism.CypherQuery{addMentionsQuery}))
	assert.NoError(cypherDriver.Write(updatedOrg))

	storedMinimalOrg, _, _ := cypherDriver.Read(minimalOrgUuid)
	storedUpdatedOrg, _, _ := cypherDriver.Read(canonicalOrgUuid)

	type version []struct {
		Version	string	`json:"r.platformVersion"`
	}

	oldPlatformVersion := version {}
	newPlatformVersion := version {}

	readMentionsQueryForOldOrg := &neoism.CypherQuery{
		Statement: `match (co:Content{uuid:{cuuid}})-[r:MENTIONS]->(c:Thing{uuid:{uuid}})
		 	    return r.platformVersion`,
		Parameters: map[string]interface{}{
			"cuuid": contentUuid,
			"uuid": minimalOrgUuid,
		},
		Result: &oldPlatformVersion,
	}
	readMentionsQueryForNewOrg := &neoism.CypherQuery{
		Statement: `match (co:Content{uuid:{cuuid}})-[r:MENTIONS]->(c:Thing{uuid:{uuid}})
		 	    return r.platformVersion`,
		Parameters: map[string]interface{}{
			"cuuid": contentUuid,
			"uuid": canonicalOrgUuid,
		},
		Result: &newPlatformVersion,
	}

	assert.NoError(cypherDriver.cypherRunner.CypherBatch([]*neoism.CypherQuery{readMentionsQueryForNewOrg,readMentionsQueryForOldOrg}))

	assert.Equal(organisation{}, storedMinimalOrg, "org should have been deleted")
	assert.Equal(updatedOrg, storedUpdatedOrg, "org should have been updated")

	assert.Equal(1, len(newPlatformVersion), "platformVersion size differs for new org")
	assert.Equal("v2", newPlatformVersion[0].Version, "platformVersion value differs for new org")

	assert.Equal(0, len(oldPlatformVersion), "platformVersion size differs for old org")

	assert.NotEmpty(storedUpdatedOrg.(organisation).HiddenLabel, "Updated org should have a hidden label value")
}

func TestWritesOrgsWithEscapedCharactersInfields(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

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
	res, err := cypherDriver.Delete(fullOrgUuid)

	assert.NoError(err)
	assert.False(res)
}

func TestDeleteWithRelationships(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	cypherDriver.Write(fullOrg)
	found, err := cypherDriver.Delete(fullOrgUuid)
	assert.True(found)

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
	found, err := cypherDriver.Delete(minimalOrgUuid)
	assert.NoError(err)
	assert.True(found, "Didn't find organisation for uuid %s", minimalOrgUuid)

	result := []struct {
		Uuid string `json:"t.uuid"`
	}{}

	getOrg := neoism.CypherQuery{
		Statement: fmt.Sprintf("MATCH (t:Thing {uuid:'%v'}) RETURN t.uuid", minimalOrgUuid),
		Result:    &result,
	}

	assert.NoError(db.Cypher(&getOrg))
	assert.Empty(result)
}

func TestToCheckYouCanNotCreateOrganisationWithDuplicateIdentifier(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)
	assert.NoError(cypherDriver.Write(fullOrg))
	err := cypherDriver.Write(dupeIdentifierOrg)
	assert.Error(err)
	assert.IsType(&neoutils.ConstraintViolationError{}, err)
}

func TestCount(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	assert.NoError(cypherDriver.Write(minimalOrg))
	assert.NoError(cypherDriver.Write(fullOrg))

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
			MATCH (org:Thing) WHERE org.uuid in {uuids} RETURN org.uuid
		`,
		Parameters: neoism.Props{
			"uuids": []string{fullOrgUuid, minimalOrgUuid, oddCharOrgUuid, dupeIdentifierOrgUuid},
		},
		Result: &result,
	}
	err := db.Cypher(&checkGraph)
	assert.NoError(err)
	assert.Empty(result)
}

func getDatabaseConnectionAndCheckClean(t *testing.T, assert *assert.Assertions) *neoism.Database {
	db := getDatabaseConnection(assert)
	cleanDB(db, t, assert)
	checkDbClean(db, t)
	return db
}

func getDatabaseConnection(assert *assert.Assertions) *neoism.Database {
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
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'})<-[:IDENTIFIES*0..]-(i:Identifier) DETACH DELETE org, i", fullOrgUuid),
		},
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'}) DETACH DELETE org", fullOrgUuid),
		},
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'})<-[:IDENTIFIES*0..]-(i:Identifier) DETACH DELETE org, i", canonicalOrgUuid),
		},
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'}) DETACH DELETE org", canonicalOrgUuid),
		},
		{
			Statement: fmt.Sprintf("MATCH (c:Content {uuid: '%v'})-[rel]-(o) DELETE c, rel ", contentUuid),
		},
		{
			Statement: fmt.Sprintf("MATCH (c:Content {uuid: '%v'}) DELETE c ", contentUuid),
		},
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'})<-[:IDENTIFIES*0..]-(i:Identifier) DETACH DELETE org, i", minimalOrgUuid),
		},
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'})<-[:IDENTIFIES*0..]-(i:Identifier) DETACH DELETE org, i", oddCharOrgUuid),
		},
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'})<-[:IDENTIFIES*0..]-(i:Identifier) DETACH DELETE org, i", dupeIdentifierOrgUuid),
		},
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'}) DETACH DELETE org", parentOrgUuid),
		},
		{
			Statement: fmt.Sprintf("MATCH (class:Thing {uuid: '%v'}) DETACH DELETE class", industryClassificationUuid),
		},
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'})<-[:IDENTIFIES*0..]-(i:Identifier) DETACH DELETE org, i", authorityNotSupportedOrgUuid),
		},
	}

	err := db.CypherBatch(qs)
	assert.NoError(err)
}

func getCypherDriver(db *neoism.Database) service {
	cr := NewCypherOrganisationService(neoutils.NewBatchCypherRunner(neoutils.StringerDb{db}, 3), db)
	cr.Initialise()
	return cr
}
