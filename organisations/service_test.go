package organisations

import (
	"fmt"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
	"os"
	"reflect"
	"testing"
	"github.com/Financial-Times/annotations-rw-neo4j/annotations"
	"encoding/json"
)

const (
	fullOrgUUID                  = "4e484678-cf47-4168-b844-6adb47f8eb58"
	minimalOrgUUID               = "33f93f25-3301-417e-9b20-50b27d215617"
	oddCharOrgUUID               = "5bb679d7-334e-4d51-a676-b1a10daaab38"
	canonicalOrgUUID             = "3f646c05-3e20-420a-b0e4-6fc1c9fb3a02"
	contentUUID                  = "c3bce4dc-c857-4fe6-8277-61c0294d9187"
	dupeIdentifierOrgUUID        = "fbe74159-f4a0-4aa0-9cca-c2bbb9e8bffe"
	parentOrgUUID                = "de38231e-e481-4958-b470-e124b2ef5a34"
	industryClassificationUUID   = "c3d17865-f9d1-42f2-9ca2-4801cb5aacc0"
	authorityNotSupportedOrgUUID = "3166b06b-a7a7-40f7-bcb1-a13dc3e478dc"
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
	IdentifierValue: minimalOrgUUID,
}

var fullOrg = organisation{
	UUID: fullOrgUUID,
	Type: PublicCompany,
	Identifiers:            []identifier{fsIdentifier, tmeIdentifier, leiCodeIdentifier},
	ProperName:             "Proper Name",
	LegalName:              "Legal Name",
	ShortName:              "Short Name",
	HiddenLabel:            "Hidden Label",
	FormerNames:            []string{"Old Name, inc.", "Older Name, inc."},
	TradeNames:             []string{"Old Trade Name, inc.", "Older Trade Name, inc."},
	LocalNames:             []string{"Oldé Name, inc.", "Tradé Name"},
	Aliases:                []string{"alias1", "alias2", "alias3"},
	ParentOrganisation:     parentOrgUUID,
	IndustryClassification: industryClassificationUUID,
}

var fullOrgWrittenForm = organisation{
	UUID: fullOrgUUID,
	Type: PublicCompany,
	//identifiers are in the expected read order
	Identifiers:            []identifier{fsIdentifier, tmeIdentifier, identifier{Authority:uppAuthority, IdentifierValue:fullOrgUUID},leiCodeIdentifier},
	ProperName:             "Proper Name",
	LegalName:              "Legal Name",
	ShortName:              "Short Name",
	HiddenLabel:            "Hidden Label",
	FormerNames:            []string{"Old Name, inc.", "Older Name, inc."},
	TradeNames:             []string{"Old Trade Name, inc.", "Older Trade Name, inc."},
	LocalNames:             []string{"Oldé Name, inc.", "Tradé Name"},
	Aliases:                []string{"alias1", "alias2", "alias3"},
	ParentOrganisation:     parentOrgUUID,
	IndustryClassification: industryClassificationUUID,
}

var minimalOrg = organisation{
	UUID:        minimalOrgUUID,
	Type:        Organisation,
	Identifiers: []identifier{fsIdentifierMinimal},
	ProperName:  "Minimal Org Proper Name",
}

var dupeIdentifierOrg = organisation{
	UUID:        dupeIdentifierOrgUUID,
	Type:        Company,
	Identifiers: []identifier{fsIdentifierOther, leiCodeIdentifier},
	ProperName:  "Dupe Identifier Proper Name",
}

var oddCharOrg = organisation{
	UUID:               oddCharOrgUUID,
	Type:               Company,
	ProperName:         "TBWA\\Paling Walters Ltd.",
	Identifiers:        []identifier{fsIdentifier, leiCodeIdentifier},
	ParentOrganisation: parentOrgUUID,
	ShortName:          "TBWA\\Paling Walters",
	FormerNames:        []string{"Paling Elli$ Cognis Ltd.", "Paling Ellis\\/ Ltd.", "Paling Walters Ltd.", "Paling Walter/'s Targis Ltd."},
	HiddenLabel:        "TBWA PALING WALTERS LTD",
}

var oddCharOrgWrittenForm = organisation{
	UUID:               oddCharOrgUUID,
	Type:               Company,
	ProperName:         "TBWA\\Paling Walters Ltd.",
	//identifiers are in the expected read order
	Identifiers:        []identifier{fsIdentifier, identifier{Authority:uppAuthority, IdentifierValue:oddCharOrgUUID}, leiCodeIdentifier},
	ParentOrganisation: parentOrgUUID,
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

	storedOrg, _, err := cypherDriver.Read(fullOrgUUID)

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
		UUID:        authorityNotSupportedOrgUUID,
		Type:        Organisation,
		Identifiers: []identifier{fsIdentifier, unsupporterIdentifier},
		ProperName:  "Proper Name",
	}

	assert.Error(cypherDriver.Write(testOrg))

	storedOrg, _, err := cypherDriver.Read(authorityNotSupportedOrgUUID)

	assert.NoError(err)
	assert.Equal(storedOrg, organisation{})

}

func TestWriteWillUpdateOrg(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	assert.NoError(cypherDriver.Write(minimalOrg))

	storedOrg, _, _ := cypherDriver.Read(minimalOrgUUID)

	assert.Empty(storedOrg.(organisation).HiddenLabel, "Minimal org should not have a hidden label value.")

	updatedOrg := organisation{
		UUID:        minimalOrgUUID,
		Type:        Organisation,
		Identifiers: []identifier{fsIdentifier},
		ProperName:  "Updated Name",
		HiddenLabel: "No longer hidden",
	}

	assert.NoError(cypherDriver.Write(updatedOrg))

	storedUpdatedOrg, _, _ := cypherDriver.Read(minimalOrgUUID)

	// add an identifier for canonical uuid - which will automatically written in store for each node
	updatedOrg.Identifiers = append(updatedOrg.Identifiers, identifier{Authority:uppAuthority, IdentifierValue:minimalOrgUUID})

	assert.Equal(updatedOrg, storedUpdatedOrg, "org should have been updated")
	assert.NotEmpty(storedUpdatedOrg.(organisation).HiddenLabel, "Updated org should have a hidden label value")
}

func TestWriteWillWriteCanonicalOrgAndDeleteAlternativeNodes(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	updatedOrg := organisation{
		UUID:        canonicalOrgUUID,
		Type:        Organisation,
		Identifiers: []identifier{fsIdentifier, uppIdentifier},
		ProperName:  "Updated Name",
		HiddenLabel: "No longer hidden",
	}

	assert.NoError(cypherDriver.Write(minimalOrg))
	assert.NoError(cypherDriver.Write(updatedOrg))

	storedMinimalOrg, _, _ := cypherDriver.Read(minimalOrgUUID)
	storedUpdatedOrg, _, _ := cypherDriver.Read(canonicalOrgUUID)

	// add an identifier for canonical uuid - which will automatically written in store for each node
	updatedOrg.Identifiers = append(updatedOrg.Identifiers, identifier{Authority:uppAuthority, IdentifierValue:canonicalOrgUUID})

	assert.Equal(organisation{}, storedMinimalOrg, "org should have been deleted")
	assert.Equal(updatedOrg, storedUpdatedOrg, "org should have been updated")
	assert.NotEmpty(storedUpdatedOrg.(organisation).HiddenLabel, "Updated org should have a hidden label value")
}

func TestWriteWillWriteCanonicalOrgAndDeleteAlternativeNodesWithRelationshipTransfer(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)

	annotationsRW := annotations.NewAnnotationsService(cypherDriver.cypherRunner, db, "v2")
	assert.NoError(annotationsRW.Initialise())

	defer cleanDB(db, t, assert)
	defer deleteAllViaService(db, assert, annotationsRW)

	updatedOrg := organisation{
		UUID:        canonicalOrgUUID,
		Type:        Organisation,
		Identifiers: []identifier{fsIdentifier, uppIdentifier},
		ProperName:  "Updated Name",
		HiddenLabel: "No longer hidden",
	}

	assert.NoError(cypherDriver.Write(minimalOrg))

	relMinimalOrg1, relMinimalOrg2, err := getNodeRelationshipNames(cypherDriver.cypherRunner, minimalOrgUUID)
	assert.Nil(err)

	relUpdatedOrg1, relUpdatedOrg2, err := getNodeRelationshipNames(cypherDriver.cypherRunner, canonicalOrgUUID)
	assert.Empty(relUpdatedOrg1)
	assert.Empty(relUpdatedOrg2)
	assert.Nil(err)

	writeJSONToService(annotationsRW,"./annotationBodyExample.json",minimalOrgUUID,assert)
	assert.NoError(cypherDriver.Write(updatedOrg))

	relUpdatedOrg1, relUpdatedOrg2, err = getNodeRelationshipNames(cypherDriver.cypherRunner, canonicalOrgUUID)
	assert.Nil(err)
	for _, rel := range relMinimalOrg1 {
		contains(relUpdatedOrg1, rel.RelationshipType)
	}
	for _, rel := range relMinimalOrg2 {
		contains(relUpdatedOrg2, rel.RelationshipType)
	}

	storedMinimalOrg, _, _ := cypherDriver.Read(minimalOrgUUID)
	storedUpdatedOrg, _, _ := cypherDriver.Read(canonicalOrgUUID)

	assert.Equal(organisation{}, storedMinimalOrg, "org should have been deleted")

	// add an identifier for canonical uuid - which will automatically written in store for each node
	updatedOrg.Identifiers = append(updatedOrg.Identifiers, identifier{Authority:uppAuthority, IdentifierValue:canonicalOrgUUID})
	assert.Equal(updatedOrg, storedUpdatedOrg, "org should have been updated")
	assert.NotEmpty(storedUpdatedOrg.(organisation).HiddenLabel, "Updated org should have a hidden label value")
}

func writeJSONToService(service annotations.Service, pathToJSONFile string, contentUUID string, assert *assert.Assertions) {
	f, err := os.Open(pathToJSONFile)
	assert.NoError(err)
	dec := json.NewDecoder(f)
	annotation, errr := service.DecodeJSON(dec)
	assert.NoError(errr)
	errrr := service.Write(contentUUID, annotation)
	assert.NoError(errrr)
}

func TestWritesOrgsWithEscapedCharactersInfields(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	assert.NoError(cypherDriver.Write(oddCharOrg))

	storedOrg, found, err := cypherDriver.Read(oddCharOrgUUID)

	assert.NoError(err, "Error finding organisation for uuid %s", oddCharOrgUUID)
	assert.True(found, "Didn't find organisation for uuid %s", oddCharOrgUUID)

	assert.True(reflect.DeepEqual(oddCharOrgWrittenForm, storedOrg), fmt.Sprintf("organisations should be the same \n EXPECTED  %+v \n ACTUAL  %+v", oddCharOrg, storedOrg))
}

func TestReadOrganisation(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	assert.NoError(cypherDriver.Write(fullOrg))

	storedOrg, found, err := cypherDriver.Read(fullOrgUUID)

	assert.NoError(err, "Error finding organisation for uuid %s", fullOrgUUID)
	assert.True(found, "Didn't find organisation for uuid %s", fullOrgUUID)

	assert.True(reflect.DeepEqual(fullOrgWrittenForm, storedOrg), fmt.Sprintf("organisations should be the same \n EXPECTED  %+v \n ACTUAL  %+v", fullOrg, storedOrg))
}

func TestDeleteNothing(t *testing.T) {
	assert := assert.New(t)
	db := getDatabaseConnectionAndCheckClean(t, assert)
	defer cleanDB(db, t, assert)

	cypherDriver := getCypherDriver(db)
	res, err := cypherDriver.Delete(fullOrgUUID)

	assert.NoError(err)
	assert.False(res)
}

func TestDeleteWithRelationships(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	assert.Nil(cypherDriver.Write(fullOrg))
	found, err := cypherDriver.Delete(fullOrgUUID)
	assert.True(found)

	storedOrg, _, err := cypherDriver.Read(fullOrgUUID)

	assert.NoError(err)
	assert.NotEmpty(storedOrg)
}

func TestDeleteNoRelationships(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert)

	assert.Nil(cypherDriver.Write(minimalOrg))
	found, err := cypherDriver.Delete(minimalOrgUUID)
	assert.NoError(err)
	assert.True(found, "Didn't find organisation for uuid %s", minimalOrgUUID)

	result := []struct {
		UUID string `json:"t.uuid"`
	}{}

	getOrg := neoism.CypherQuery{
		Statement: fmt.Sprintf("MATCH (t:Thing {uuid:'%v'}) RETURN t.uuid", minimalOrgUUID),
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
		UUID string `json:"org.uuid"`
	}{}

	checkGraph := neoism.CypherQuery{
		Statement: `
			MATCH (org:Thing) WHERE org.uuid in {uuids} RETURN org.uuid
		`,
		Parameters: neoism.Props{
			"uuids": []string{fullOrgUUID, minimalOrgUUID, oddCharOrgUUID, dupeIdentifierOrgUUID},
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
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'})<-[:IDENTIFIES*0..]-(i:Identifier) DETACH DELETE org, i", fullOrgUUID),
		},
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'}) DETACH DELETE org", fullOrgUUID),
		},
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'})<-[:IDENTIFIES*0..]-(i:Identifier) DETACH DELETE org, i", canonicalOrgUUID),
		},
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'}) DETACH DELETE org", canonicalOrgUUID),
		},
		{
			Statement: fmt.Sprintf("MATCH (c:Content {uuid: '%v'})-[rel]-(o) DELETE c, rel ", contentUUID),
		},
		{
			Statement: fmt.Sprintf("MATCH (c:Content {uuid: '%v'}) DELETE c ", contentUUID),
		},
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'})<-[:IDENTIFIES*0..]-(i:Identifier) DETACH DELETE org, i", minimalOrgUUID),
		},
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'}) DETACH DELETE org", minimalOrgUUID),
		},
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'})<-[:IDENTIFIES*0..]-(i:Identifier) DETACH DELETE org, i", oddCharOrgUUID),
		},
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'})<-[:IDENTIFIES*0..]-(i:Identifier) DETACH DELETE org, i", dupeIdentifierOrgUUID),
		},
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'}) DETACH DELETE org", parentOrgUUID),
		},
		{
			Statement: fmt.Sprintf("MATCH (class:Thing {uuid: '%v'}) DETACH DELETE class", industryClassificationUUID),
		},
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'})<-[:IDENTIFIES*0..]-(i:Identifier) DETACH DELETE org, i", authorityNotSupportedOrgUUID),
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

func deleteAllViaService(db *neoism.Database, assert *assert.Assertions, annotationsRW annotations.Service){
	annotationsRW.Delete(minimalOrgUUID)
	annotationsRW.Delete(canonicalOrgUUID)
	qs := []*neoism.CypherQuery{
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'}) DETACH DELETE org", "2384fa7a-d514-3d6a-a0ea-3a711f66d0d8"),
		},
		{
			Statement: fmt.Sprintf("MATCH (org:Thing {uuid: '%v'}) DETACH DELETE org", "ccaa202e-3d27-3b75-b2f2-261cf5038a1f"),
		},
	}
	err := db.CypherBatch(qs)
	assert.NoError(err)
}