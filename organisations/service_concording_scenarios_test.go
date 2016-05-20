package organisations

import (
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/annotations-rw-neo4j/annotations"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

const (
	org1UUID    = "0d99ab07-3b0a-4313-939e-caa02db23aa1"
	org2UUID    = "b40d53d3-3b0d-4069-90d9-0ccf9d7e1d0c"
	org3UUID    = "e3772e07-b43d-4e77-888f-be3ba61279a48"
	org4UUID    = "5c510ad1-2b73-4375-90e1-6ccbc50bd21f"
	contentUUID = "c3bce4dc-c857-4fe6-8277-61c0294d9187"
)

var concordedUUIDs = []string{org1UUID, org2UUID, org3UUID, org4UUID}

var uppOrg1Identifier = identifier{
	Authority:       uppAuthority,
	IdentifierValue: org1UUID,
}

var uppOrg2Identifier = identifier{
	Authority:       uppAuthority,
	IdentifierValue: org2UUID,
}

var uppOrg3Identifier = identifier{
	Authority:       uppAuthority,
	IdentifierValue: org3UUID,
}

var uppOrg4Identifier = identifier{
	Authority:       uppAuthority,
	IdentifierValue: org4UUID,
}

var fsOrg1Identifier = identifier{
	Authority:       fsAuthority,
	IdentifierValue: "org1 factset id",
}

var fsOrg4Identifier = identifier{
	Authority:       fsAuthority,
	IdentifierValue: "org4 factset id",
}

var leiCodeOrg4Identifier = identifier{
	Authority:       leiAuthority,
	IdentifierValue: "leiCodeIdentifier org4",
}

var leiCodeOrgxIdentifier = identifier{
	Authority:       leiAuthority,
	IdentifierValue: "leiCodeIdentifier",
}

var tmeOrg2Identifier = identifier{
	Authority:       tmeAuthority,
	IdentifierValue: "tmeIdentifier org2",
}

var tmeOrg3Identifier = identifier{
	Authority:       tmeAuthority,
	IdentifierValue: "tmeIdentifier org3",
}

var tmeOrg4Identifier = identifier{
	Authority:       tmeAuthority,
	IdentifierValue: "tmeIdentifier org4",
}

var org1 = organisation{
	UUID:        org1UUID,
	Type:        Organisation,
	Identifiers: []identifier{fsOrg1Identifier, uppOrg1Identifier, leiCodeOrgxIdentifier},
	ProperName:  "Proper Name 1",
}

var org2 = organisation{
	UUID:               org2UUID,
	Type:               Organisation,
	Identifiers:        []identifier{tmeOrg2Identifier, uppOrg2Identifier},
	ProperName:         "Proper Name 2",
	ParentOrganisation: org4UUID,
}

var org3 = organisation{
	UUID:        org3UUID,
	Type:        Organisation,
	Identifiers: []identifier{tmeOrg3Identifier, uppOrg3Identifier},
	ProperName:  "Proper Name 3",
}

var org4 = organisation{
	UUID:        org4UUID,
	Type:        Organisation,
	Identifiers: []identifier{fsOrg4Identifier, tmeOrg4Identifier, uppOrg4Identifier, leiCodeOrg4Identifier},
	ProperName:  "Proper Name 4",
}

func TestConcordeThreeOrganisations(t *testing.T) {
	assert := assert.New(t)
	db := getDatabaseConnectionAndCheckClean(t, assert, concordedUUIDs)
	cypherDriver := getCypherDriver(db)

	defer cleanDB(db, t, assert, concordedUUIDs)

	org1Updated := organisation{
		UUID:        org1UUID,
		Type:        Organisation,
		Identifiers: []identifier{fsOrg1Identifier, tmeOrg2Identifier, tmeOrg3Identifier, uppOrg1Identifier, uppOrg2Identifier, uppOrg3Identifier, leiCodeOrgxIdentifier},
		ProperName:  "Updated Name",
	}

	assert.NoError(cypherDriver.Write(org1))
	assert.NoError(cypherDriver.Write(org2))
	assert.NoError(cypherDriver.Write(org3))

	_, found, _ := cypherDriver.Read(org1UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org1UUID)
	_, found, _ = cypherDriver.Read(org2UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org2UUID)
	_, found, _ = cypherDriver.Read(org3UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org3UUID)

	assert.NoError(cypherDriver.Write(org1Updated))

	_, found, _ = cypherDriver.Read(org2UUID)
	assert.False(found, "Organisation for uuid %s should have been deleted", org2UUID)
	_, found, _ = cypherDriver.Read(org3UUID)
	assert.False(found, "Organisation for uuid %s should have been deleted", org3UUID)

	org1Stored, found, _ := cypherDriver.Read(org1UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org1UUID)
	assert.Equal(org1Updated, org1Stored)
}

func TestConcordeOrgsAndDeleteAlternativeNodes(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert, concordedUUIDs)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert, concordedUUIDs)

	updatedOrg1 := organisation{
		UUID:        org1UUID,
		Type:        Organisation,
		Identifiers: []identifier{fsOrg1Identifier, uppOrg1Identifier, uppOrg2Identifier},
		ProperName:  "Updated Name",
	}

	assert.NoError(cypherDriver.Write(org1))
	assert.NoError(cypherDriver.Write(org2))

	storedOrg1, _, _ := cypherDriver.Read(org1UUID)
	assert.Equal(org1, storedOrg1, "orgs should be equal ")

	assert.NoError(cypherDriver.Write(updatedOrg1))

	storedOrg2, _, _ := cypherDriver.Read(org2UUID)
	storedUpdatedOrg1, _, _ := cypherDriver.Read(org1UUID)

	assert.Equal(organisation{}, storedOrg2, "org should have been deleted")
	assert.Equal(updatedOrg1, storedUpdatedOrg1, "org should have been updated")
}

func TestConcordeOrgsAndDeleteAlternativeNodesWithRelationshipTransfer(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert, concordedUUIDs)
	cypherDriver := getCypherDriver(db)

	annotationsRW := annotations.NewAnnotationsService(cypherDriver.cypherRunner, db, "v2")
	assert.NoError(annotationsRW.Initialise())

	defer cleanDB(db, t, assert, concordedUUIDs)
	defer deleteAllViaService(db, assert, annotationsRW)

	assert.NoError(cypherDriver.Write(org2))

	relOrg2L, relOrg2R, err := getNodeRelationshipNames(cypherDriver.cypherRunner, org2UUID)
	assert.Nil(err)
	relOrg1L, relOrg1R, err := getNodeRelationshipNames(cypherDriver.cypherRunner, org1UUID)
	assert.Empty(relOrg1L)
	assert.Empty(relOrg1R)
	assert.Nil(err)

	updatedOrg1 := organisation{
		UUID:        org1UUID,
		Type:        Organisation,
		Identifiers: []identifier{fsOrg4Identifier, uppOrg1Identifier, uppOrg2Identifier},
		ProperName:  "Updated Name",
	}

	writeJSONToService(annotationsRW, "./annotationBodyExample.json", contentUUID, assert)
	assert.NoError(cypherDriver.Write(updatedOrg1))

	relUpdatedOrg1L, relUpdatedOrg1R, err := getNodeRelationshipNames(cypherDriver.cypherRunner, org1UUID)
	assert.Nil(err)
	for _, rel := range relOrg2L {
		contains(relUpdatedOrg1L, rel.RelationshipType)
	}
	for _, rel := range relOrg2R {
		contains(relUpdatedOrg1R, rel.RelationshipType)
	}

	storedOrg2, _, _ := cypherDriver.Read(org2UUID)
	storedOrg1, _, _ := cypherDriver.Read(org1UUID)

	assert.Equal(organisation{}, storedOrg2, "org should have been deleted")
	assert.Equal(updatedOrg1, storedOrg1, "org should have been updated")
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

func deleteAllViaService(db *neoism.Database, assert *assert.Assertions, annotationsRW annotations.Service) {
	_, err := annotationsRW.Delete(contentUUID)
	assert.Nil(err)

	qs := []*neoism.CypherQuery{
		{
			Statement: fmt.Sprintf("MATCH (c:Thing {uuid: '%v'})-[rel]-(o) DELETE c, rel ", contentUUID),
		},
		{
			Statement: fmt.Sprintf("MATCH (c:Thing {uuid: '%v'}) DELETE c ", contentUUID),
		},
	}

	err = db.CypherBatch(qs)
	assert.NoError(err)
}
