package organisations

import (
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/annotations-rw-neo4j/annotations"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

const (
	org1UUID              = "0d99ab07-3b0a-4313-939e-caa02db23aa1"
	org2UUID              = "b40d53d3-3b0d-4069-90d9-0ccf9d7e1d0c"
	org3UUID              = "ba956ba9-e552-4abf-9850-1346da690bb8"
	org9UUID              = "bbb7173b-2e90-4cc5-b439-252427e46cd0"
	org8UUID              = "5c510ad1-2b73-4375-90e1-6ccbc50bd21f"
	contentUUID           = "c3bce4dc-c857-4fe6-8277-61c0294d9187"
	fsOrg1Identifier      = "org1 factset id"
	fsOrg8Identifier      = "org8 factset id"
	leiCodeOrg8Identifier = "leiCodeIdentifier org8"
	leiCodeOrgxIdentifier = "leiCodeIdentifier"
	tmeOrg2Identifier     = "tmeIdentifier org2"
	tmeOrg3Identifier     = "tmeIdentifier org3"
	tmeOrg8Identifier     = "tmeIdentifier org8"
	tmeOrg9Identifier     = "tmeIdentifier org9"
)

var concordedUUIDs = []string{org1UUID, org2UUID, org3UUID, org9UUID, org8UUID}

var org1 = organisation{
	UUID: org1UUID,
	Type: Organisation,
	AlternativeIdentifiers: alternativeIdentifiers{
		FactsetIdentifier: fsOrg1Identifier,
		UUIDS:             []string{org1UUID},
		LeiCode:           leiCodeOrgxIdentifier,
		TME:               []string{},
	},
	ProperName: "Proper Name 1",
}

var org2 = organisation{
	UUID: org2UUID,
	Type: Organisation,
	AlternativeIdentifiers: alternativeIdentifiers{
		UUIDS: []string{org2UUID},
		TME:   []string{tmeOrg2Identifier},
	},
	ProperName:         "Proper Name 2",
	ParentOrganisation: org8UUID,
}

var org3 = organisation{
	UUID: org3UUID,
	Type: Organisation,
	AlternativeIdentifiers: alternativeIdentifiers{
		UUIDS: []string{org3UUID},
		TME:   []string{tmeOrg3Identifier},
	},
	ProperName:         "Proper Name 3",
	ParentOrganisation: org2UUID,
}

var org8 = organisation{
	UUID: org8UUID,
	Type: Organisation,
	AlternativeIdentifiers: alternativeIdentifiers{
		FactsetIdentifier: fsOrg8Identifier,
		UUIDS:             []string{org8UUID},
		TME:               []string{tmeOrg8Identifier},
		LeiCode:           leiCodeOrg8Identifier,
	},
	ProperName: "Proper Name 8",
}

var org9 = organisation{
	UUID: org9UUID,
	Type: Organisation,
	AlternativeIdentifiers: alternativeIdentifiers{
		UUIDS: []string{org9UUID},
		TME:   []string{tmeOrg9Identifier},
	},
	ProperName: "Proper Name 9",
}

func TestConcordeThreeOrganisations(t *testing.T) {
	assert := assert.New(t)
	db := getDatabaseConnectionAndCheckClean(t, assert, concordedUUIDs)
	cypherDriver := getCypherDriver(db)

	defer cleanDB(db, t, assert, concordedUUIDs)

	org1Updated := organisation{
		UUID: org1UUID,
		Type: Organisation,
		AlternativeIdentifiers: alternativeIdentifiers{
			FactsetIdentifier: fsOrg1Identifier,
			UUIDS:             []string{org1UUID, org2UUID, org9UUID},
			LeiCode:           leiCodeOrgxIdentifier,
			TME:               []string{tmeOrg2Identifier, tmeOrg9Identifier},
		},
		ProperName: "Updated Name",
	}

	assert.NoError(cypherDriver.Write(org1))
	assert.NoError(cypherDriver.Write(org2))
	assert.NoError(cypherDriver.Write(org9))

	_, found, _ := cypherDriver.Read(org1UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org1UUID)
	_, found, _ = cypherDriver.Read(org2UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org2UUID)
	_, found, _ = cypherDriver.Read(org9UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org9UUID)

	assert.NoError(cypherDriver.Write(org1Updated))

	_, found, _ = cypherDriver.Read(org2UUID)
	assert.False(found, "Organisation for uuid %s should have been deleted", org2UUID)
	_, found, _ = cypherDriver.Read(org9UUID)
	assert.False(found, "Organisation for uuid %s should have been deleted", org9UUID)

	org1Stored, found, _ := cypherDriver.Read(org1UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org1UUID)
	assert.Equal(org1Updated, org1Stored)
}

// concorde node with multiple major mentions (mentions with platformVersion v1)
func TestConcordeOrganisationsWithRelationships(t *testing.T) {
	assert := assert.New(t)
	db := getDatabaseConnectionAndCheckClean(t, assert, concordedUUIDs)
	cypherDriver := getCypherDriver(db)

	defer cleanDB(db, t, assert, concordedUUIDs)

	// STEP 1: write nodes
	assert.NoError(cypherDriver.Write(org1))
	assert.NoError(cypherDriver.Write(org2))
	assert.NoError(cypherDriver.Write(org9))
	assert.NoError(cypherDriver.Write(org8))

	_, found, _ := cypherDriver.Read(org1UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org1UUID)
	_, found, _ = cypherDriver.Read(org2UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org2UUID)
	_, found, _ = cypherDriver.Read(org9UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org9UUID)
	_, found, _ = cypherDriver.Read(org8UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org8UUID)

	//STEP 2: write relationships

	//write V2 mentions, and about annotation for org2UUID, write V2 mentions annotation for org8UUID
	v2AnnotationsRW := annotations.NewAnnotationsService(cypherDriver.cypherRunner, db, "v2")
	assert.NoError(v2AnnotationsRW.Initialise())
	writeJSONToService(v2AnnotationsRW, "./test-resources/annotationBodyForOrg2AndOrg8.json", contentUUID, assert)

	//write V1 mentions annotation for org1UUID and org9UUID - considered as major mentions
	v1AnnotationsRW := annotations.NewAnnotationsService(cypherDriver.cypherRunner, db, "v1")
	assert.NoError(v1AnnotationsRW.Initialise())
	writeJSONToService(v1AnnotationsRW, "./test-resources/annotationBodyForOrg1AndOrg9.json", contentUUID, assert)

	//STEP3: concorde org1, with org2 and org9
	updatedOrg1 := organisation{
		UUID: org1UUID,
		Type: Organisation,
		AlternativeIdentifiers: alternativeIdentifiers{
			FactsetIdentifier: fsOrg1Identifier,
			UUIDS:             []string{org1UUID, org2UUID, org9UUID},
			LeiCode:           leiCodeOrgxIdentifier,
			TME:               []string{tmeOrg2Identifier, tmeOrg9Identifier},
		},
		// should come out from the transformer like this, otherwise won't be merged
		ProperName:         "Updated Name",
		ParentOrganisation: org8UUID, // should come out from the transformer - otherwise won't be transferred
	}

	assert.NoError(cypherDriver.Write(updatedOrg1))

	//RESULTS concording should result in:

	// - the presence of node 1 and 8, absence of node 2, 9
	_, found, _ = cypherDriver.Read(org1UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org1UUID)
	_, found, _ = cypherDriver.Read(org8UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org8UUID)
	_, found, _ = cypherDriver.Read(org2UUID)
	assert.False(found, "Didn't find organisation for uuid %s", org2UUID)
	_, found, _ = cypherDriver.Read(org9UUID)
	assert.False(found, "Didn't find organisation for uuid %s", org9UUID)

	//- for org 8:
	//	 - one v2 mentions from content - which existed
	//	 - one SUB_ORGANISATION_OF to org8 from org2
	//	 - 4 IDENTIFIES relationships from identifiers to nodes
	transferredPropertyLR, transferredPropertyRL, err := readRelationshipDetails(cypherDriver.cypherRunner, "Thing", org8UUID)
	assert.Nil(err)
	assert.Equal(0, len(transferredPropertyRL))
	assert.Equal(2, len(transferredPropertyLR))
	assert.Contains(transferredPropertyLR, property{Type: "MENTIONS", PlatformVersion: "v2"})
	assert.Contains(transferredPropertyLR, property{Type: "SUB_ORGANISATION_OF", PlatformVersion: ""})

	transferredPropertyLR, transferredPropertyRL, err = readRelationshipDetails(cypherDriver.cypherRunner, "Identifier", org8UUID)
	assert.Nil(err)
	assert.Equal(0, len(transferredPropertyRL))
	assert.Equal(4, len(transferredPropertyLR))
	for _, rel := range transferredPropertyLR {
		assert.Equal("IDENTIFIES", rel.Type)
		assert.Equal("", rel.PlatformVersion)
	}

	// - for org 1:
	//	 - one v2 mentions from content
	//	 - one v1 mentions from content (two merged in one, with properties from the randomly selected relationship)
	//	 - one v2 about from content
	//	 - one SUB_ORGANISATION_OF to org8
	//	 - 7 IDENTIFIES relationships from identifiers to node
	transferredPropertyLR, transferredPropertyRL, err = readRelationshipDetails(cypherDriver.cypherRunner, "Thing", org1UUID)
	assert.Nil(err)
	assert.Equal(3, len(transferredPropertyLR))
	assert.Contains(transferredPropertyLR, property{Type: "MENTIONS", PlatformVersion: "v2"})
	assert.Contains(transferredPropertyLR, property{Type: "MENTIONS", PlatformVersion: "v1"})
	assert.Contains(transferredPropertyLR, property{Type: "ABOUT", PlatformVersion: "v2"})
	assert.Equal(1, len(transferredPropertyRL))
	assert.Contains(transferredPropertyRL, property{Type: "SUB_ORGANISATION_OF", PlatformVersion: ""})

	transferredPropertyLR, transferredPropertyRL, err = readRelationshipDetails(cypherDriver.cypherRunner, "Identifier", org1UUID)
	assert.Nil(err)
	assert.Equal(0, len(transferredPropertyRL))
	assert.Equal(7, len(transferredPropertyLR))
	assert.Contains(transferredPropertyLR, property{Type: "IDENTIFIES", PlatformVersion: ""})
	for _, rel := range transferredPropertyLR {
		assert.Equal("IDENTIFIES", rel.Type)
		assert.Equal("", rel.PlatformVersion)
	}
}

// Concorde nodes with incoming and outgoing has-organisation-of relationships
func TestTransferIncomingHasSubOrganisationOfRelationships(t *testing.T) {
	assert := assert.New(t)
	//4 nodes with:
	// [org3]-[sub-organisation-of]->[org2]
	// [org2]-[sub-organisation-of]->[org8]
	// INPUT: org1 will be concorded with org2 ([org1]-[sub-organisation-of]->[org8] should come from the transformer)
	// Result: [org3]-[sub-organisation-of]->[org1] should be transferred

	db := getDatabaseConnectionAndCheckClean(t, assert, concordedUUIDs)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert, concordedUUIDs)

	//Step1: write nodes
	assert.NoError(cypherDriver.Write(org1))
	assert.NoError(cypherDriver.Write(org2))
	assert.NoError(cypherDriver.Write(org3))
	assert.NoError(cypherDriver.Write(org8))

	_, found, _ := cypherDriver.Read(org1UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org1UUID)
	_, found, _ = cypherDriver.Read(org2UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org2UUID)
	_, found, _ = cypherDriver.Read(org3UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org3UUID)
	_, found, _ = cypherDriver.Read(org8UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org8UUID)

	//Step 2: concorde org1 with org2
	updatedOrg1 := organisation{
		UUID: org1UUID,
		Type: Organisation,
		AlternativeIdentifiers: alternativeIdentifiers{
			FactsetIdentifier: fsOrg1Identifier,
			UUIDS:             []string{org1UUID, org2UUID},
			TME:               []string{tmeOrg2Identifier},
		}, // should come out from the transformer like this, otherwise won't be merged
		ProperName:         "Updated Name",
		ParentOrganisation: org8UUID, // should come out from the transformer - otherwise won't be transferred
	}

	assert.NoError(cypherDriver.Write(updatedOrg1))

	//Step3: check results
	// -> no org2
	_, found, _ = cypherDriver.Read(org1UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org1UUID)
	_, found, _ = cypherDriver.Read(org2UUID)
	assert.False(found, "Organisation for uuid %s should have been concorded", org2UUID)
	_, found, _ = cypherDriver.Read(org3UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org3UUID)
	_, found, _ = cypherDriver.Read(org8UUID)
	assert.True(found, "Didn't find organisation for uuid %s", org8UUID)

	// -> org1 present with incoming and outgoing sub-organisation-of relationships
	storedOrg1, _, _ := cypherDriver.Read(org1UUID)
	assert.Equal(updatedOrg1, storedOrg1, "orgs should be equal ")

	transferredPropertyLR, transferredPropertyRL, err := readRelationshipDetails(cypherDriver.cypherRunner, "Thing", org1UUID)
	assert.Nil(err)
	assert.Equal(1, len(transferredPropertyLR))
	assert.Contains(transferredPropertyLR, property{Type: "SUB_ORGANISATION_OF", PlatformVersion: ""})
	assert.Equal(1, len(transferredPropertyRL))
	assert.Contains(transferredPropertyRL, property{Type: "SUB_ORGANISATION_OF", PlatformVersion: ""})

}

// Check that alternative nodes are deleted at concordence, but identifiers are kept
func TestConcordeOrgsAndDeleteAlternativeNodes(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert, concordedUUIDs)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert, concordedUUIDs)

	updatedOrg1 := organisation{
		UUID: org1UUID,
		Type: Organisation,
		AlternativeIdentifiers: alternativeIdentifiers{
			FactsetIdentifier: fsOrg1Identifier,
			UUIDS:             []string{org1UUID, org2UUID},
			TME:               []string{},
		},
		ProperName: "Updated Name",
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

// Concorde relationships with the same platformVersion - if any
func TestConcordeOrgsWithRelationshipPlatformVersionTransfer(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert, concordedUUIDs)
	cypherDriver := getCypherDriver(db)

	annotationsRW := annotations.NewAnnotationsService(cypherDriver.cypherRunner, db, "v1")
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
		UUID: org1UUID,
		Type: Organisation,
		AlternativeIdentifiers: alternativeIdentifiers{
			FactsetIdentifier: fsOrg8Identifier,
			UUIDS:             []string{org1UUID, org2UUID},
			TME:               []string{},
		},
		ProperName:         "Updated Name",
		ParentOrganisation: org8UUID,
	}

	writeJSONToService(annotationsRW, "./test-resources/annotationBodyForOrg2.json", contentUUID, assert)
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

	transferredPropertyLR, transferredPropertyRL, err := readRelationshipDetails(cypherDriver.cypherRunner, "Thing", org1UUID)
	assert.Nil(err)
	assert.Equal(2, len(transferredPropertyLR))
	assert.Contains(transferredPropertyLR, property{Type: "MENTIONS", PlatformVersion: "v1"})
	assert.Contains(transferredPropertyLR, property{Type: "ABOUT", PlatformVersion: "v1"})
	assert.Equal(1, len(transferredPropertyRL))
	assert.Contains(transferredPropertyRL, property{Type: "SUB_ORGANISATION_OF", PlatformVersion: ""})
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

type property struct {
	Type            string `json:"name"`
	PlatformVersion string `json:"r.platformVersion"`
}

// return relationship details in both directions
func readRelationshipDetails(cypherRunner neoutils.CypherRunner, contentType string, orgUUID string) ([]property, []property, error) {

	transferredLRProperty := []property{}
	readRelationshipsQueryLR := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`match (co:%s)-[r]->(c:Thing{uuid:{uuid}})
 				return r.platformVersion, type(r) as name`, contentType),
		Parameters: map[string]interface{}{
			"uuid": orgUUID,
		},
		Result: &transferredLRProperty,
	}

	transferredRLProperty := []property{}
	readRelationshipsQueryRL := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`match (co:%s)<-[r]-(c:Thing{uuid:{uuid}})
 				return r.platformVersion, type(r) as name`, contentType),
		Parameters: map[string]interface{}{
			"uuid": orgUUID,
		},
		Result: &transferredRLProperty,
	}

	err := cypherRunner.CypherBatch([]*neoism.CypherQuery{readRelationshipsQueryLR, readRelationshipsQueryRL})

	return transferredLRProperty, transferredRLProperty, err
}
