package organisations

import (
	"fmt"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/Financial-Times/up-rw-app-api-go/rwapi"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"github.com/Financial-Times/annotations-rw-neo4j/annotations"
)

const (
	fullOrgUUID                = "4e484678-cf47-4168-b844-6adb47f8eb58"
	privateOrgUUID             = "9e01380d-ac20-48a4-b187-88dc57f37e48"
	minimalOrgUUID             = "33f93f25-3301-417e-9b20-50b27d215617"
	oddCharOrgUUID             = "5bb679d7-334e-4d51-a676-b1a10daaab38"
	dupeLeiIdentifierOrgUUID   = "fbe74159-f4a0-4aa0-9cca-c2bbb9e8bffe"
	dupeOtherIdentifierOrgUUID = "4b89a949-a032-4114-9a8c-f59c37170d65"
	parentOrgUUID              = "de38231e-e481-4958-b470-e124b2ef5a34"
	industryClassificationUUID = "c3d17865-f9d1-42f2-9ca2-4801cb5aacc0"
	fsIdentifier               = "identifierValue"
	fsIdentifierMinimal        = "identifierMinimalValue"
	fsIdentifierOther          = "identifierOtherValue"
	fsIdentifierAnother        = "anotherIdentifierValue"
	leiCodeIdentifier          = "leiCodeIdentifier"
	tmeIdentifier              = "tmeIdentifier"
	tmeIdentifierAnother       = "tmeIdentifierAnother"
)

var uuidsToClean = []string{fullOrgUUID, privateOrgUUID, minimalOrgUUID, oddCharOrgUUID, dupeLeiIdentifierOrgUUID, dupeOtherIdentifierOrgUUID, industryClassificationUUID, parentOrgUUID, contentUUID}

var fullOrg = organisation{
	UUID: fullOrgUUID,
	Type: PublicCompany,
	AlternativeIdentifiers: alternativeIdentifiers{
		UUIDS:             []string{fullOrgUUID},
		TME:               []string{tmeIdentifier},
		FactsetIdentifier: fsIdentifier,
		LeiCode:           leiCodeIdentifier,
	},
	ProperName:             "Proper Name",
	PrefLabel:              "Pref label",
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

var privateOrg = organisation{
	UUID: privateOrgUUID,
	Type: Organisation,
	AlternativeIdentifiers: alternativeIdentifiers{
		UUIDS:             []string{privateOrgUUID},
		TME:               []string{tmeIdentifierAnother},
		FactsetIdentifier: fsIdentifierAnother,
		LeiCode:           leiCodeIdentifier,
	},
	ProperName:             "Proper Name Ltd.",
	LegalName:              "Legal Name Ltd.",
	ShortName:              "Short Name Ltd.",
	HiddenLabel:            "Hidden Label Ltd.",
	FormerNames:            []string{"Old Name Ltd., Ltd.", "Older Name, Ltd.."},
	TradeNames:             []string{"Old Trade Name Ltd., Ltd..", "Older Trade Name Ltd., Ltd.."},
	LocalNames:             []string{"Oldé Name Ltd., Ltd..", "Tradé Name Ltd."},
	Aliases:                []string{"alias1", "alias2", "alias3"},
	IndustryClassification: industryClassificationUUID,
}

var minimalOrg = organisation{
	UUID: minimalOrgUUID,
	Type: Organisation,
	AlternativeIdentifiers: alternativeIdentifiers{
		UUIDS:             []string{minimalOrgUUID},
		FactsetIdentifier: fsIdentifierMinimal,
		TME:               []string{},
	},
	ProperName: "Minimal Org Proper Name",
}

var dupeLeiIdentifierOrg = organisation{
	UUID: dupeLeiIdentifierOrgUUID,
	Type: Company,
	AlternativeIdentifiers: alternativeIdentifiers{
		UUIDS:             []string{dupeLeiIdentifierOrgUUID},
		FactsetIdentifier: fsIdentifierOther,
		LeiCode:           leiCodeIdentifier,
		TME:               []string{},
	},
	ProperName: "Dupe Identifier Proper Name",
}

var dupeOtherIdentifierOrg = organisation{
	UUID: dupeOtherIdentifierOrgUUID,
	Type: Company,
	AlternativeIdentifiers: alternativeIdentifiers{
		UUIDS:             []string{dupeOtherIdentifierOrgUUID},
		TME:               []string{tmeIdentifier},
		FactsetIdentifier: fsIdentifierOther,
	},
	ProperName: "Dupe Identifier Proper Name",
}

var oddCharOrg = organisation{
	UUID:       oddCharOrgUUID,
	Type:       Company,
	ProperName: "TBWA\\Paling Walters Ltd.",
	AlternativeIdentifiers: alternativeIdentifiers{
		UUIDS:             []string{oddCharOrgUUID},
		FactsetIdentifier: fsIdentifier,
		LeiCode:           leiCodeIdentifier,
		TME:               []string{},
	},
	ParentOrganisation: parentOrgUUID,
	ShortName:          "TBWA\\Paling Walters",
	FormerNames:        []string{"Paling Elli$ Cognis Ltd.", "Paling Ellis\\/ Ltd.", "Paling Walters Ltd.", "Paling Walter/'s Targis Ltd."},
	HiddenLabel:        "TBWA PALING WALTERS LTD",
}

func TestWriteNewOrganisation(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert, uuidsToClean)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert, uuidsToClean)

	assert.NoError(cypherDriver.Write(fullOrg))

	storedOrg, _, err := cypherDriver.Read(fullOrgUUID)

	assert.NoError(err)
	assert.NotEmpty(storedOrg)
}

func TestWriteWillUpdateOrg(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert, uuidsToClean)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert, uuidsToClean)

	assert.NoError(cypherDriver.Write(minimalOrg))

	storedOrg, _, _ := cypherDriver.Read(minimalOrgUUID)

	assert.Empty(storedOrg.(organisation).HiddenLabel, "Minimal org should not have a hidden label value.")

	updatedOrg := organisation{
		UUID: minimalOrgUUID,
		Type: Organisation,
		AlternativeIdentifiers: alternativeIdentifiers{
			FactsetIdentifier: fsIdentifier,
			TME:               []string{},
			UUIDS:             []string{},
		},
		ProperName:  "Updated Name",
		HiddenLabel: "No longer hidden",
	}

	assert.NoError(cypherDriver.Write(updatedOrg))

	storedUpdatedOrg, _, _ := cypherDriver.Read(minimalOrgUUID)

	assert.Equal(updatedOrg, storedUpdatedOrg, "org should have been updated")
	assert.NotEmpty(storedUpdatedOrg.(organisation).HiddenLabel, "Updated org should have a hidden label value")
}

func TestWritesOrgsWithEscapedCharactersInfields(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert, uuidsToClean)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert, uuidsToClean)

	assert.NoError(cypherDriver.Write(oddCharOrg))

	storedOrg, found, err := cypherDriver.Read(oddCharOrgUUID)

	assert.NoError(err, "Error finding organisation for uuid %s", oddCharOrgUUID)
	assert.True(found, "Didn't find organisation for uuid %s", oddCharOrgUUID)

	assert.True(reflect.DeepEqual(oddCharOrg, storedOrg), fmt.Sprintf("organisations should be the same \n EXPECTED  %+v \n ACTUAL  %+v", oddCharOrg, storedOrg))
}

// This currently can happen as we currently don't support the lifecycle of an organisation
// For example "Bobs Burgers Ltd." spends money on applying for an LEI code and then floats and effectively becomes a
// new organisation "Bobs Burgers Plc." and rather than spend money again on getting an LEI just uses the one it applied
// for as "Bobs Burgers Ltd." Until we deal with this lifecycle we should retain the relationship to both orgs
func TestWritesTwoOrgsWithTheSameLegalEntityIdentifier(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert, uuidsToClean)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert, uuidsToClean)

	assert.NoError(cypherDriver.Write(fullOrg))
	assert.NoError(cypherDriver.Write(privateOrg))

	_, found, err := cypherDriver.Read(privateOrgUUID)

	assert.NoError(err, "Error finding organisation for uuid %s", privateOrgUUID)
	assert.True(found, "Didn't find organisation for uuid %s", privateOrgUUID)

	_, found2, err2 := cypherDriver.Read(fullOrgUUID)

	assert.NoError(err2, "Error finding organisation for uuid %s", fullOrgUUID)
	assert.True(found2, "Didn't find organisation for uuid %s", fullOrgUUID)
}

func TestReadOrganisation(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert, uuidsToClean)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert, uuidsToClean)

	assert.NoError(cypherDriver.Write(fullOrg))

	storedOrg, found, err := cypherDriver.Read(fullOrgUUID)

	assert.NoError(err, "Error finding organisation for uuid %s", fullOrgUUID)
	assert.True(found, "Didn't find organisation for uuid %s", fullOrgUUID)

	assert.True(reflect.DeepEqual(fullOrg, storedOrg), fmt.Sprintf("organisations should be the same \n EXPECTED  %+v \n ACTUAL  %+v", fullOrg, storedOrg))
}

func TestDeleteNothing(t *testing.T) {
	assert := assert.New(t)
	db := getDatabaseConnectionAndCheckClean(t, assert, uuidsToClean)
	defer cleanDB(db, t, assert, uuidsToClean)

	cypherDriver := getCypherDriver(db)
	res, err := cypherDriver.Delete(fullOrgUUID)

	assert.NoError(err)
	assert.False(res)
}

func TestDeleteWillRemoveNodeAndAllAssociatedIfNoExtraRelationships(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert, uuidsToClean)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert, uuidsToClean)

	assert.Nil(cypherDriver.Write(fullOrg))

	found, err := cypherDriver.Delete(fullOrgUUID)
	assert.NoError(err)
	assert.True(found, "Didn't find organisation for uuid %s", fullOrgUUID)

	o, found, err := cypherDriver.Read(fullOrgUUID)

	assert.Equal(organisation{}, o, "Found organisation %v which should have been deleted", o)
	assert.False(found, "Found organisation for uuid %v which should have been deleted", fullOrgUUID)
	assert.NoError(err, "Error trying to find organisation for uuid %v", fullOrgUUID)
	assert.Equal(false, doesThingExistAtAll(fullOrgUUID, db, t, assert), "Found thing which should have been deleted with uuid %v", fullOrgUUID)
}

func TestDeleteWillMaintainExternalRelationshipsOnThingNodeIfRelationshipsExist(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert, uuidsToClean)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert, uuidsToClean)

	assert.Nil(cypherDriver.Write(fullOrg))

	//add external relationship(one not maintained by this service)
	v2AnnotationsRW := annotations.NewCypherAnnotationsService(cypherDriver.conn, "v2", "v2-annotation")
	writeJSONToService(v2AnnotationsRW, "./test-resources/singleAnnotationForFullOrg.json", contentUUID, assert)
	found, err := cypherDriver.Delete(fullOrgUUID)
	assert.True(found, "Didnt manage to delete organisation for uuid %v", fullOrgUUID)
	assert.NoError(err, "Error deleting organisation for uuid %v", fullOrgUUID)

	o, found, err := cypherDriver.Read(fullOrgUUID)

	assert.Equal(organisation{}, o, "Found organisation %v which should have been deleted", o)
	assert.False(found, "Found organisation for uuid %v which should have been deleted", fullOrgUUID)
	assert.NoError(err, "Error trying to find organisation for uuid %v", fullOrgUUID)
	assert.Equal(true, doesThingExistWithIdentifiers(fullOrgUUID, db, t, assert), "Found no trace of organisation which had relationships and thus should still exist as a thing %v", fullOrgUUID)
}

// Temporary solution, until the organisation lifecycle will be correctly managed.
func TestToCheckYouCanCreateOrganisationWithDuplicateLeiIdentifier(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert, uuidsToClean)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert, uuidsToClean)
	assert.NoError(cypherDriver.Write(fullOrg))
	err := cypherDriver.Write(dupeLeiIdentifierOrg)
	assert.Nil(err)

	_, found, err := cypherDriver.Read(fullOrgUUID)
	assert.NoError(err, "Error finding organisation for uuid %s", fullOrgUUID)
	assert.True(found, "Didn't find organisation for uuid %s", fullOrgUUID)

	_, found, err = cypherDriver.Read(dupeLeiIdentifierOrgUUID)
	assert.NoError(err, "Error finding organisation for uuid %s", dupeLeiIdentifierOrgUUID)
	assert.True(found, "Didn't find organisation for uuid %s", dupeLeiIdentifierOrgUUID)
}

// The uniqueness constraint is valid for tme, factset and upp identifiers/*
func TestToCheckYouCanNotCreateOrganisationWithDuplicateIdentifier(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert, uuidsToClean)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert, uuidsToClean)
	assert.NoError(cypherDriver.Write(fullOrg))
	err := cypherDriver.Write(dupeOtherIdentifierOrg)
	assert.Error(err)
	assert.IsType(rwapi.ConstraintOrTransactionError{}, err)
}

func TestCount(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert, uuidsToClean)
	cypherDriver := getCypherDriver(db)
	defer cleanDB(db, t, assert, uuidsToClean)

	assert.NoError(cypherDriver.Write(minimalOrg))
	assert.NoError(cypherDriver.Write(fullOrg))

	count, err := cypherDriver.Count()
	assert.NoError(err)
	assert.Equal(2, count)
}

func doesThingExistAtAll(uuid string, db neoutils.NeoConnection, t *testing.T, assert *assert.Assertions) bool {
	result := []struct {
		Uuid string `json:"thing.uuid"`
	}{}

	checkGraph := neoism.CypherQuery{
		Statement: `
			MATCH (a:Thing {uuid: "%v"}) return a.uuid
		`,
		Parameters: neoism.Props{
			"uuid": uuid,
		},
		Result: &result,
	}

	err := db.CypherBatch([]*neoism.CypherQuery{&checkGraph})
	assert.NoError(err)
	assert.Empty(result)

	if len(result) == 0 {
		return false
	}

	return true
}

func doesThingExistWithIdentifiers(uuid string, db neoutils.NeoConnection, t *testing.T, assert *assert.Assertions) bool {

	result := []struct {
		UUID string `json:"UUID"`
	}{}

	checkGraph := neoism.CypherQuery{
		Statement: `
			MATCH (a:Thing {uuid: {Uuid}})-[:IDENTIFIES]-(:Identifier)
			RETURN distinct a.uuid as UUID
		`,
		Parameters: neoism.Props{
			"Uuid": uuid,
		},
		Result: &result,
	}

	err := db.CypherBatch([]*neoism.CypherQuery{&checkGraph})
	assert.NoError(err)
	assert.NotEmpty(result)

	if len(result) == 0 {
		return false
	}

	return true
}
