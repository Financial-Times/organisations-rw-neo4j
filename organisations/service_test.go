package organisations

import (
	"github.com/Financial-Times/neo-utils-go"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestWrite(t *testing.T) {
	assert := assert.New(t)
	uuid := "4e484678-cf47-4168-b844-6adb47f8eb58"

	db := getDatabaseConnection(t)
	checkDbClean(db, t)
	cypherDriver := getCypherDriver(db)
	fsIdentifier := identifier{
		Authority:       fsAuthority,
		IdentifierValue: "identifierValue",
	}
	lieCodeIdentifier := identifier{
		Authority:       leiIdentifier,
		IdentifierValue: "lieCodeIdentifier",
	}
	org := organisation{
		UUID:                   uuid,
		Type:                   Organisation,
		Identifiers:            []identifier{fsIdentifier, lieCodeIdentifier},
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

	assert.NoError(cypherDriver.Write(org))
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
func getCypherDriver(db *neoism.Database) CypherDriver {
	return NewCypherDriver(neoutils.StringerDb{db}, db)
}
