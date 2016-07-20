// +build !jenkins

package roles

import (
	"os"
	"testing"

	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
)

var rolesDriver baseftrwapp.Service

func TestDelete(t *testing.T) {
	assert := assert.New(t)
	uuid := "12345"

	rolesDriver = getRolesCypherDriver(t)

	altId := alternativeIdentifiers{UUIDS: []string{"UUID"}, FactsetIdentifier: "FACTSETID"}
	roleToDelete := role{UUID: uuid, PrefLabel: "TestRole", AlternativeIdentifiers: altId}

	assert.NoError(rolesDriver.Write(roleToDelete), "Failed to write role")

	found, err := rolesDriver.Delete(uuid)
	assert.True(found, "Didn't manage to delete role for uuid %", uuid)
	assert.NoError(err, "Error deleting role for uuid %s", uuid)

	p, found, err := rolesDriver.Read(uuid)

	assert.Equal(role{}, p, "Found role %s who should have been deleted", p)
	assert.False(found, "Found role for uuid %s who should have been deleted", uuid)
	assert.NoError(err, "Error trying to find role for uuid %s", uuid)
}

func TestCreateAllValuesPresent(t *testing.T) {
	assert := assert.New(t)
	uuid := "12345"
	rolesDriver = getRolesCypherDriver(t)

	altId := alternativeIdentifiers{UUIDS: []string{"UUID"}, FactsetIdentifier: "FACTSETID"}
	roleToWrite := role{UUID: uuid, PrefLabel: "TestRole", AlternativeIdentifiers: altId}

	assert.NoError(rolesDriver.Write(roleToWrite), "Failed to write role")

	readRoleForUUIDAndCheckFieldsMatch(t, uuid, roleToWrite)

	cleanUp(t, uuid)
}

func TestCreateNoFactsetIdentifierPresent(t *testing.T) {
	assert := assert.New(t)
	uuid := "12345"
	rolesDriver = getRolesCypherDriver(t)

	altId := alternativeIdentifiers{UUIDS: []string{"UUID"}}
	roleToWrite := role{UUID: uuid, PrefLabel: "TestRole", AlternativeIdentifiers: altId}

	assert.NoError(rolesDriver.Write(roleToWrite), "Failed to write role")

	readRoleForUUIDAndCheckFieldsMatch(t, uuid, roleToWrite)

	cleanUp(t, uuid)
}

func TestCreateHandlesSpecialCharacters(t *testing.T) {
	assert := assert.New(t)
	uuid := "12345"
	rolesDriver = getRolesCypherDriver(t)

	altId := alternativeIdentifiers{UUIDS: []string{"UUID"}, FactsetIdentifier: "FACTSETID"}
	roleToWrite := role{UUID: uuid, PrefLabel: "TestRole", AlternativeIdentifiers: altId}

	assert.NoError(rolesDriver.Write(roleToWrite), "Failed to write role")

	readRoleForUUIDAndCheckFieldsMatch(t, uuid, roleToWrite)

	cleanUp(t, uuid)
}

func TestCreateNotAllValuesPresent(t *testing.T) {
	assert := assert.New(t)
	uuid := "12345"
	rolesDriver = getRolesCypherDriver(t)

	altId := alternativeIdentifiers{UUIDS: []string{"UUID"}, FactsetIdentifier: "FACTSETID"}
	roleToWrite := role{UUID: uuid, PrefLabel: "TestRole", AlternativeIdentifiers: altId}

	assert.NoError(rolesDriver.Write(roleToWrite), "Failed to write role")

	readRoleForUUIDAndCheckFieldsMatch(t, uuid, roleToWrite)

	cleanUp(t, uuid)
}

func TestCreateAddsBoardRoleLabelForBoardRole(t *testing.T) {
	assert := assert.New(t)
	uuid := "12345"
	rolesDriver = getRolesCypherDriver(t)

	altId := alternativeIdentifiers{UUIDS: []string{"UUID"}, FactsetIdentifier: "FACTSETID"}
	roleToWrite := role{UUID: uuid, PrefLabel: "TestRole", AlternativeIdentifiers: altId}
	assert.NoError(rolesDriver.Write(roleToWrite), "Failed to write role")

	readRoleForUUIDAndCheckFieldsMatch(t, uuid, roleToWrite)

	cleanUp(t, uuid)

}

func readRoleForUUIDAndCheckFieldsMatch(t *testing.T, uuid string, expectedRole role) {
	assert := assert.New(t)
	storedRole, found, err := rolesDriver.Read(uuid)

	assert.NoError(err, "Error finding role for uuid %s", uuid)
	assert.True(found, "Didn't find role for uuid %s", uuid)
	assert.Equal(expectedRole, storedRole, "roles should be the same")
}

func getRolesCypherDriver(t *testing.T) CypherDriver {
	assert := assert.New(t)
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}

	db, err := neoism.Connect(url)
	assert.NoError(err, "Failed to connect to Neo4j")
	return NewCypherDriver(neoutils.StringerDb{db}, db)
}

func cleanUp(t *testing.T, uuid string) {
	assert := assert.New(t)
	found, err := rolesDriver.Delete(uuid)
	assert.True(found, "Didn't manage to delete role for uuid %", uuid)
	assert.NoError(err, "Error deleting role for uuid %s", uuid)
}
