// +build !jenkins

package roles

import (
	"os"
	"testing"

	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/stretchr/testify/assert"
)

var rolesDriver baseftrwapp.Service

func TestDelete(t *testing.T) {
	uuid := "12345"

	rolesDriver = getRolesCypherDriver(t)

	altId := alternativeIdentifiers{UUIDS: []string{"UUID"}, FactsetIdentifier: "FACTSETID"}
	roleToDelete := role{UUID: uuid, PrefLabel: "TestRole", AlternativeIdentifiers: altId}

	assert.NoError(t, rolesDriver.Write(roleToDelete), "Failed to write role")

	found, err := rolesDriver.Delete(uuid)
	assert.True(t, found, "Didn't manage to delete role for uuid %", uuid)
	assert.NoError(t, err, "Error deleting role for uuid %s", uuid)

	p, found, err := rolesDriver.Read(uuid)

	assert.Equal(t, role{}, p, "Found role %s who should have been deleted", p)
	assert.False(t, found, "Found role for uuid %s who should have been deleted", uuid)
	assert.NoError(t, err, "Error trying to find role for uuid %s", uuid)
}

func TestCreateAllValuesPresent(t *testing.T) {
	uuid := "12345"
	rolesDriver = getRolesCypherDriver(t)

	altId := alternativeIdentifiers{UUIDS: []string{"UUID"}, FactsetIdentifier: "FACTSETID"}
	roleToWrite := role{UUID: uuid, PrefLabel: "TestRole", AlternativeIdentifiers: altId}

	assert.NoError(t, rolesDriver.Write(roleToWrite), "Failed to write role")

	readRoleForUUIDAndCheckFieldsMatch(t, uuid, roleToWrite)

	cleanUp(t, uuid)
}

func TestCreateNoFactsetIdentifierPresent(t *testing.T) {
	uuid := "12345"
	rolesDriver = getRolesCypherDriver(t)

	altId := alternativeIdentifiers{UUIDS: []string{"UUID"}}
	roleToWrite := role{UUID: uuid, PrefLabel: "TestRole", AlternativeIdentifiers: altId}

	assert.NoError(t, rolesDriver.Write(roleToWrite), "Failed to write role")

	readRoleForUUIDAndCheckFieldsMatch(t, uuid, roleToWrite)

	cleanUp(t, uuid)
}

func TestCreateHandlesSpecialCharacters(t *testing.T) {
	uuid := "12345"
	rolesDriver = getRolesCypherDriver(t)

	altId := alternativeIdentifiers{UUIDS: []string{"UUID"}, FactsetIdentifier: "FACTSETID"}
	roleToWrite := role{UUID: uuid, PrefLabel: "TestRole", AlternativeIdentifiers: altId}

	assert.NoError(t, rolesDriver.Write(roleToWrite), "Failed to write role")

	readRoleForUUIDAndCheckFieldsMatch(t, uuid, roleToWrite)

	cleanUp(t, uuid)
}

func TestCreateNotAllValuesPresent(t *testing.T) {
	uuid := "12345"
	rolesDriver = getRolesCypherDriver(t)

	altId := alternativeIdentifiers{UUIDS: []string{"UUID"}, FactsetIdentifier: "FACTSETID"}
	roleToWrite := role{UUID: uuid, PrefLabel: "TestRole", AlternativeIdentifiers: altId}

	assert.NoError(t, rolesDriver.Write(roleToWrite), "Failed to write role")

	readRoleForUUIDAndCheckFieldsMatch(t, uuid, roleToWrite)

	cleanUp(t, uuid)
}

func TestCreateAddsBoardRoleLabelForBoardRole(t *testing.T) {
	uuid := "12345"
	rolesDriver = getRolesCypherDriver(t)

	altId := alternativeIdentifiers{UUIDS: []string{"UUID"}, FactsetIdentifier: "FACTSETID"}
	roleToWrite := role{UUID: uuid, PrefLabel: "TestRole", AlternativeIdentifiers: altId}
	assert.NoError(t, rolesDriver.Write(roleToWrite), "Failed to write role")

	readRoleForUUIDAndCheckFieldsMatch(t, uuid, roleToWrite)

	cleanUp(t, uuid)

}

func readRoleForUUIDAndCheckFieldsMatch(t *testing.T, uuid string, expectedRole role) {
	storedRole, found, err := rolesDriver.Read(uuid)

	assert.NoError(t, err, "Error finding role for uuid %s", uuid)
	assert.True(t, found, "Didn't find role for uuid %s", uuid)
	assert.Equal(t, expectedRole, storedRole, "roles should be the same")
}

func getRolesCypherDriver(t *testing.T) CypherDriver {
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}

	conf := neoutils.DefaultConnectionConfig()
	conf.Transactional = false
	db, err := neoutils.Connect(url, conf)
	assert.NoError(t, err, "Failed to connect to Neo4j")
	cr := NewCypherDriver(db)
	cr.Initialise()
	return cr
}

func cleanUp(t *testing.T, uuid string) {
	found, err := rolesDriver.Delete(uuid)
	assert.True(t, found, "Didn't manage to delete role for uuid %", uuid)
	assert.NoError(t, err, "Error deleting role for uuid %s", uuid)
}
