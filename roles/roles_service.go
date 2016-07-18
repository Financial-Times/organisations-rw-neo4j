package roles

import (
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
)

const (
	fsAuthority = "http://api.ft.com/system/FACTSET"
)

// CypherDriver - CypherDriver
type CypherDriver struct {
	cypherRunner neoutils.CypherRunner
	indexManager neoutils.IndexManager
}

//NewCypherDriver instantiate driver
func NewCypherDriver(cypherRunner neoutils.CypherRunner, indexManager neoutils.IndexManager) CypherDriver {
	return CypherDriver{cypherRunner, indexManager}
}

//Initialise initialisation of the indexes
func (pcd CypherDriver) Initialise() error {

	err := neoutils.EnsureIndexes(pcd.indexManager,  map[string]string{
		"Identifier": "value",
	})

	if err != nil {
		return err
	}

	return neoutils.EnsureConstraints(pcd.indexManager, map[string]string{
		"Role":              "uuid",
		"FactsetIdentifier": "value",
		"UPPIdentifier":     "value"})
}

// Check - Feeds into the Healthcheck and checks whether we can connect to Neo and that the datastore isn't empty
func (pcd CypherDriver) Check() error {
	return neoutils.Check(pcd.cypherRunner)
}

// Read - reads a role given a UUID
func (pcd CypherDriver) Read(uuid string) (interface{}, bool, error) {
	results := []struct {
		UUID              string   `json:"uuid"`
		PrefLabel         string   `json:"prefLabel"`
		FactsetIdentifier string   `json:"factsetIdentifier"`
		UUIDs             []string `json:"uuids"`
		Labels            []string `json:"labels"`
	}{}

	query := &neoism.CypherQuery{
		Statement: `MATCH (n:Role {uuid:{uuid}})
		OPTIONAL MATCH (upp:UPPIdentifier)-[:IDENTIFIES]->(n)
		OPTIONAL MATCH (factset:FactsetIdentifier)-[:IDENTIFIES]->(n)
		return n.uuid
		as uuid, n.prefLabel as prefLabel,
		factset.value as factsetIdentifier,
		collect(upp.value) as uuids,
		labels(n) as labels`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}

	err := pcd.cypherRunner.CypherBatch([]*neoism.CypherQuery{query})

	if err != nil {
		return role{}, false, err
	}

	if len(results) == 0 {
		return role{}, false, nil
	}

	result := results[0]

	r := role{
		UUID:      result.UUID,
		PrefLabel: result.PrefLabel,
	}

	for labelLocation := range result.Labels {
		if result.Labels[labelLocation] == "BoardRole" {
			r.IsBoardRole = true
		}
	}

	if result.FactsetIdentifier != "" {
		r.AlternativeIdentifiers.FactsetIdentifier = result.FactsetIdentifier
	}

	r.AlternativeIdentifiers.UUIDS = result.UUIDs

	return r, true, nil
}

//Write - Writes a Role node
func (pcd CypherDriver) Write(thing interface{}) error {
	roleToWrite := thing.(role)

	//cleanUP all the previous IDENTIFIERS referring to that uuid
	deletePreviousIdentifiersQuery := &neoism.CypherQuery{
		Statement: `MATCH (t:Thing {uuid:{uuid}})
		OPTIONAL MATCH (t)<-[iden:IDENTIFIES]-(i)
		DELETE iden, i
		REMOVE t:BoardRole`,
		Parameters: map[string]interface{}{
			"uuid": roleToWrite.UUID,
		},
	}

	//create-update node for ROLE
	statement := `MERGE (n:Thing {uuid: {uuid}})
				set n={allprops}
				set n :Role`

	if roleToWrite.IsBoardRole {
		statement += ` set n :BoardRole`
	}

	createRoleQuery := &neoism.CypherQuery{
		Statement: statement,
		Parameters: map[string]interface{}{
			"uuid": roleToWrite.UUID,
			"allprops": map[string]interface{}{
				"uuid":      roleToWrite.UUID,
				"prefLabel": roleToWrite.PrefLabel,
			},
		},
	}

	queryBatch := []*neoism.CypherQuery{deletePreviousIdentifiersQuery, createRoleQuery}

	for _, alternativeUUID := range roleToWrite.AlternativeIdentifiers.UUIDS {
		alternativeIdentifierQuery := createNewIdentifierQuery(roleToWrite.UUID, uppIdentifierLabel, alternativeUUID)
		queryBatch = append(queryBatch, alternativeIdentifierQuery)
	}

	queryBatch = append(queryBatch, createNewIdentifierQuery(roleToWrite.UUID, factsetIdentifierLabel, roleToWrite.AlternativeIdentifiers.FactsetIdentifier))

	return pcd.cypherRunner.CypherBatch([]*neoism.CypherQuery(queryBatch))
}

func createNewIdentifierQuery(uuid string, identifierLabel string, identifierValue string) *neoism.CypherQuery {
	statementTemplate := fmt.Sprintf(`MERGE (t:Thing {uuid:{uuid}})
					CREATE (i:Identifier {value:{value}})
					MERGE (t)<-[:IDENTIFIES]-(i)
					set i : %s `, identifierLabel)
	query := &neoism.CypherQuery{
		Statement: statementTemplate,
		Parameters: map[string]interface{}{
			"uuid":  uuid,
			"value": identifierValue,
		},
	}
	return query
}

//Delete - Deletes a Role
func (pcd CypherDriver) Delete(uuid string) (bool, error) {
	clearNode := &neoism.CypherQuery{
		Statement: `
			MATCH (t:Thing {uuid: {uuid}})
			OPTIONAL MATCH (t)<-[iden:IDENTIFIES]-(i:Identifier)
			REMOVE t:Role
			REMOVE t:BoardRole
			DELETE iden, i
			SET t = {uuid:{uuid}}
		`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		IncludeStats: true,
	}

	removeNodeIfUnused := &neoism.CypherQuery{
		Statement: `
			MATCH (t:Thing {uuid: {uuid}})
			OPTIONAL MATCH (t)-[a]-(x)
			WITH t, count(a) AS relCount
			WHERE relCount = 0
			DELETE t
		`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
	}

	err := pcd.cypherRunner.CypherBatch([]*neoism.CypherQuery{clearNode, removeNodeIfUnused})

	s1, err := clearNode.Stats()
	if err != nil {
		return false, err
	}

	var deleted bool
	if s1.ContainsUpdates && s1.LabelsRemoved > 0 {
		deleted = true
	}

	return deleted, err
}

// DecodeJSON - Decodes JSON into role
func (pcd CypherDriver) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	r := role{}
	err := dec.Decode(&r)
	return r, r.UUID, err

}

// Count - Returns a count of the number of roles in this Neo instance
func (pcd CypherDriver) Count() (int, error) {

	results := []struct {
		Count int `json:"c"`
	}{}

	query := &neoism.CypherQuery{
		Statement: `MATCH (n:Role) return count(n) as c`,
		Result:    &results,
	}

	err := pcd.cypherRunner.CypherBatch([]*neoism.CypherQuery{query})

	if err != nil {
		return 0, err
	}

	return results[0].Count, nil
}
