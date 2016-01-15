package roles

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Financial-Times/go-fthealth/v1a"
	"github.com/Financial-Times/neo-cypher-runner-go"
	"github.com/Financial-Times/neo-utils-go"
	"github.com/jmcvetta/neoism"
)

const (
	fsAuthority = "http://api.ft.com/system/FACTSET"
)

// CypherDriver - CypherDriver
type CypherDriver struct {
	cypherRunner neocypherrunner.CypherRunner
	indexManager neoutils.IndexManager
}

//NewCypherDriver instantiate driver
func NewCypherDriver(cypherRunner neocypherrunner.CypherRunner, indexManager neoutils.IndexManager) CypherDriver {
	return CypherDriver{cypherRunner, indexManager}
}

//Initialise initialisation of the indexes
func (pcd CypherDriver) Initialise() error {
	return neoutils.EnsureIndexes(pcd.indexManager, map[string]string{"Role": "uuid"})
}

// Check - Feeds into the Healthcheck and checks whether we can connect to Neo and that the datastore isn't empty
func (pcd CypherDriver) Check() (check v1a.Check) {
	type hcUUIDResult struct {
		ID string `json:"ID"`
	}

	checker := func() (string, error) {
		var result []hcUUIDResult

		query := &neoism.CypherQuery{
			Statement: `MATCH (n)
					return ID(n) as ID
					limit 1`,
			Result: &result,
		}

		err := pcd.cypherRunner.CypherBatch([]*neoism.CypherQuery{query})

		if err != nil {
			return "", err
		}
		if len(result) == 0 {
			return "", errors.New("Nothing Found in this Neo4J instance")
		}
		return fmt.Sprintf("Found something with a valid ID = %v", result[0].ID), nil
	}

	return v1a.Check{
		BusinessImpact:   "Cannot read/write roles via this writer",
		Name:             "Check connectivity to Neo4j - neoUrl is a parameter in hieradata for this service",
		PanicGuide:       "TODO - write panic guide",
		Severity:         1,
		TechnicalSummary: fmt.Sprintf("Cannot connect to Neo4j instance %s with at least one node loaded in it", pcd.cypherRunner),
		Checker:          checker,
	}
}

// Read - reads a role given a UUID
func (pcd CypherDriver) Read(uuid string) (interface{}, bool, error) {
	results := []struct {
		UUID              string `json:"uuid"`
		PrefLabel         string `json:"prefLabel"`
		FactsetIdentifier string `json:"factsetIdentifier"`
	}{}

	query := &neoism.CypherQuery{
		Statement: `MATCH (n:Role {uuid:{uuid}}) return n.uuid
		as uuid, n.prefLabel as prefLabel,
		n.factsetIdentifier as factsetIdentifier`,
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

	if result.FactsetIdentifier != "" {
		r.Identifiers = append(r.Identifiers, identifier{fsAuthority, result.FactsetIdentifier})
	}
	return r, true, nil
}

//Write - Writes a Role node
func (pcd CypherDriver) Write(thing interface{}) error {
	r := thing.(role)

	params := map[string]interface{}{
		"uuid": r.UUID,
	}

	if r.PrefLabel != "" {
		params["prefLabel"] = r.PrefLabel
	}

	for _, identifier := range r.Identifiers {
		if identifier.Authority == fsAuthority {
			params["factsetIdentifier"] = identifier.IdentifierValue
		}
	}

	// TODO set BoardRole if isBoardRole is True
	statement := `MERGE (n:Thing {uuid: {uuid}})
				set n={allprops}
				set n :Role`

	if r.IsBoardRole {
		statement += ` set n :BoardRole`
	}
	query := &neoism.CypherQuery{
		Statement: statement,
		Parameters: map[string]interface{}{
			"uuid":     r.UUID,
			"allprops": params,
		},
	}

	return pcd.cypherRunner.CypherBatch([]*neoism.CypherQuery{query})

}

//Delete - Deletes a Role
func (pcd CypherDriver) Delete(uuid string) (bool, error) {
	clearNode := &neoism.CypherQuery{
		Statement: `
			MATCH (p:Thing {uuid: {uuid}})
			REMOVE p:Role
			REMOVE p:BoardRole
			SET p={props}
		`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
			"props": map[string]interface{}{
				"uuid": uuid,
			},
		},
		IncludeStats: true,
	}

	removeNodeIfUnused := &neoism.CypherQuery{
		Statement: `
			MATCH (p:Thing {uuid: {uuid}})
			OPTIONAL MATCH (p)-[a]-(x)
			WITH p, count(a) AS relCount
			WHERE relCount = 0
			DELETE p
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
