package organisations

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
)

type service struct {
	cypherRunner neoutils.CypherRunner
	indexManager neoutils.IndexManager
}

func NewCypherOrganisationService(cypherRunner neoutils.CypherRunner, indexManager neoutils.IndexManager) service {
	return service{cypherRunner, indexManager}
}

func (cd service) Initialise() error {
	return neoutils.EnsureConstraints(cd.indexManager, map[string]string{
		"Thing":        "uuid",
		"Concept":      "uuid",
		"Organisation": "uuid"})
}

func setProps(props *map[string]interface{}, item *string, propName string) {
	if *item != "" {
		(*props)[propName] = *item
	}
}

func setListProps(props *map[string]interface{}, itemList *[]string, propName string) {
	var items []string

	for _, item := range *itemList {
		items = append(items, item)
	}

	if len(items) > 0 {
		(*props)[propName] = items
	}
}

//Write - Writes an Organisation node
func (cd service) Write(thing interface{}) error {
	o := thing.(organisation)

	props := map[string]interface{}{
		"uuid":       o.UUID,
		"properName": o.ProperName,
		"prefLabel":  o.ProperName,
	}

	setProps(&props, &o.LegalName, "legalName")
	setProps(&props, &o.ShortName, "shortName")
	setProps(&props, &o.HiddenLabel, "hiddenLabel")

	var tmeIdentifiers []string

	for _, identifier := range o.Identifiers {
		if identifier.Authority == fsAuthority {
			props["factsetIdentifier"] = identifier.IdentifierValue
		}
		if identifier.Authority == leiIdentifier {
			props["leiCode"] = identifier.IdentifierValue
		}
		if identifier.Authority == tmeAuthority {
			tmeIdentifiers = append(tmeIdentifiers, identifier.IdentifierValue)
		}
	}

	if len(tmeIdentifiers) > 0 {
		props["tmeIdentifiers"] = tmeIdentifiers
	}

	setListProps(&props, &o.FormerNames, "formerNames")
	setListProps(&props, &o.LocalNames, "localNames")
	setListProps(&props, &o.TradeNames, "tradeNames")
	setListProps(&props, &o.Aliases, "aliases")

	deleteEntityRelationshipsQuery := &neoism.CypherQuery{
		Statement: `MATCH (o:Thing {uuid:{uuid}})
					OPTIONAL MATCH (o)-[hc:HAS_CLASSIFICATION]->(ic)
					OPTIONAL MATCH (o)-[soo:SUB_ORGANISATION_OF]->(p)
					DELETE hc, soo`,
		Parameters: map[string]interface{}{
			"uuid": o.UUID,
		},
	}

	// fmt.Printf("Delete rels query: ", deleteEntityRelationshipsQuery)

	queries := []*neoism.CypherQuery{deleteEntityRelationshipsQuery}

	var statement bytes.Buffer
	statement.WriteString(`MERGE (o:Thing {uuid: {uuid}})
					REMOVE o:PublicCompany:Company:Organisation:Concept:Thing
					SET o={props} `)

	err, stringType := o.Type.String()
	if err == nil {
		statement.WriteString("SET o:" + stringType + " ")
	} else {
		return err
	}

	if o.IndustryClassification != "" {
		statement.WriteString("MERGE (ic:Thing{uuid:'" + o.IndustryClassification + "'}) MERGE (o)-[:HAS_CLASSIFICATION]->(ic) ")
	}

	if o.ParentOrganisation != "" {
		statement.WriteString("MERGE (p:Thing{uuid:'" + o.ParentOrganisation + "'}) MERGE (o)-[:SUB_ORGANISATION_OF]->(p) ")
	}

	writeQuery := &neoism.CypherQuery{
		Statement: statement.String(),
		Parameters: map[string]interface{}{
			"uuid":  o.UUID,
			"props": props,
		},
	}

	// fmt.Printf("Write Query:", writeQuery)
	queries = append(queries, writeQuery)

	fmt.Printf("Full Query:", queries)
	return cd.cypherRunner.CypherBatch(queries)
}

//Read - Internal Read of an Organisation
func (cd service) Read(uuid string) (interface{}, bool, error) {
	results := []struct {
		UUID              string   `json:"o.uuid"`
		Type              []string `json:"type"`
		ProperName        string   `json:"o.properName"`
		LegalName         string   `json:"o.legalName"`
		ShortName         string   `json:"o.shortName"`
		HiddenLabel       string   `json:"o.hiddenLabel"`
		FactsetIdentifier string   `json:"o.factsetIdentifier"`
		TMEIdentifiers    []string `json:"tmeIdentifiers"`
		LeiCode           string   `json:"o.leiCode"`
		TradeNames        []string `json:"o.tradeNames"`
		LocalNames        []string `json:"o.localNames"`
		FormerNames       []string `json:"o.formerNames"`
		Aliases           []string `json:"o.aliases"`
		ParentOrgUUID     string   `json:"par.uuid"`
		IndustryUUID      string   `json:"ind.uuid"`
	}{}

	readQuery := &neoism.CypherQuery{
		Statement: `MATCH (o:Organisation:Concept{uuid:{uuid}})
            OPTIONAL MATCH (o)-[:SUB_ORGANISATION_OF]->(par:Thing) OPTIONAL MATCH (o)-[:HAS_CLASSIFICATION]->(ind:Thing)
            RETURN o.uuid, o.properName, labels(o) AS type, o.factsetIdentifier, o.tmeIdentifiers, o.leiCode, o.legalName, o.shortName, o.hiddenLabel,
            o.formerNames, o.tradeNames, o.localNames, o.aliases, ind.uuid, par.uuid`,

		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}

	if err := cd.cypherRunner.CypherBatch([]*neoism.CypherQuery{readQuery}); err != nil || len(results) == 0 {
		return organisation{}, false, err
	}

	result := results[0]

	o := organisation{
		UUID:                   result.UUID,
		ProperName:             result.ProperName,
		LegalName:              result.LegalName,
		ShortName:              result.ShortName,
		HiddenLabel:            result.HiddenLabel,
		TradeNames:             result.TradeNames,
		LocalNames:             result.LocalNames,
		FormerNames:            result.FormerNames,
		Aliases:                result.Aliases,
		ParentOrganisation:     result.ParentOrgUUID,
		IndustryClassification: result.IndustryUUID,
	}

	addType(&o.Type, &result.Type)

	if result.FactsetIdentifier != "" {
		addIdentifier(&o.Identifiers, &result.FactsetIdentifier, fsAuthority)
	}
	if result.LeiCode != "" {
		addIdentifier(&o.Identifiers, &result.LeiCode, leiIdentifier)
	}

	for _, tmeValue := range result.TMEIdentifiers {
		o.Identifiers = append(o.Identifiers, identifier{tmeAuthority, tmeValue})
	}

	/*if result.FactsetIdentifier == "" && result.LeiCode == "" &&  len(result.TMEIdentifiers)==0{
		o.Identifiers = make([]identifier, 0, 0)
	}*/
	return o, true, nil
}

func addType(orgType *OrgType, types *[]string) {
	i := len(*types)
	if i == 3 {
		*orgType = Organisation
	}
	if i == 4 {
		*orgType = Company
	}
	if i == 5 {
		*orgType = PublicCompany
	}
}

func addIdentifier(identifiers *[]identifier, result *string, code string) {

	if *result != "" {
		(*identifiers) = append(*identifiers, identifier{code, *result})
	}
}

//Delete - Deletes an Organisation
func (pcd service) Delete(uuid string) (bool, error) {
	clearNode := &neoism.CypherQuery{
		Statement: `
			MATCH (org:Thing {uuid: {uuid}})
			REMOVE org:Concept:Organisation:Company:PublicCompany SET org={ uuid: {uuid}}
		`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		IncludeStats: true,
	}

	qs := []*neoism.CypherQuery{
		clearNode,
		{
			Statement: `
		MATCH (org:Thing {uuid: {uuid}})
		OPTIONAL MATCH (org)-[a]-(x) WITH org, count(a) AS relCount WHERE relCount = 0
		DELETE org
		`,
			Parameters: map[string]interface{}{
				"uuid": uuid,
			},
		},
	}

	err := pcd.cypherRunner.CypherBatch(qs)

	s1, err := clearNode.Stats()

	if err != nil {
		return false, err
	}

	if s1.ContainsUpdates && s1.LabelsRemoved > 0 {
		return true, err
	}

	return false, err
}

func (s service) Check() error {
	return neoutils.Check(s.cypherRunner)
}

type countResult []struct {
	Count int `json:"c"`
}

func (s service) Count() (int, error) {

	results := countResult{}

	err := s.cypherRunner.CypherBatch([]*neoism.CypherQuery{{
		Statement: `MATCH (n:Organisation) return count(n) as c`,
		Result:    &results,
	}})

	if err != nil {
		return 0, err
	}

	return results[0].Count, nil
}

func (s service) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	org := organisation{}
	err := dec.Decode(&org)
	return org, org.UUID, err

}

const (
	fsAuthority   = "http://api.ft.com/system/FACTSET-EDM"
	leiIdentifier = "http://api.ft.com/system/LEI"
	tmeAuthority  = "http://api.ft.com/system/TME"
)
