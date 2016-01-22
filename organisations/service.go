package organisations

import (
	"bytes"

	"encoding/json"

	"github.com/Financial-Times/neo-cypher-runner-go"
	"github.com/Financial-Times/neo-utils-go"
	"github.com/jmcvetta/neoism"
)

type service struct {
	cypherRunner neocypherrunner.CypherRunner
	indexManager neoutils.IndexManager
}

func NewCypherOrganisationService(cypherRunner neocypherrunner.CypherRunner, indexManager neoutils.IndexManager) service {
	return service{cypherRunner, indexManager}
}

func (cd service) Initialise() error {
	return neoutils.EnsureConstraints(cd.indexManager, map[string]string{
		"Thing":        "uuid",
		"Concept":      "uuid",
		"Organisation": "uuid"})
}

//Write - Writes an Organisation node
func (cd service) Write(thing interface{}) error {
	o := thing.(organisation)

	props := map[string]interface{}{
		"uuid":       o.UUID,
		"properName": o.ProperName,
		"prefLabel":  o.ProperName,
	}

	if o.LegalName != "" {
		props["legalName"] = o.LegalName
	}

	if o.ShortName != "" {
		props["shortName"] = o.ShortName
	}

	if o.HiddenLabel != "" {
		props["hiddenLabel"] = o.HiddenLabel
	}

	for _, identifier := range o.Identifiers {
		if identifier.Authority == fsAuthority {
			props["factsetIdentifier"] = identifier.IdentifierValue
		}
		if identifier.Authority == leiIdentifier {
			props["leiCode"] = identifier.IdentifierValue
		}
	}

	var formerNames []string

	for _, formerName := range o.FormerNames {
		formerNames = append(formerNames, formerName)
	}

	if len(formerNames) > 0 {
		props["formerNames"] = formerNames
	}

	var localNames []string

	for _, localName := range o.LocalNames {
		localNames = append(localNames, localName)
	}

	if len(localNames) > 0 {
		props["localNames"] = localNames
	}

	var tradeNames []string

	for _, tradeName := range o.TradeNames {
		tradeNames = append(tradeNames, tradeName)
	}

	if len(tradeNames) > 0 {
		props["tradeNames"] = tradeNames
	}

	var tmeLabels []string

	for _, tmeLabel := range o.TmeLabels {
		tmeLabels = append(tmeLabels, tmeLabel)
	}

	if len(tmeLabels) > 0 {
		props["tmeLabels"] = tmeLabels
	}

	var statement bytes.Buffer
	statement.WriteString(`MERGE (o:Thing {uuid: {uuid}})
					REMOVE o:PublicCompany:Company:Organisation:Concept:Thing
					SET o={props} `)
	statement.WriteString("SET o:" + o.Type.String())
	if o.IndustryClassification != "" {
		statement.WriteString("MERGE (ic:Thing{uuid:'" + o.IndustryClassification + "'}) MERGE (o)-[:HAS_CLASSIFICATION]->(ic) ")
	}

	if o.ParentOrganisation != "" {
		statement.WriteString("MERGE (p:Thing{uuid:'" + o.ParentOrganisation + "'}) MERGE (o)-[:SUB_ORGANISATION_OF]->(p) ")
	}

	query := &neoism.CypherQuery{
		Statement: statement.String(),
		Parameters: map[string]interface{}{
			"uuid":  o.UUID,
			"props": props,
		},
	}

	queries := []*neoism.CypherQuery{query}
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
		LeiCode           string   `json:"o.leiCode"`
		TradeNames        []string `json:"o.tradeNames"`
		LocalNames        []string `json:"o.localNames"`
		FormerNames       []string `json:"o.formerNames"`
		TmeLabels         []string `json:"o.tmeLabels"`
		ParentOrgUUID     string   `json:"par.uuid"`
		IndustryUUID      string   `json:"ind.uuid"`
	}{}

	query := &neoism.CypherQuery{
		Statement: `MATCH (o:Organisation:Concept{uuid:{uuid}})
            OPTIONAL MATCH (o)-[:SUB_ORGANISATION_OF]->(par:Thing) OPTIONAL MATCH (o)-[:HAS_CLASSIFICATION]->(ind:Thing)
            RETURN o.uuid, o.properName, labels(o) AS type, o.factsetIdentifier, o.leiCode, o.legalName, o.shortName, o.hiddenLabel,
            o.formerNames, o.tradeNames, o.localNames, o.tmeLabels, ind.uuid, par.uuid`,

		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}

	err := cd.cypherRunner.CypherBatch([]*neoism.CypherQuery{query})

	if err != nil {
		return organisation{}, false, err
	}

	if len(results) == 0 {
		return organisation{}, false, nil
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
		TmeLabels:              result.TmeLabels,
		ParentOrganisation:     result.ParentOrgUUID,
		IndustryClassification: result.IndustryUUID,
	}

	i := len(result.Type)
	if i == 3 {
		o.Type = Organisation
	}
	if i == 4 {
		o.Type = Company
	}
	if i == 5 {
		o.Type = PublicCompany
	}

	if result.FactsetIdentifier != "" {
		o.Identifiers = append(o.Identifiers, identifier{fsAuthority, result.FactsetIdentifier})
	}

	if result.LeiCode != "" {
		o.Identifiers = append(o.Identifiers, identifier{leiIdentifier, result.LeiCode})
	}

	if result.FactsetIdentifier == "" && result.LeiCode == "" {
		o.Identifiers = make([]identifier, 0, 0)
	}

	return o, true, nil
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
		&neoism.CypherQuery{
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

func (s service) Count() (int, error) {

	results := []struct {
		Count int `json:"c"`
	}{}

	err := s.cypherRunner.CypherBatch([]*neoism.CypherQuery{&neoism.CypherQuery{
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
)
