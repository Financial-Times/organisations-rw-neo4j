package organisations

import (
	"bytes"
	"github.com/Financial-Times/neo-cypher-runner-go"
	"github.com/Financial-Times/neo-utils-go"
	"github.com/jmcvetta/neoism"
)

type CypherDriver struct {
	cypherRunner neocypherrunner.CypherRunner
	indexManager neoutils.IndexManager
}

func NewCypherDriver(cypherRunner neocypherrunner.CypherRunner, indexManager neoutils.IndexManager) CypherDriver {
	return CypherDriver{cypherRunner, indexManager}
}

func (cd CypherDriver) Initialise() error {
	return neoutils.EnsureIndexes(cd.indexManager, map[string]string{
		"Thing":        "uuid",
		"Concept":      "uuid",
		"Organisation": "uuid"})
}

//Write - Writes an Organisation node
func (cd CypherDriver) Write(thing interface{}) error {
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

	params2 := map[string][]string{}

	for _, formerName := range o.FormerNames {
		params2["formerNames"] = append(params2["formerNames"], formerName)
	}

	for _, localName := range o.LocalNames {
		params2["localNames"] = append(params2["localNames"], localName)
	}

	for _, tradeName := range o.TradeNames {
		params2["tradeNames"] = append(params2["tradeNames"], tradeName)
	}

	for _, tmeLabel := range o.TmeLabels {
		params2["tmeLabels"] = append(params2["tmeLabels"], tmeLabel)
	}

	//
	//	MERGE (o:Thing {uuid:'0786619b-0969-43d4-9372-f27e4029f565'})
	//	REMOVE o:PublicCompany:Company:Organisation:Concept:Thing
	//	SET o:Organisation:Concept:Thing
	//	SET o={
	//		uuid:'0786619b-0969-43d4-9372-f27e4029f565',
	//		properName:'Proper Name',
	//		prefLabel:'Proper Name',
	//		factsetIdentifier:'identifierValue',
	//		leiCode:'leiCode',
	//		legalName:'Legal Name',
	//		shortName:'Short Name',
	//		hiddenLabel:'Hidden Label',
	//		formerNames:[
	//		'Older Name, inc.',
	//		'Old Name, inc.'
	//	],
	//		localNames:[
	//		'Oldé Name, inc.',
	//		'Tradé Name'
	//	],
	//		tradeNames:[
	//		'Older Name, inc.',
	//		'Old Name, inc.'
	//	],
	//		tmeLabels:[
	//		'tmeLabel1',
	//		'tmeLabel3',
	//		'tmeLabel2'
	//	]
	//	}
	//	MERGE (p:Thing{uuid:'b68b6570-4eb5-4624-98ed-ca3366e42311'})
	//	MERGE (o)-[:SUB_ORGANISATION_OF]->(p)
	//	MERGE (ic:Thing{uuid:'e077af65-267e-4c06-8f06-ad7b9f3f8b19'})
	//	MERGE (o)-[:HAS_CLASSIFICATION]->(ic)

	var statement bytes.Buffer
	statement.WriteString(`MERGE (o:Thing {uuid: {uuid}})
					REMOVE o:PublicCompany:Company:Organisation:Concept:Thing
					SET o:Organisation:Concept:Thing
					SET o={props} `)

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

//Delete - Deletes an Organisation
func (pcd CypherDriver) Delete(uuid string) (bool, error) {
	clearNode := &neoism.CypherQuery{
		Statement: `
			MATCH (org:Thing {uuid: {uuid}})
			REMOVE org:Concept:Organisation SET org={ uuid: {uuid}}
		`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		IncludeStats: true,
	}

	deleteNode := &neoism.CypherQuery{
		Statement: `
		MATCH (org:Thing {uuid: '%s'})
		OPTIONAL MATCH (org)-[a]-(x) WITH org, count(a) AS relCount WHERE relCount = 0
		DELETE org
		`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		IncludeStats: true,
	}

	err := pcd.cypherRunner.CypherBatch([]*neoism.CypherQuery{clearNode, deleteNode})

	s1, err := clearNode.Stats()

	if err != nil {
		return false, err
	}

	if s1.ContainsUpdates && s1.LabelsRemoved > 0 {
		return true, err
	}

	return false, err
}

const (
	fsAuthority   = "http://api.ft.com/system/FACTSET"
	leiIdentifier = "http://api.ft.com/system/LEI"
)
