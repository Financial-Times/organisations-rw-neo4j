package organisations

import (
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

func (cd CypherDriver) Write(thing interface{}) error {
	o := thing.(organisation)

	params := map[string]interface{}{
		"uuid":       o.UUID,
		"properName": o.ProperName,
		"prefLabel":  o.ProperName,
	}

	if o.IndustryClassification != "" {
		params["industryClassification"] = o.IndustryClassification
	}

	if o.ParentOrganisation != "" {
		params["parentOrganisation"] = o.ParentOrganisation
	}

	if o.LegalName != "" {
		params["legalName"] = o.LegalName
	}

	if o.ShortName != "" {
		params["shortName"] = o.ShortName
	}

	if o.HiddenLabel != "" {
		params["hiddenLabel"] = o.HiddenLabel
	}

	for _, identifier := range o.Identifiers {
		if identifier.Authority == fsAuthority {
			params["factsetIdentifier"] = identifier.IdentifierValue
		}
		if identifier.Authority == leiIdentifier {
			params["leiCode"] = identifier.IdentifierValue
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

	query := &neoism.CypherQuery{
		Statement: `MERGE (o:Thing {uuid: {uuid}})
					REMOVE o:PublicCompany:Company:Organisation:Concept:Thing
					SET o:Organisation:Concept:Thing
					set o={allprops}
					MERGE (p:Thing{uuid: {parentOrganisation}})
					MERGE (o)-[:SUB_ORGANISATION_OF]->(p)
					MERGE (ic:Thing{uuid: {industryClassification}})
					MERGE (o)-[:HAS_CLASSIFICATION]->(ic)
		`,
		Parameters: map[string]interface{}{
			"uuid":     o.UUID,
			"parentOrganisation": o.ParentOrganisation,
			"industryClassification": o.IndustryClassification,
			"allprops": params,
		},
	}

	queries := []*neoism.CypherQuery{query}
	return cd.cypherRunner.CypherBatch(queries)
}

const (
	fsAuthority   = "http://api.ft.com/system/FACTSET"
	leiIdentifier = "http://api.ft.com/system/LEI"
)
