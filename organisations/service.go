package organisations

import (
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
)

type service struct {
	cypherRunner neoutils.CypherRunner
	indexManager neoutils.IndexManager
}

//NewCypherOrganisationService returns a new service responsible for writing organisations in Neo4j
func NewCypherOrganisationService(cypherRunner neoutils.CypherRunner, indexManager neoutils.IndexManager) service {
	return service{cypherRunner, indexManager}
}

func (cd service) Initialise() error {
	return neoutils.EnsureConstraints(cd.indexManager, map[string]string{
		"Thing":        "uuid",
		"Concept":      "uuid",
		"Organisation": "uuid",
		"Identifier":   "value"})
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
	props := constructOrganisationProperties(o)

	deleteEntityRelationshipsQuery := constructDeleteEntityRelationshipQuery(o.UUID)
	resetOrgQuery := constructResetOrganisationQuery(o.UUID, props)

	queries := []*neoism.CypherQuery{deleteEntityRelationshipsQuery, resetOrgQuery}

	mergingQueriesForOldNodes, err := cd.constructMergingOldOrganisationNodesQueries(o.UUID, o.Identifiers)
	if err != nil {
		return err
	}

	if len(mergingQueriesForOldNodes) != 0 {
		queries = append(queries, mergingQueriesForOldNodes...)
	}

	identifierLabels := map[string]string{
		fsAuthority:  factsetIdentifierLabel,
		leiAuthority: leiIdentifierLabel,
		tmeAuthority: tmeIdentifierLabel,
		uppAuthority: uppIdentifierLabel,
	}

	for _, identifier := range o.Identifiers {
		if identifierLabels[identifier.Authority] == "" {
			return requestError{fmt.Sprintf("This identifier type- %v, is not supported. Only '%v', '%v', '%v' and '%v' are currently supported", identifier.Authority, fsAuthority, leiAuthority, tmeAuthority, uppAuthority)}
		}
		addIdentifierQuery := addIdentifierQuery(identifier, o.UUID, identifierLabels[identifier.Authority])
		queries = append(queries, addIdentifierQuery)
	}
	//add upp identifier for the canonical uuid
	addIdentifierQuery := addIdentifierQuery(identifier{Authority: uppAuthority, IdentifierValue: o.UUID}, o.UUID, identifierLabels[uppAuthority])
	queries = append(queries, addIdentifierQuery)

	//add type
	err, stringType := o.Type.String()
	if err == nil {
		setTypeStatement := fmt.Sprintf(`MERGE (o:Thing {uuid: {uuid}})  set o : %s `, stringType)
		setTypeQuery := &neoism.CypherQuery{
			Statement: setTypeStatement,
			Parameters: map[string]interface{}{
				"uuid": o.UUID,
			},
		}
		queries = append(queries, setTypeQuery)

	} else {
		return err
	}

	if o.IndustryClassification != "" {
		industryClassQuery := constructCreateIndustryClassificationQuery(o.UUID, o.IndustryClassification)
		queries = append(queries, industryClassQuery)
	}

	if o.ParentOrganisation != "" {
		parentQuery := constructCreateParentOrganisationQuery(o.UUID, o.ParentOrganisation)
		queries = append(queries, parentQuery)
	}
	return cd.cypherRunner.CypherBatch(queries)
}

func (cd service) constructMergingOldOrganisationNodesQueries(canonicalUUID string, possibleOldNodes []identifier) ([]*neoism.CypherQuery, error) {

	queries := []*neoism.CypherQuery{}

	for _, identifier := range possibleOldNodes {
		// only nodes with uppAuthority can be older organisation nodes
		if identifier.Authority == uppAuthority {
			nodeExists, err := cd.checkNodeExistence(identifier.IdentifierValue)
			if err != nil {
				return nil, err
			}
			if nodeExists {
				deleteEntityRelationshipsForDeprecatedOrgNodeQuery := constructDeleteEntityRelationshipQuery(identifier.IdentifierValue)
				queries = append(queries, deleteEntityRelationshipsForDeprecatedOrgNodeQuery)

				// re-point the remaining relationships from previous node to the canonical/actual one
				transferQueries, err := TransferRelationships(cd.cypherRunner, canonicalUUID, identifier.IdentifierValue)
				if err != nil {
					return nil, err
				}
				if len(transferQueries) != 0 {
					queries = append(queries, transferQueries...)
				}

				// delete oldOrg
				deleteOldOrganisationQuery := constructDeleteEmptyNodeQuery(identifier.IdentifierValue)
				queries = append(queries, deleteOldOrganisationQuery)
			}
		}
	}

	return queries, nil
}

func (cd service) checkNodeExistence(uuid string) (bool, error) {
	type result []struct {
		Count int `json:"nr"`
	}
	res := result{}

	checkNodeExistenceQuery := &neoism.CypherQuery{
		Statement: `match (a:Thing{uuid:{uuid}})
			           return count(a) as nr`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &res,
	}

	readQueries := []*neoism.CypherQuery{checkNodeExistenceQuery}
	err := cd.cypherRunner.CypherBatch(readQueries)

	if err != nil {
		return false, err
	}

	if len(res) != 1 {
		return false, fmt.Errorf("DB inconsistence: one count result should be returned for node with UUID %s", uuid)
	}

	if res[0].Count == 0 {
		return false, nil
	} else if res[0].Count == 1 {
		return true, nil
	} else {
		return false, fmt.Errorf("DB inconsistence: %d node (instead of max 1) exists with UUID %s", res[0].Count, uuid)
	}
}

//Read - Internal Read of an Organisation
func (cd service) Read(uuid string) (interface{}, bool, error) {

	results := []struct {
		UUID          string       `json:"o.uuid"`
		Type          []string     `json:"type"`
		ProperName    string       `json:"o.properName"`
		LegalName     string       `json:"o.legalName"`
		ShortName     string       `json:"o.shortName"`
		HiddenLabel   string       `json:"o.hiddenLabel"`
		Identifiers   []identifier `json:"identifiers"`
		TradeNames    []string     `json:"o.tradeNames"`
		LocalNames    []string     `json:"o.localNames"`
		FormerNames   []string     `json:"o.formerNames"`
		Aliases       []string     `json:"o.aliases"`
		ParentOrgUUID string       `json:"par.uuid"`
		IndustryUUID  string       `json:"ind.uuid"`
	}{}

	readQuery := &neoism.CypherQuery{

		Statement: `MATCH (o:Organisation:Concept{uuid:{uuid}})
            OPTIONAL MATCH (o)-[:SUB_ORGANISATION_OF]->(par:Thing) 
            OPTIONAL MATCH (o)-[:HAS_CLASSIFICATION]->(ind:Thing)
            OPTIONAL MATCH (o)<-[:IDENTIFIES]-(id:Identifier)
			with o, ind, par,  collect({authority:id.authority, identifierValue:id.value})as identifiers
            RETURN o.uuid , o.properName , labels(o) as Type, o.legalName, o.shortName, o.hiddenLabel,
            o.formerNames, o.tradeNames, o.localNames, o.aliases, ind.uuid, par.uuid, identifiers`,
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
		Identifiers:            result.Identifiers,
		Aliases:                result.Aliases,
		ParentOrganisation:     result.ParentOrgUUID,
		IndustryClassification: result.IndustryUUID,
	}

	addType(&o.Type, &result.Type)
	sortIdentifiers(o.Identifiers)

	if len(result.Identifiers) == 1 && (result.Identifiers[0].IdentifierValue == "") {
		o.Identifiers = make([]identifier, 0, 0)
	}

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

//Delete - Deletes an Organisation
func (cd service) Delete(uuid string) (bool, error) {
	clearNode := &neoism.CypherQuery{
		Statement: `
			MATCH (org:Thing {uuid: {uuid}})
			OPTIONAL MATCH (org)<-[iden:IDENTIFIES]-(i:Identifier)
			REMOVE org:Concept:Organisation:Company:PublicCompany
			DELETE iden, i
			SET org={ uuid: {uuid}}
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

	err := cd.cypherRunner.CypherBatch(qs)

	s1, err := clearNode.Stats()

	if err != nil {
		return false, err
	}

	if s1.ContainsUpdates && s1.LabelsRemoved > 0 {
		return true, err
	}

	return false, err
}

func (cd service) Check() error {
	return neoutils.Check(cd.cypherRunner)
}

type countResult []struct {
	Count int `json:"c"`
}

func (cd service) Count() (int, error) {

	results := countResult{}

	err := cd.cypherRunner.CypherBatch([]*neoism.CypherQuery{{
		Statement: `MATCH (n:Organisation) return count(n) as c`,
		Result:    &results,
	}})

	if err != nil {
		return 0, err
	}

	return results[0].Count, nil
}

func (cd service) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	org := organisation{}
	err := dec.Decode(&org)
	return org, org.UUID, err
}

type requestError struct {
	details string
}

func (re requestError) Error() string {
	return "Invalid Request"
}

func (re requestError) InvalidRequestDetails() string {
	return re.details
}

const (
	fsAuthority            = "http://api.ft.com/system/FACTSET-EDM"
	leiAuthority           = "http://api.ft.com/system/LEI"
	tmeAuthority           = "http://api.ft.com/system/FT-TME"
	uppAuthority           = "http://api.ft.com/system/FT-UPP"
	factsetIdentifierLabel = "FactsetIdentifier"
	leiIdentifierLabel     = "LegalEntityIdentifier"
	tmeIdentifierLabel     = "TMEIdentifier"
	uppIdentifierLabel     = "UPPIdentifier"
)
