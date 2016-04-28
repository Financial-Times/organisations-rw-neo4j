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


func constructOrganisationProperties(o organisation) (map[string]interface{}) {

	props := map[string]interface{}{
		"uuid":       o.UUID,
		"properName": o.ProperName,
		"prefLabel":  o.ProperName,
	}

	setProps(&props, &o.LegalName, "legalName")
	setProps(&props, &o.ShortName, "shortName")
	setProps(&props, &o.HiddenLabel, "hiddenLabel")
	setListProps(&props, &o.FormerNames, "formerNames")
	setListProps(&props, &o.LocalNames, "localNames")
	setListProps(&props, &o.TradeNames, "tradeNames")
	setListProps(&props, &o.Aliases, "aliases")

	return props
}

func constructDeleteEntityRelationshipQuery(uuid string) (*neoism.CypherQuery) {
	deleteEntityRelationshipsQuery := &neoism.CypherQuery{
		Statement: `MATCH (o:Thing {uuid:{uuid}})
		OPTIONAL MATCH (o)-[hc:HAS_CLASSIFICATION]->(ic)
		OPTIONAL MATCH (o)-[soo:SUB_ORGANISATION_OF]->(p)
		OPTIONAL MATCH (o)<-[iden:IDENTIFIES]-(i)
		DELETE hc, soo, iden, i`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
	}

	return deleteEntityRelationshipsQuery
}

func constructResetOrganisationQuery(uuid string, props map[string]interface{}) (*neoism.CypherQuery) {
	resetOrgQuery := &neoism.CypherQuery{
		Statement: `MERGE (o:Thing {uuid: {uuid}})
					REMOVE o:PublicCompany:Company:Organisation:Concept
					SET o={props}`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
			"props": props,
		},
	}

	return resetOrgQuery
}

func constructTransferAnnotationsQuery(oldOrgUUID string, newOrgUUID string, predicate string) (*neoism.CypherQuery) {
	transferAnnotationsQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (oldOrg:Thing {uuid:{uuid}})
					MATCH (oldOrg)<-[oldRel:%s]-(p)
					MATCH (newOrg:Thing {uuid:{canonicalUUID}})
					MERGE (newOrg)<-[newRel:%s]-(p)
					SET newRel = oldRel
					DELETE oldRel`,predicate,predicate),

		Parameters: map[string]interface{}{
			"uuid": oldOrgUUID,
			"canonicalUUID": newOrgUUID,
		},
	}
	return transferAnnotationsQuery
}

//Write - Writes an Organisation node
func (cd service) Write(thing interface{}) error {

	o := thing.(organisation)
	props := constructOrganisationProperties(o)

	deleteEntityRelationshipsQuery := constructDeleteEntityRelationshipQuery(o.UUID)

	resetOrgQuery := constructResetOrganisationQuery(o.UUID, props)

	queries := []*neoism.CypherQuery{deleteEntityRelationshipsQuery, resetOrgQuery}

	identifierLabels := map[string]string{
		fsAuthority:  factsetIdentifierLabel,
		leiAuthority: leiIdentifierLabel,
		tmeAuthority: tmeIdentifierLabel,
		uppAuthority: uppIdentifierLabel,
	}

	// clean-up the old organisation nodes (which have now been transformed to UPP identifiers)
	for _, identifier := range o.Identifiers {
		if identifier.Authority == uppAuthority {
			deleteEntityRelationshipsForDeprecatedOrgNodeQuery := constructDeleteEntityRelationshipQuery(identifier.IdentifierValue)
			queries = append(queries, deleteEntityRelationshipsForDeprecatedOrgNodeQuery)

			// re-point the remaining relationships from previous node to the canonical/actual one (annotations)
			// delete oldOrg and oldRelationship
			transferMentionsAnnotationsQuery := constructTransferAnnotationsQuery(identifier.IdentifierValue, o.UUID, "MENTIONS")
			transferAboutAnnotationsQuery := constructTransferAnnotationsQuery(identifier.IdentifierValue, o.UUID, "ABOUT")
			transferHasOrganisationQuery := constructTransferAnnotationsQuery(identifier.IdentifierValue, o.UUID, "HAS_ORGANISATION")

			queries = append(queries, transferMentionsAnnotationsQuery)
			queries = append(queries, transferAboutAnnotationsQuery)
			queries = append(queries, transferHasOrganisationQuery)

			// delete everything that remained
			deleteOldOrganisationQuery := &neoism.CypherQuery{
				Statement: `MATCH (o:Thing {uuid:{uuid}})
				OPTIONAL MATCH (o)-[r]-(c) DELETE o, r, c`,
				Parameters: map[string]interface{}{
					"uuid": identifier.IdentifierValue,
				},
			}

			queries = append(queries, deleteOldOrganisationQuery)
		}
	}

	for _, identifier := range o.Identifiers {

		if identifierLabels[identifier.Authority] == "" {
			return requestError{fmt.Sprintf("This identifier type- %v, is not supported. Only '%v', '%v' and '%v' are currently supported", identifier.Authority, fsAuthority, leiAuthority, tmeAuthority, uppAuthority)}
		}
		addIdentifierQuery := addIdentifierQuery(identifier, o.UUID, identifierLabels[identifier.Authority])
		queries = append(queries, addIdentifierQuery)
	}

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

		industryClassQuery := &neoism.CypherQuery{
			Statement: "MERGE (o:Thing {uuid: {uuid}}) MERGE (ic:Thing{uuid: {indUuid}}) MERGE (o:Thing {uuid: {uuid}})-[:HAS_CLASSIFICATION]->(ic) ",
			Parameters: map[string]interface{}{
				"uuid":    o.UUID,
				"indUuid": o.IndustryClassification,
			},
		}
		queries = append(queries, industryClassQuery)
	}

	if o.ParentOrganisation != "" {
		parentQuery := &neoism.CypherQuery{
			Statement: "MERGE (o:Thing {uuid: {uuid}}) MERGE (p:Thing{uuid: {paUuid}}) MERGE (o)-[:SUB_ORGANISATION_OF]->(p) ",
			Parameters: map[string]interface{}{
				"uuid":   o.UUID,
				"paUuid": o.ParentOrganisation,
			},
		}
		queries = append(queries, parentQuery)
	}

	return cd.cypherRunner.CypherBatch(queries)
}

func addIdentifierQuery(identifier identifier, uuid string, identifierLabel string) *neoism.CypherQuery {

	statementTemplate := fmt.Sprintf(`MERGE (o:Thing {uuid:{uuid}})
					CREATE (i:Identifier {value:{value} , authority:{authority}})
					CREATE (o)<-[:IDENTIFIES]-(i)
					set i : %s `, identifierLabel)

	query := &neoism.CypherQuery{
		Statement: statementTemplate,
		Parameters: map[string]interface{}{
			"uuid":      uuid,
			"value":     identifier.IdentifierValue,
			"authority": identifier.Authority,
		},
	}
	return query
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
func (pcd service) Delete(uuid string) (bool, error) {
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
