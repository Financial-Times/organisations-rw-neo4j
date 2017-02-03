package organisations

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/Financial-Times/neo-utils-go/neoutils"
	log "github.com/Sirupsen/logrus"
	"github.com/jmcvetta/neoism"
)

type service struct {
	conn       neoutils.NeoConnection
	writeCount int
}

//NewCypherOrganisationService returns a new service responsible for writing organisations in Neo4j
func NewCypherOrganisationService(cypherRunner neoutils.NeoConnection) service {
	return service{cypherRunner, 0}
}

func (cd service) Initialise() error {

	err := cd.conn.EnsureIndexes(map[string]string{
		"Identifier": "value",
	})

	if err != nil {
		return err
	}

	return cd.conn.EnsureConstraints(map[string]string{
		"Thing":             "uuid",
		"Concept":           "uuid",
		"Organisation":      "uuid",
		"FactsetIdentifier": "value",
		"TMEIdentifier":     "value",
		"UPPIdentifier":     "value"})
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

	mergingQueriesForOldNodes, err := cd.constructMergingOldOrganisationNodesQueries(o.UUID, o.AlternativeIdentifiers.UUIDS)
	if err != nil {
		return err
	}

	if len(mergingQueriesForOldNodes) != 0 {
		queries = append(queries, mergingQueriesForOldNodes...)
	}

	//ADD all the IDENTIFIER nodes and IDENTIFIES relationships
	for _, alternativeUUID := range o.AlternativeIdentifiers.TME {
		alternativeIdentifierQuery := createNewIdentifierQuery(o.UUID, tmeIdentifierLabel, alternativeUUID)
		queries = append(queries, alternativeIdentifierQuery)
	}

	for _, alternativeUUID := range o.AlternativeIdentifiers.UUIDS {
		alternativeIdentifierQuery := createNewIdentifierQuery(o.UUID, uppIdentifierLabel, alternativeUUID)
		queries = append(queries, alternativeIdentifierQuery)
	}

	if o.AlternativeIdentifiers.FactsetIdentifier != "" {
		queries = append(queries, createNewIdentifierQuery(o.UUID, factsetIdentifierLabel, o.AlternativeIdentifiers.FactsetIdentifier))
	}

	if o.AlternativeIdentifiers.LeiCode != "" {
		queries = append(queries, createNewIdentifierQuery(o.UUID, leiIdentifierLabel, o.AlternativeIdentifiers.LeiCode))
	}

	if o.IndustryClassification != "" {
		industryClassQuery := constructCreateIndustryClassificationQuery(o.UUID, o.IndustryClassification)
		queries = append(queries, industryClassQuery)
	}

	if o.ParentOrganisation != "" {
		parentQuery := constructCreateParentOrganisationQuery(o.UUID, o.ParentOrganisation)
		queries = append(queries, parentQuery)
	}
	err = cd.conn.CypherBatch(queries)
	if err == nil {
		cd.writeCount += 1
		log.Debugf("Write count: %v, timestamp: %v", cd.writeCount, time.Now().Format("15:04:05.000"))
	}
	return err
}

func (cd service) constructMergingOldOrganisationNodesQueries(canonicalUUID string, possibleOldNodes []string) ([]*neoism.CypherQuery, error) {

	queries := []*neoism.CypherQuery{}

	for _, identifier := range possibleOldNodes {
		// only nodes with uppAuthority can be older organisation nodes
		if identifier != canonicalUUID {
			nodeExists, err := cd.checkNodeExistence(identifier)
			if err != nil {
				return nil, err
			}
			if nodeExists {
				deleteEntityRelationshipsForDeprecatedOrgNodeQuery := constructDeleteEntityRelationshipQuery(identifier)
				queries = append(queries, deleteEntityRelationshipsForDeprecatedOrgNodeQuery)

				// re-point the remaining relationships from previous node to the canonical/actual one
				transferQueries, err := CreateTransferRelationshipsQueries(cd.conn, canonicalUUID, identifier)
				if err != nil {
					return nil, err
				}
				if len(transferQueries) != 0 {
					queries = append(queries, transferQueries...)
				}

				// delete oldOrg
				deleteOldOrganisationQuery := constructDeleteEmptyNodeQuery(identifier)
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
	err := cd.conn.CypherBatch(readQueries)

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
		UUID                   string                 `json:"uuid"`
		Type                   []string               `json:"type"`
		ProperName             string                 `json:"properName"`
		PrefLabel              string                 `json:"prefLabel"`
		LegalName              string                 `json:"legalName"`
		ShortName              string                 `json:"shortName"`
		HiddenLabel            string                 `json:"hiddenLabel"`
		AlternativeIdentifiers alternativeIdentifiers `json:"alternativeIdentifiers"`
		TradeNames             []string               `json:"tradeNames"`
		LocalNames             []string               `json:"localNames"`
		FormerNames            []string               `json:"formerNames"`
		Aliases                []string               `json:"aliases"`
		IndustryClassification string                 `json:"industryClassification"`
		ParentOrganisation     string                 `json:"parentOrganisation"`
	}{}

	readQuery := &neoism.CypherQuery{
		Statement: `MATCH (o:Organisation:Concept{uuid:{uuid}})
            			OPTIONAL MATCH (o)-[:SUB_ORGANISATION_OF]->(par:Thing)
            			OPTIONAL MATCH (o)-[:HAS_CLASSIFICATION]->(ind:Thing)
           			OPTIONAL MATCH (upp:UPPIdentifier)-[:IDENTIFIES]->(o)
	    			OPTIONAL MATCH (factset:FactsetIdentifier)-[:IDENTIFIES]->(o)
	   			OPTIONAL MATCH (tme:TMEIdentifier)-[:IDENTIFIES]->(o)
	    			OPTIONAL MATCH (lei:LegalEntityIdentifier)-[:IDENTIFIES]->(o)
            		 	RETURN o.uuid as uuid,
					o.properName as properName,
					labels(o) as Type,
					o.prefLabel as prefLabel,
					o.legalName as legalName,
					o.shortName as shortName,
					o.hiddenLabel as hiddenLabel,
					o.formerNames as formerNames,
					o.tradeNames as tradeNames,
					o.localNames as localNames,
					o.aliases as aliases,
					ind.uuid as industryClassification,
					par.uuid as parentOrganisation,
					{uuids:collect(distinct upp.value),
					 TME:collect(distinct tme.value),
					 factsetIdentifier:factset.value,
					 leiCode:lei.value} as alternativeIdentifiers`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}

	if err := cd.conn.CypherBatch([]*neoism.CypherQuery{readQuery}); err != nil || len(results) == 0 {
		return organisation{}, false, err
	}

	result := results[0]

	o := organisation{
		UUID:                   result.UUID,
		ProperName:             result.ProperName,
		PrefLabel:              result.PrefLabel,
		LegalName:              result.LegalName,
		ShortName:              result.ShortName,
		HiddenLabel:            result.HiddenLabel,
		TradeNames:             result.TradeNames,
		LocalNames:             result.LocalNames,
		FormerNames:            result.FormerNames,
		AlternativeIdentifiers: result.AlternativeIdentifiers,
		Aliases:                result.Aliases,
		ParentOrganisation:     result.ParentOrganisation,
		IndustryClassification: result.IndustryClassification,
	}

	addType(&o.Type, &result.Type)
	sort.Strings(o.AlternativeIdentifiers.TME)
	sort.Strings(o.AlternativeIdentifiers.UUIDS)

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
			OPTIONAL MATCH (org)-[so:SUB_ORGANISATION_OF]->(par:Thing)
			OPTIONAL MATCH (org)-[cb:HAS_CLASSIFICATION]->(ic:Thing)
			REMOVE org:Concept:Organisation:Company:PublicCompany
			DELETE so, cb
			SET org={uuid: {uuid}}
		`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		IncludeStats: true,
	}
	removeNodeIfUnused := []*neoism.CypherQuery{
		clearNode,
		{
			Statement: `
		MATCH (t:Thing {uuid: {uuid}})
		OPTIONAL MATCH (t)<-[a]-(x:Thing)
		OPTIONAL MATCH (t)-[ir:IDENTIFIES]-(id:Identifier)
		WITH ir, id, t, count(a) AS relCount
		WHERE relCount = 0
		DELETE t, ir, id
		`,
			Parameters: map[string]interface{}{
				"uuid": uuid,
			},
		},
	}

	err := cd.conn.CypherBatch(removeNodeIfUnused)

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
	return neoutils.Check(cd.conn)
}

type countResult []struct {
	Count int `json:"c"`
}

func (cd service) Count() (int, error) {

	results := countResult{}

	err := cd.conn.CypherBatch([]*neoism.CypherQuery{{
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
