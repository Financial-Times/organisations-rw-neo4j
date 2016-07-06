package organisations

import (
	"fmt"
	"github.com/jmcvetta/neoism"
)

func constructOrganisationProperties(o organisation) map[string]interface{} {
	props := map[string]interface{}{
		"uuid":       o.UUID,
		"properName": o.ProperName,
		"prefLabel":  o.PrefLabel,
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

func constructDeleteEntityRelationshipQuery(uuid string) *neoism.CypherQuery {
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

func constructResetOrganisationQuery(uuid string, props map[string]interface{}) *neoism.CypherQuery {
	resetOrgQuery := &neoism.CypherQuery{
		Statement: `MERGE (o:Thing {uuid: {uuid}})
					REMOVE o:PublicCompany:Company:Organisation:Concept
					SET o={props}`,
		Parameters: map[string]interface{}{
			"uuid":  uuid,
			"props": props,
		},
	}

	return resetOrgQuery
}

func constructDeleteEmptyNodeQuery(uuid string) *neoism.CypherQuery {
	return &neoism.CypherQuery{
		Statement: `MATCH (o:Thing {uuid:{uuid}})
					    DELETE o`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
	}
}

func constructCreateParentOrganisationQuery(uuid string, parentUUID string) *neoism.CypherQuery {
	return &neoism.CypherQuery{
		Statement: `MERGE (o:Thing {uuid: {uuid}})
		  	    MERGE (parentupp:Identifier:UPPIdentifier{value:{paUuid}})
                            MERGE (parentupp)-[:IDENTIFIES]->(p:Thing) ON CREATE SET p.uuid = {paUuid}
		            MERGE (o)-[:SUB_ORGANISATION_OF]->(p)`,
		Parameters: map[string]interface{}{
			"uuid":   uuid,
			"paUuid": parentUUID,
		},
	}
}

func constructCreateIndustryClassificationQuery(uuid string, industryClassificationUUID string) *neoism.CypherQuery {
	return &neoism.CypherQuery{
		Statement: "MERGE (o:Thing {uuid: {uuid}}) MERGE (ic:Thing{uuid: {indUuid}}) MERGE (o)-[:HAS_CLASSIFICATION]->(ic) ",
		Parameters: map[string]interface{}{
			"uuid":    uuid,
			"indUuid": industryClassificationUUID,
		},
	}
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
