package organisations

import (
	"fmt"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
)

type relationships []struct {
	RelationshipType string `json:"relationship"`
}

// TransferRelationships is reponsible for moving relationships from node with destinationUUID to node with sourceUUID
func TransferRelationships(cypherRunner neoutils.CypherRunner, destinationUUID string, sourceUUID string) ([]*neoism.CypherQuery, error) {

	relationshipsFromSourceNode, relationshipsToSourceNode, err := getNodeRelationshipNames(cypherRunner, sourceUUID)
	if err != nil {
		return nil, err
	}

	// NOTE: there will be relationships, like: SUB_ORGANISATION_OF, HAS_CLASSIFICATION or IDENTIFIES which even if will be returned here, at the actual execution phase will no longer exists in the db
	writeQueries := []*neoism.CypherQuery{}
	for _, rel := range relationshipsFromSourceNode {
		transfQuery := constructTransferRelationshipsQuery(sourceUUID, destinationUUID, rel.RelationshipType, true)
		writeQueries = append(writeQueries, transfQuery)
	}

	for _, rel := range relationshipsToSourceNode {
		transfQuery := constructTransferRelationshipsQuery(sourceUUID, destinationUUID, rel.RelationshipType, false)
		writeQueries = append(writeQueries, transfQuery)
	}

	return writeQueries, nil
}

func getNodeRelationshipNames(cypherRunner neoutils.CypherRunner, uuid string) (relationshipsFromNodeWithUUID relationships, relationshipsToNodeWithUUID relationships, err error) {
	// find all the -> relationships
	relationshipsFromNodeWithUUID = relationships{}
	readRelationshipsFromNodeWithUUIDQuery := &neoism.CypherQuery{
		Statement: `match (a:Thing{uuid:{uuid}})-[r]-(b)
			    where startnode(r) = a
			    return distinct type(r) as relationship`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &relationshipsFromNodeWithUUID,
	}

	// find all the <- relationships
	relationshipsToNodeWithUUID = relationships{}
	readRelationshipsToNodeWithUUIDQuery := &neoism.CypherQuery{
		Statement: `match (a:Thing{uuid:{uuid}})-[r]-(b)
			    where endnode(r) = a
			    return distinct type(r) as relationship`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &relationshipsToNodeWithUUID,
	}

	readQueries := []*neoism.CypherQuery{readRelationshipsFromNodeWithUUIDQuery, readRelationshipsToNodeWithUUIDQuery}

	err = cypherRunner.CypherBatch(readQueries)

	if err != nil {
		return nil, nil, err
	}

	return relationshipsFromNodeWithUUID, relationshipsToNodeWithUUID, nil

}

func constructTransferRelationshipsQuery(fromUUID string, toUUID string, predicate string, toRight bool) *neoism.CypherQuery {

	var leftArrow, righArrow string
	if toRight {
		righArrow = ">"
	} else {
		leftArrow = "<"
	}

	transferAnnotationsQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (oldNode:Thing {uuid:{fromUUID}})
					MATCH (newNode:Thing {uuid:{toUUID}})
					MATCH (oldNode)%s-[oldRel:%s]-%s(p)
					MERGE (newNode)%s-[newRel:%s]-%s(p)
					SET newRel = oldRel
					DELETE oldRel`, leftArrow, predicate, righArrow, leftArrow, predicate, righArrow),

		Parameters: map[string]interface{}{
			"fromUUID": fromUUID,
			"toUUID":   toUUID,
		},
	}
	return transferAnnotationsQuery
}
