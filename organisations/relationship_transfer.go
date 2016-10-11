package organisations

import (
	"fmt"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
)

type relationships []struct {
	RelationshipType string `json:"relationship"`
}

// TransferRelationships is responsible for moving relationships from node with sourceUUID to node with destinationUUID
func CreateTransferRelationshipsQueries(cypherRunner neoutils.CypherRunner, destinationUUID string, sourceUUID string) ([]*neoism.CypherQuery, error) {

	relationshipsFromSourceNode, relationshipsToSourceNode, err := getNodeRelationshipNames(cypherRunner, sourceUUID)
	if err != nil {
		return nil, err
	}

	writeQueries := []*neoism.CypherQuery{}
	for _, rel := range relationshipsFromSourceNode {
		transfQuery := constructTransferRelationshipsFromNodeQuery(sourceUUID, destinationUUID, rel.RelationshipType)
		transfQuery2 := constructTransferRelationshipsFromNodeQueryWithoutPlatformVersion(sourceUUID, destinationUUID, rel.RelationshipType)
		writeQueries = append(writeQueries, transfQuery, transfQuery2)
	}

	for _, rel := range relationshipsToSourceNode {
		transfQuery := constructTransferRelationshipsToNodeQuery(sourceUUID, destinationUUID, rel.RelationshipType)
		transfQuery2 := constructTransferRelationshipsToNodeQueryWithoutPlatformVersion(sourceUUID, destinationUUID, rel.RelationshipType)
		writeQueries = append(writeQueries, transfQuery, transfQuery2)
	}

	return writeQueries, nil
}

func getNodeRelationshipNames(cypherRunner neoutils.CypherRunner, uuid string) (relationshipsFromNodeWithUUID relationships, relationshipsToNodeWithUUID relationships, err error) {
	// find all the -> relationships
	relationshipsFromNodeWithUUID = relationships{}
	readRelationshipsFromNodeWithUUIDQuery := &neoism.CypherQuery{
		Statement: `match (a:Thing{uuid:{uuid}})-[r]->(b)
			    return distinct type(r) as relationship`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &relationshipsFromNodeWithUUID,
	}

	// find all the <- relationships
	relationshipsToNodeWithUUID = relationships{}
	readRelationshipsToNodeWithUUIDQuery := &neoism.CypherQuery{
		Statement: `match (a:Thing{uuid:{uuid}})<-[r]-(b)
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

func constructTransferRelationshipsFromNodeQuery(fromUUID string, toUUID string, predicate string) *neoism.CypherQuery {
	transferAnnotationsQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (oldNode:Organisation {uuid:{fromUUID}})-[oldRel:%s]->(p)
					WHERE HAS(oldRel.platformVersion)
					MATCH (newNode:Organisation {uuid:{toUUID}})
					MERGE (newNode)-[newRel:%s{platformVersion:oldRel.platformVersion}]->(p)
					on create SET newRel = oldRel
					DELETE oldRel`, predicate, predicate, predicate),

		Parameters: map[string]interface{}{
			"fromUUID": fromUUID,
			"toUUID":   toUUID,
		},
	}
	return transferAnnotationsQuery
}

func constructTransferRelationshipsFromNodeQueryWithoutPlatformVersion(fromUUID string, toUUID string, predicate string) *neoism.CypherQuery {
	transferAnnotationsQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (oldNode:Organisation {uuid:{fromUUID}})-[oldRel:%s]->(p)
					WHERE NOT HAS(oldRel.platformVersion)
					MATCH (newNode:Organisation {uuid:{toUUID}})
					MERGE (newNode)-[newRel:%s]->(p)
					on create SET newRel = oldRel
					DELETE oldRel`, predicate, predicate, predicate),

		Parameters: map[string]interface{}{
			"fromUUID": fromUUID,
			"toUUID":   toUUID,
		},
	}
	return transferAnnotationsQuery
}

func constructTransferRelationshipsToNodeQuery(fromUUID string, toUUID string, predicate string) *neoism.CypherQuery {
	transferAnnotationsQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (oldNode:Organisation {uuid:{fromUUID}})<-[oldRel:%s]-(p)
					WHERE HAS(oldRel.platformVersion)
					MATCH (newNode:Organisation {uuid:{toUUID}})
					MERGE (newNode)<-[newRel:%s{platformVersion:oldRel.platformVersion}]-(p)
					ON create SET newRel = oldRel
					DELETE oldRel`, predicate, predicate, predicate),

		Parameters: map[string]interface{}{
			"fromUUID": fromUUID,
			"toUUID":   toUUID,
		},
	}
	return transferAnnotationsQuery
}

func constructTransferRelationshipsToNodeQueryWithoutPlatformVersion(fromUUID string, toUUID string, predicate string) *neoism.CypherQuery {
	transferAnnotationsQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (oldNode:Organisation {uuid:{fromUUID}})<-[oldRel:%s]-(p)
					WHERE not HAS(oldRel.platformVersion)
					MATCH (newNode:Organisation {uuid:{toUUID}})
					MERGE (newNode)<-[newRel:%s]-(p)
					ON CREATE SET newRel = oldRel
					DELETE oldRel`, predicate, predicate, predicate),

		Parameters: map[string]interface{}{
			"fromUUID": fromUUID,
			"toUUID":   toUUID,
		},
	}
	return transferAnnotationsQuery
}