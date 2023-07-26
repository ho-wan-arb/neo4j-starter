package n4j

import (
	"context"
	"fmt"
	"log"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const (
	dbName = "neo4j"
)

type Adapter struct {
	driver neo4j.DriverWithContext
}

func NewAdapter(driver neo4j.DriverWithContext) *Adapter {
	return &Adapter{
		driver: driver,
	}
}

// CreateFixedEntity creates a hard-coded entity.
func (a *Adapter) CreateFixedEntity(ctx context.Context) error {
	session := a.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: dbName})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx,
		func(tx neo4j.ManagedTransaction) (any, error) {
			_, err := tx.Run(ctx, `
				CREATE INDEX identifier_type_value IF NOT EXISTS
				FOR (idn:Identifier) ON (idn.type,idn.value)
				`, map[string]any{},
			)
			return nil, err
		},
	)
	if err != nil {
		return fmt.Errorf("create index: %w", err)
	}

	_, err = session.ExecuteWrite(ctx,
		func(tx neo4j.ManagedTransaction) (any, error) {
			_, err := tx.Run(ctx, `
				CREATE (ESGBook:Entity {name:'ESG Book', created_at:2021})
				CREATE (Isin1:Identifier {type:'isin', value:'isin-1'})
				CREATE (Isin2:Identifier {type:'isin', value:'isin-2'})
				CREATE (Isin3:Identifier {type:'isin', value:'isin-3'})
				CREATE (Isin1)-[:IDENTIFIES {roles:['primary_identifier','identifier']}]->(ESGBook)
				CREATE (Isin2)-[:IDENTIFIES {roles:['identifier']}]->(ESGBook)
				CREATE (Isin3)-[:IDENTIFIES {roles:['identifier']}]->(ESGBook)
				`, map[string]any{},
			)
			return nil, err
		},
	)
	if err != nil {
		return fmt.Errorf("create entity: %w", err)
	}

	return nil
}

// Cleanup removes all existing nodes and relations.
func (a *Adapter) Cleanup(ctx context.Context) error {
	session := a.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: dbName})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx,
		func(tx neo4j.ManagedTransaction) (any, error) {
			_, err := tx.Run(ctx, `
				MATCH (n) DETACH DELETE n
				`, map[string]any{},
			)
			return nil, err
		},
	)
	if err != nil {
		return fmt.Errorf("execute write: %w", err)
	}

	return nil
}

func Connect(ctx context.Context) (neo4j.DriverWithContext, func(), error) {
	// connect to server running the local deployment
	const (
		dbUri      = "neo4j://localhost"
		dbUser     = "neo4j"
		dbPassword = "changeme"
	)

	driver, err := neo4j.NewDriverWithContext(dbUri, neo4j.BasicAuth(dbUser, dbPassword, ""))

	cleanup := func() {
		if err := driver.Close(ctx); err != nil {
			log.Fatal(err)
		}
	}
	if err != nil {
		return nil, cleanup, fmt.Errorf("new driver: %w", err)
	}

	if err := driver.VerifyConnectivity(ctx); err != nil {
		return nil, cleanup, fmt.Errorf("verify connectivity: %w", err)
	}

	return driver, cleanup, nil
}
