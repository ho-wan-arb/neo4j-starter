package n4j

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"neo4j-starter/resolve"

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

type queryBuilder struct {
	strings.Builder
	params map[string]any
}

func newQueryBuilder() *queryBuilder {
	return &queryBuilder{
		strings.Builder{},
		map[string]any{},
	}
}

// Cleanup removes all existing nodes and relations.
func (a *Adapter) Cleanup(ctx context.Context) error {
	session := a.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: dbName})
	defer session.Close(ctx)

	var err error

	_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `
			DROP INDEX identifier_type_value IF EXISTS
			`, map[string]any{},
		)
		return nil, err
	})
	if err != nil {
		return fmt.Errorf("execute write: %w", err)
	}

	_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `
			DROP INDEX identifier_duration IF EXISTS
			`, map[string]any{},
		)
		return nil, err
	})
	if err != nil {
		return fmt.Errorf("execute write: %w", err)
	}

	_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `
			MATCH (n) DETACH DELETE n
			`, map[string]any{},
		)
		return nil, err
	})
	if err != nil {
		return fmt.Errorf("execute write: %w", err)
	}

	return nil
}

func (a *Adapter) CreateEntities(ctx context.Context, entities []*resolve.Entity) error {
	session := a.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: dbName})
	defer session.Close(ctx)

	var err error

	qb := newQueryBuilder()

	// create entities
	for i, entity := range entities {
		entityRef := fmt.Sprintf("entity_%d", i)
		qb.WriteString(fmt.Sprintf(`
			CREATE (%[1]s:Entity {id:$%[1]s})
		`, entityRef))
		qb.params[entityRef] = entity.ID.String()

		qb.writeEntityNames(entity, entityRef, i)
		qb.writeEntityCountries(entity, entityRef, i)
		qb.writeEntityIdentifiers(entity, entityRef, i)
		qb.writeEntitySecurities(entity, entityRef, i)
	}

	// fmt.Println(qb.String())
	// fmt.Println(qb.params)

	_, err = session.ExecuteWrite(ctx,
		func(tx neo4j.ManagedTransaction) (any, error) {
			_, err := tx.Run(ctx, qb.String(), qb.params)
			return nil, err
		},
		neo4j.WithTxTimeout(60*time.Minute),
	)
	if err != nil {
		return fmt.Errorf("create entities: %w", err)
	}

	// TODO - can we speed up writes using an index?

	// _, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
	// 	_, err := tx.Run(ctx, `
	// 		CREATE INDEX identifier_type_value IF NOT EXISTS
	// 		FOR (idn:Identifier) ON (idn.type,idn.value)
	// 		`, map[string]any{},
	// 	)
	// 	return nil, err
	// })
	// if err != nil {
	// 	return fmt.Errorf("create index: %w", err)
	// }

	// _, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
	// 	_, err := tx.Run(ctx, `
	// 		CREATE INDEX identifier_duration IF NOT EXISTS
	// 		FOR ()-[h:HAS_IDENTIFIER]-() ON (h.from,h.until)
	// 		`, map[string]any{},
	// 	)
	// 	return nil, err
	// })
	// if err != nil {
	// 	return fmt.Errorf("create index: %w", err)
	// }

	return nil
}

func (qb *queryBuilder) writeEntityNames(entity *resolve.Entity, entityRef string, entityIndex int) {
	for j, name := range entity.Name {
		entityNameKey := fmt.Sprintf("entity_%d_name_%d", entityIndex, j)
		entityNameKeyFrom := fmt.Sprintf("%s_from", entityNameKey)
		entityNameKeyUntil := fmt.Sprintf("%s_until", entityNameKey)

		var until string
		if name.Duration.EndDate != nil {
			until = fmt.Sprintf(",until:$%s", entityNameKeyUntil)
		}

		qb.WriteString(fmt.Sprintf(`
			CREATE (%[1]s:EntityName {value:$%[1]s})
			CREATE (%[2]s)-[:HAS_NAME {from:$%[3]s%[4]s}]->(%[1]s)
		`, entityNameKey, entityRef, entityNameKeyFrom, until))
		qb.params[entityNameKey] = name.Detail.Value
		qb.params[entityNameKeyFrom] = name.Duration.StartDate.Format(time.RFC3339)
		if name.Duration.EndDate != nil {
			qb.params[entityNameKeyUntil] = name.Duration.EndDate.Format(time.RFC3339)
		}
	}
}

func (qb *queryBuilder) writeEntityCountries(entity *resolve.Entity, entityRef string, entityIndex int) {
	for j, country := range entity.Country {
		entityCountryKey := fmt.Sprintf("entity_%d_country_%d", entityIndex, j)
		entityCountryKeyFrom := fmt.Sprintf("%s_from", entityCountryKey)
		entityCountryKeyUntil := fmt.Sprintf("%s_until", entityCountryKey)

		var until string
		if country.Duration.EndDate != nil {
			until = fmt.Sprintf(",until:$%s", entityCountryKeyUntil)
		}

		qb.WriteString(fmt.Sprintf(`
			CREATE (%[1]s:Country {value:$%[1]s})
			CREATE (%[2]s)-[:HAS_COUNTRY {from:$%[3]s%[4]s}]->(%[1]s)
		`, entityCountryKey, entityRef, entityCountryKeyFrom, until))
		qb.params[entityCountryKey] = country.Detail.Value
		qb.params[entityCountryKeyFrom] = country.Duration.StartDate.Format(time.RFC3339)
		if country.Duration.EndDate != nil {
			qb.params[entityCountryKeyUntil] = country.Duration.EndDate.Format(time.RFC3339)
		}
	}
}

func (qb *queryBuilder) writeEntityIdentifiers(entity *resolve.Entity, entityRef string, entityIndex int) {
	for j, identifiersDurations := range entity.Identifiers {
		for k, identifier := range identifiersDurations.Detail {
			entityIdentifierKey := fmt.Sprintf("entity_%d_duration_%d_identifier_%d", entityIndex, j, k)
			entityIdentifierKeyType := fmt.Sprintf("%s_type", entityIdentifierKey)
			entityIdentifierKeyValue := fmt.Sprintf("%s_value", entityIdentifierKey)

			// create identifier nodes
			qb.WriteString(fmt.Sprintf(`
				CREATE (%[1]s:Identifier {type:$%[2]s,value:$%[3]s})
			`, entityIdentifierKey, entityIdentifierKeyType, entityIdentifierKeyValue))
			qb.params[entityIdentifierKeyType] = identifier.Type
			qb.params[entityIdentifierKeyValue] = identifier.Value

			// create identifier relations
			entityIdentifierKeyFrom := fmt.Sprintf("%s_from", entityIdentifierKey)
			entityIdentifierKeyUntil := fmt.Sprintf("%s_until", entityIdentifierKey)

			var until string
			if identifiersDurations.Duration.EndDate != nil {
				until = fmt.Sprintf(",until:$%s", entityIdentifierKeyUntil)
			}

			qb.WriteString(fmt.Sprintf(`
				CREATE (%[1]s)-[:HAS_IDENTIFIER {from:$%[2]s%[3]s}]->(%[4]s)
			`, entityRef, entityIdentifierKeyFrom, until, entityIdentifierKey))

			qb.params[entityIdentifierKeyFrom] = identifiersDurations.Duration.StartDate.Format(time.RFC3339)
			if identifiersDurations.Duration.EndDate != nil {
				qb.params[entityIdentifierKeyUntil] = identifiersDurations.Duration.EndDate.Format(time.RFC3339)
			}
		}
	}
}

func (qb *queryBuilder) writeEntitySecurities(entity *resolve.Entity, entityRef string, entityIndex int) {
	for j, securitiesDurations := range entity.Securities {
		for k, security := range securitiesDurations.Detail {
			entitySecurityKey := fmt.Sprintf("entity_%d_duration_%d_security_%d", entityIndex, j, k)
			entitySecurityKeyName := fmt.Sprintf("%s_name", entitySecurityKey)

			// create security node
			qb.WriteString(fmt.Sprintf(`
				CREATE (%[1]s:Security {name:$%[2]s})
			`, entitySecurityKey, entitySecurityKeyName))
			qb.params[entitySecurityKeyName] = security.Name

			// create security relations
			entitySecurityKeyFrom := fmt.Sprintf("%s_from", entitySecurityKey)
			entitySecurityKeyUntil := fmt.Sprintf("%s_until", entitySecurityKey)

			var until string
			if securitiesDurations.Duration.EndDate != nil {
				until = fmt.Sprintf(",until:$%s", entitySecurityKeyUntil)
			}
			var primary string
			if security.IsPrimary {
				primary = ",primary:true"
			}

			qb.WriteString(fmt.Sprintf(`
				CREATE (%[1]s)-[:HAS_SECURITY {from:$%[2]s%[3]s%[4]s}]->(%[5]s)
			`, entityRef, entitySecurityKeyFrom, until, primary, entitySecurityKey))

			qb.params[entitySecurityKeyFrom] = securitiesDurations.Duration.StartDate.Format(time.RFC3339)
			if securitiesDurations.Duration.EndDate != nil {
				qb.params[entitySecurityKeyUntil] = securitiesDurations.Duration.EndDate.Format(time.RFC3339)
			}

			qb.writeSecurityIdentifiers(securitiesDurations.Duration, security, entitySecurityKey)
		}
	}
}

func (qb *queryBuilder) writeSecurityIdentifiers(securitiesDuration resolve.Duration, security resolve.Security, entitySecurityKey string) {
	for j, identifier := range security.Identifiers {
		securityIdentifierKey := fmt.Sprintf("%s_identifier_%d", entitySecurityKey, j)
		securityIdentifierKeyType := fmt.Sprintf("%s_type", securityIdentifierKey)
		securityIdentifierKeyValue := fmt.Sprintf("%s_value", securityIdentifierKey)

		// create identifier nodes
		qb.WriteString(fmt.Sprintf(`
			CREATE (%[1]s:Identifier {type:$%[2]s,value:$%[3]s})
		`, securityIdentifierKey, securityIdentifierKeyType, securityIdentifierKeyValue))
		qb.params[securityIdentifierKeyType] = identifier.Type
		qb.params[securityIdentifierKeyValue] = identifier.Value

		// create identifier relations
		securityIdentifierKeyFrom := fmt.Sprintf("%s_from", securityIdentifierKey)
		securityIdentifierKeyUntil := fmt.Sprintf("%s_until", securityIdentifierKey)

		var until string
		if securitiesDuration.EndDate != nil {
			until = fmt.Sprintf(",until:$%s", securityIdentifierKeyUntil)
		}

		qb.WriteString(fmt.Sprintf(`
			CREATE (%[1]s)-[:HAS_IDENTIFIER {from:$%[2]s%[3]s}]->(%[4]s)
		`, entitySecurityKey, securityIdentifierKeyFrom, until, securityIdentifierKey))

		qb.params[securityIdentifierKeyFrom] = securitiesDuration.StartDate.Format(time.RFC3339)
		if securitiesDuration.EndDate != nil {
			qb.params[securityIdentifierKeyUntil] = securitiesDuration.EndDate.Format(time.RFC3339)
		}
	}
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
