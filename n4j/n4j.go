package n4j

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"neo4j-starter/resolve"

	"github.com/gofrs/uuid"
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

// ToQueryWithParams generates a query with the params replaced by values and
// whitespace cleaned up. This can be used to log the query so it can be used
// with EXPLAIN and PROFILE. However it does not work with slices.
func (qb *queryBuilder) ToQueryWithParams() string {
	s := qb.String()

	var sortedKeys [][]string
	// sort key of from longest to shortest to avoid replacing params too early
	for k, v := range qb.params {
		sortedKeys = append(sortedKeys, []string{k, fmt.Sprint(v)})
	}
	sort.Slice(sortedKeys, func(i, j int) bool {
		return len(sortedKeys[i][0]) > len(sortedKeys[j][0])
	})

	for _, kv := range sortedKeys {
		s = strings.ReplaceAll(s, fmt.Sprintf(`$%s`, kv[0]), fmt.Sprintf(`'%v'`, kv[1]))
	}

	s = strings.ReplaceAll(s, "\t", "")
	s = strings.ReplaceAll(s, "\n\n", "\n")
	return s
}

// Cleanup removes all existing nodes and relations.
func (a *Adapter) Cleanup(ctx context.Context) error {
	session := a.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: dbName})
	defer session.Close(ctx)

	var err error

	_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `
			DROP CONSTRAINT entity_id IF EXISTS
			`, map[string]any{},
		)
		return nil, err
	})
	if err != nil {
		return fmt.Errorf("execute drop identifier_type_value: %w", err)
	}

	_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `
			DROP INDEX identifier_type_value IF EXISTS
			`, map[string]any{},
		)
		return nil, err
	})
	if err != nil {
		return fmt.Errorf("execute drop identifier_type_value: %w", err)
	}

	_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `
			DROP INDEX identifier_duration IF EXISTS
			`, map[string]any{},
		)
		return nil, err
	})
	if err != nil {
		return fmt.Errorf("execute drop identifier_duration: %w", err)
	}

	// Use implicit transactions to delete nodes in batches
	_, err = session.Run(ctx, `
		MATCH (n)
		CALL {
			WITH n
			DETACH DELETE n
		} IN TRANSACTIONS OF 10000 ROWS
	`, nil, neo4j.WithTxTimeout(5*time.Minute))
	if err != nil {
		return fmt.Errorf("execute delete: %w", err)
	}

	return nil
}

func createIndex(ctx context.Context, session neo4j.SessionWithContext) error {
	var err error
	_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `
			CREATE CONSTRAINT entity_id IF NOT EXISTS
			FOR (e:Entity) REQUIRE e.id IS UNIQUE
			`, map[string]any{},
		)
		return nil, err
	})
	if err != nil {
		return fmt.Errorf("create index entity_id: %w", err)
	}

	// Can also use NODE KEY to enforce a unique constraint on identifier type,value
	// But this is an enterprise feature. The index speeds up lookup but cannot
	// enforce uniqueness.
	_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `
			CREATE INDEX identifier_type_value IF NOT EXISTS
			FOR (idn:Identifier) ON (idn.type,idn.value)
			`, map[string]any{},
		)
		return nil, err
	})
	if err != nil {
		return fmt.Errorf("create index identifier_type_value: %w", err)
	}

	_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `
			CREATE INDEX identifier_duration IF NOT EXISTS
			FOR ()-[h:HAS_IDENTIFIER]-() ON (h.from,h.until)
			`, map[string]any{},
		)
		return nil, err
	})
	if err != nil {
		return fmt.Errorf("create index identifier_duration: %w", err)
	}

	return nil
}

func (a *Adapter) LookupEntities(ctx context.Context, lookups []resolve.Lookup) ([]resolve.LookupResult, error) {
	session := a.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: dbName, AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	timerStart := time.Now()

	lookupList := make([][]any, 0, len(lookups))
	for _, lookup := range lookups {
		lookupList = append(lookupList, []any{
			string(lookup.Identifier.Type),
			lookup.Identifier.Value,
			dateToOptionalString(lookup.Date), // can be null
		})
	}

	qb := newQueryBuilder()
	qb.WriteString(`
		WITH $lookupList as lookups
		UNWIND lookups AS lookup
		OPTIONAL MATCH (idn:Identifier {type: lookup[0],value: lookup[1]})
		OPTIONAL MATCH (idn)--(:Entity|Security)-[:HAS_SECURITY*0..1]-(entity:Entity)
		OPTIONAL MATCH (entity)-[hi:HAS_IDENTIFIER]->(i:Identifier)
			WHERE (hi.from <= lookup[2] and (hi.until IS NULL OR lookup[2] < hi.until))
		OPTIONAL MATCH (entity)-[hn:HAS_NAME]->(name:Name)
			WHERE (hn.from <= lookup[2] and (hn.until IS NULL OR lookup[2] < hn.until))
		OPTIONAL MATCH (entity)-[hs:HAS_SECURITY]->(security:Security)-->(si:Identifier)
			WHERE (hs.from <= lookup[2] and (hs.until IS NULL OR lookup[2] < hs.until))
		RETURN lookup,entity,collect(distinct(i)) as identifiers, name, collect(distinct(security)) as securities, collect(distinct(si)) as security_identifiers
	`)
	qb.params["lookupList"] = lookupList

	res, err := neo4j.ExecuteRead(ctx, session,
		func(tx neo4j.ManagedTransaction) ([]resolve.LookupResult, error) {
			result, err := tx.Run(ctx, qb.String(), qb.params)
			if err != nil {
				return nil, fmt.Errorf("run: %w", err)
			}

			lookups := make([]resolve.LookupResult, 0, len(lookups))

			for result.Next(ctx) {
				var entity resolve.Entity

				record := result.Record()

				entityNode, _, err := neo4j.GetRecordValue[neo4j.Node](record, "entity")
				if err != nil {
					return nil, fmt.Errorf("get record value for entity: %w", err)
				}

				id, err := neo4j.GetProperty[string](entityNode, "id")
				if err != nil {
					lookups = append(lookups, resolve.LookupResult{
						Success: false,
					})
					continue
				}

				entity.ID, err = uuid.FromString(id)
				if err != nil {
					return nil, fmt.Errorf("uuid from string: %w", err)
				}

				nameNode, _, err := neo4j.GetRecordValue[neo4j.Node](record, "name")
				if err != nil {
					return nil, fmt.Errorf("get name: %w", err)
				}
				name, ok := nameNode.Props["value"]
				if ok {
					s := name.(string)
					entity.Name = []resolve.DetailDuration[resolve.EntityName]{
						{
							Detail: resolve.EntityName{
								Value: s,
							},
						},
					}
				}

				rawIdentifiers, ok := record.Get("identifiers")
				if ok {
					identifierNodes := rawIdentifiers.([]any)
					identifiers := make([]resolve.Identifier, 0, len(identifierNodes))
					for _, identifier := range identifierNodes {
						identifierNode := identifier.(neo4j.Node)

						identifiers = append(identifiers, resolve.Identifier{
							Type:  resolve.IdentifierType(fmt.Sprint(identifierNode.Props["type"])),
							Value: fmt.Sprint(identifierNode.Props["value"]),
						})
					}
					entity.Identifiers = append(entity.Identifiers, resolve.DetailDuration[[]resolve.Identifier]{
						Detail: identifiers,
					})
					// return the 'point in time' identifiers, not the full history
				}

				rawSecurities, ok := record.Get("securities")
				if ok {
					securityNodes := rawSecurities.([]any)
					securities := make([]resolve.Security, 0, len(securityNodes))
					for _, security := range securityNodes {
						securityNodes := security.(neo4j.Node)
						securities = append(securities, resolve.Security{
							Name: fmt.Sprint(securityNodes.Props["name"]),
						})
					}
					entity.Securities = append(entity.Securities, resolve.DetailDuration[[]resolve.Security]{
						Detail: securities,
					})
				}

				// TODO - map identifiers, security, security identifers in result
				_, _ = record.Get("security_identifiers")

				lookups = append(lookups, resolve.LookupResult{
					Success: true,
					Entity:  &entity,
				})
			}

			if err = result.Err(); err != nil {
				return nil, fmt.Errorf("result error: %w", err)
			}

			return lookups, err
		},
	)
	if err != nil {
		return nil, fmt.Errorf("lookup direct entities: %w", err)
	}
	fmt.Printf("time taken: %v\n", time.Since(timerStart))

	return res, nil
}

// LookupDirectEntities looks up identifiers directly connected to entities such
// as entity_id, and does not support security identifiers.
func (a *Adapter) LookupDirectEntities(ctx context.Context, lookups []resolve.Lookup) ([]resolve.LookupResult, error) {
	session := a.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: dbName, AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	lookupResults := make([]resolve.LookupResult, 0, len(lookups))

	timerStart := time.Now()

	for _, lookup := range lookups {
		qb := newQueryBuilder()

		qb.WriteString(`
			MATCH (idn:Identifier{type:$type,value:$value})<--(ent:Entity)-[hi:HAS_IDENTIFIER]->(i:Identifier)
				WHERE ($date > hi.from and (hi.until IS NULL OR $date < hi.until))
			OPTIONAL MATCH (ent)-[hs:HAS_SECURITY]->(sec:Security)-->(si:Identifier)
				WHERE ($date > hs.from and (hs.until IS NULL OR $date < hs.until))
			WITH ent,collect(distinct(idn)) + collect(i) as ii,collect(distinct(sec)) as securities,collect(si) as security_identifiers
				UNWIND ii as idns
			OPTIONAL MATCH (ent)-[hn:HAS_NAME]->(name:EntityName)
				WHERE ($date > hn.from and (hn.until IS NULL OR $date < hn.until))
			OPTIONAL MATCH (ent)-[hc:HAS_COUNTRY]->(country:Country)
				WHERE ($date > hc.from and (hc.until IS NULL OR $date < hc.until))
			RETURN ent as entity,collect(distinct(idns)) as identifiers,name,country,securities,security_identifiers
		`)
		qb.params["type"] = lookup.Identifier.Type
		qb.params["value"] = lookup.Identifier.Value
		qb.params["date"] = lookup.Date.Format(time.RFC3339)

		var found bool

		ent, err := neo4j.ExecuteRead(ctx, session,
			func(tx neo4j.ManagedTransaction) (*resolve.Entity, error) {
				result, err := tx.Run(ctx, qb.String(), qb.params)
				if err != nil {
					return nil, fmt.Errorf("run: %w", err)
				}

				var res resolve.Entity

				record, err := result.Single(ctx)
				if err != nil {
					// ignore error if not found
					return nil, nil
				}

				rawEntity, ok := record.Get("entity")
				if ok {
					found = true
					entityNode, _ := rawEntity.(neo4j.Node)

					res.ID = uuid.FromStringOrNil(fmt.Sprint(entityNode.Props["id"]))
				}

				rawName, _ := record.Get("name")
				if ok {
					nameNode, _ := rawName.(neo4j.Node)
					name, ok := nameNode.Props["value"]
					if ok {
						s := name.(string)
						res.Name = []resolve.DetailDuration[resolve.EntityName]{
							{
								Detail: resolve.EntityName{
									Value: s,
								},
							},
						}
					}

				}
				// For now, only the entity ID and name is mapped to result
				_, _ = record.Get("country")
				_, _ = record.Get("identifiers")

				return &res, err
			},
		)
		if err != nil {
			return nil, fmt.Errorf("lookup direct entities: %w", err)
		}

		lookupResults = append(lookupResults, resolve.LookupResult{
			Success: found,
			Entity:  ent,
		})
	}

	fmt.Printf("time taken: %v\n", time.Since(timerStart))

	return lookupResults, nil
}

func (a *Adapter) CreateEntities(ctx context.Context, entities []*resolve.Entity) error {
	session := a.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: dbName})
	defer session.Close(ctx)

	var err error
	err = createIndex(ctx, session)
	if err != nil {
		return err
	}

	qb := newQueryBuilder()

	// create entity, name, entity identifiers
	// TODO - country
	entityList := make([][]any, 0, len(entities))
	for _, entity := range entities {
		// ideally would be able to define schema using types, but it seems the
		// n4j driver only accepts arrays and maps, and not structs
		// 'any' is required in order to support nulls

		// nameDurations: [][]{name, from, until(opt)}
		nameDurations := make([][]any, 0, len(entity.Name))
		for _, nameDuration := range entity.Name {
			nameDurations = append(nameDurations, []any{
				nameDuration.Detail.Value,
				dateToOptionalString(&nameDuration.Duration.StartDate),
				dateToOptionalString(nameDuration.Duration.EndDate),
			})
		}

		// identifierDurations: [][]{[]string{idn_type,idn_value},from,until(opt)}
		identifierDurations := make([][]any, 0, len(entity.Identifiers))
		for _, identifierDuration := range entity.Identifiers {
			idns := make([][]string, 0, len(identifierDuration.Detail))
			for _, idn := range identifierDuration.Detail {
				idns = append(idns, []string{string(idn.Type), idn.Value})
			}
			identifierDurations = append(identifierDurations, []any{
				idns,
				dateToOptionalString(&identifierDuration.Duration.StartDate),
				dateToOptionalString(identifierDuration.Duration.EndDate),
			})
		}

		// securityDurations: [][]{name,from,until(opt),[][]string{idn_type,idn_value}}
		securityDurations := make([]any, 0, len(entity.Securities))
		for _, secDuration := range entity.Securities {
			securities := make([]any, 0, len(secDuration.Detail))
			for _, sec := range secDuration.Detail {
				idns := make([][]string, 0, len(sec.Identifiers))
				for _, idn := range sec.Identifiers {
					idns = append(idns, []string{string(idn.Type), idn.Value})
				}

				securities = append(securities, []any{
					sec.Name,
					dateToOptionalString(&secDuration.Duration.StartDate),
					dateToOptionalString(secDuration.Duration.EndDate),
					idns,
				})
			}
			securityDurations = append(securityDurations, securities)
		}

		e := []any{entity.ID.String(), nameDurations, identifierDurations, securityDurations}

		entityList = append(entityList, e)
	}

	qb.WriteString(`
		WITH $entityList as entities
		UNWIND entities AS e
		CREATE (ent:Entity {id: e[0]})
		FOREACH (nd IN e[1] | CREATE (ent)-[:HAS_NAME {from: nd[1], until: nd[2]}]->(:Name {value: nd[0]}))
		FOREACH (idnd IN e[2] |
			FOREACH (idn IN idnd[0] |
				MERGE (im:Identifier {type: idn[0],value: idn[1]})
				CREATE (ent)-[:HAS_IDENTIFIER {from: idnd[1], until: idnd[2]}]->(im)
			)
		)
		FOREACH (s IN e[3] |
			FOREACH (sd IN s |
				CREATE (ent)-[:HAS_SECURITY {from: sd[1], until: sd[2]}]->(sec:Security {name: sd[0]})
				FOREACH (idn IN sd[3] |
					MERGE (im:Identifier {type: idn[0],value: idn[1]})
					CREATE (sec)-[:HAS_IDENTIFIER]->(im)
				)
			)
		)
	`)
	qb.params["entityList"] = entityList

	// note: we could put duration on security identifier instead of security, so
	// that lookup can use the identifier link similar to entity identifier

	// fmt.Println(qb.ToQueryWithParams())

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

	return nil
}

func dateToOptionalString(d *time.Time) *string {
	if d == nil {
		return nil
	}
	s := d.UTC().Format(time.RFC3339)
	return &s
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

func PrettyPrint(i any) string {
	s, _ := json.MarshalIndent(i, "", "\t")
	return string(s)
}
