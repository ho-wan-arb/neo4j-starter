package resolvetest

import (
	"fmt"
	"time"

	"neo4j-starter/resolve"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/gofrs/uuid"
)

type DataGen struct {
	*gofakeit.Faker
	now      time.Time
	entityID int // incremental
	assetID  int // incremental

	// keep track of what identifiers can be used for lookups
	resolveIDs  []string
	fsEntityIDs []string
	isins       []string
	cusips      []string
}

func NewDataGen(seed int64) *DataGen {
	g := DataGen{
		Faker: gofakeit.New(seed),
		now:   time.Now().UTC(),
	}
	return &g
}

func (g *DataGen) UUID() uuid.UUID {
	return uuid.FromStringOrNil(g.Faker.UUID())
}

func (g *DataGen) NewEntities(n int) []*resolve.Entity {
	entities := make([]*resolve.Entity, 0, n)
	for i := 0; i < n; i++ {
		entities = append(entities, g.NewEntity())
	}
	return entities
}

func (g *DataGen) NewEntity() *resolve.Entity {
	from := g.DateRange(g.now.AddDate(-10, 0, 0), g.now).Truncate(24 * time.Hour)
	e := resolve.Entity{
		ID:          g.UUID(),
		Name:        g.NewEntityNameDurations(g.IntRange(0, 2), from),
		Country:     g.NewEntityCountryDurations(g.IntRange(0, 1), from),
		Identifiers: g.NewIdentifiersDurations(from),
		Securities:  g.NewSecuritiesDurations(4, from),
	}
	return &e
}

func (g *DataGen) NewEntityNameDurations(changes int, from time.Time) []resolve.DetailDuration[resolve.EntityName] {
	names := make([]resolve.DetailDuration[resolve.EntityName], 0, changes)

	for i := 0; i <= changes; i++ {
		name := resolve.DetailDuration[resolve.EntityName]{
			Detail:   resolve.EntityName{Value: g.Company()},
			Duration: resolve.Duration{StartDate: from},
		}

		if i < changes {
			until := g.DateRange(from, g.now).Truncate(24 * time.Hour)
			name.Duration.EndDate = &until
			from = until
		}
		names = append(names, name)
	}

	return names
}

func (g *DataGen) NewEntityCountryDurations(changes int, from time.Time) []resolve.DetailDuration[resolve.EntityCountry] {
	names := make([]resolve.DetailDuration[resolve.EntityCountry], 0, changes)

	for i := 0; i <= changes; i++ {
		name := resolve.DetailDuration[resolve.EntityCountry]{
			Detail:   resolve.EntityCountry{Value: g.Country()},
			Duration: resolve.Duration{StartDate: from},
		}

		if i < changes {
			until := g.DateRange(from, g.now).Truncate(24 * time.Hour)
			name.Duration.EndDate = &until
			from = until
		}
		names = append(names, name)
	}

	return names
}

func (g *DataGen) NewIdentifiersDurations(from time.Time) []resolve.DetailDuration[[]resolve.Identifier] {
	identifiersDurations := make([]resolve.DetailDuration[[]resolve.Identifier], 0, 1)

	srayEntityID := g.newSrayEntityID()

	initialIdentifiers := []resolve.Identifier{
		{
			Type:  "resolve_id",
			Value: g.newResolveID(),
		},
		{
			Type:  "sray_entity_id",
			Value: srayEntityID,
		},
		{
			Type: "fs_entity_id",
			// zero pad sray entity id and add suffix, to retain 1-1 mapping
			Value: g.newFSEntityID(srayEntityID),
		},
	}

	identifiersDurations = append(identifiersDurations, resolve.DetailDuration[[]resolve.Identifier]{
		Detail:   initialIdentifiers,
		Duration: resolve.Duration{StartDate: from},
	})

	// random chance to drop the sray_entity_id + fs_entity_id
	if f := g.Float32Range(0, 1); f < 0.3 {
		until := g.DateRange(from, g.now).Truncate(24 * time.Hour)
		identifiersDurations[0].Duration.EndDate = &until

		updatedIdentifiers := initialIdentifiers[:1]
		identifiersDurations = append(identifiersDurations, resolve.DetailDuration[[]resolve.Identifier]{
			Detail:   updatedIdentifiers,
			Duration: resolve.Duration{StartDate: until},
		})
	}

	return identifiersDurations
}

func (g *DataGen) NewSecuritiesDurations(maxSecurities int, from time.Time) []resolve.DetailDuration[[]resolve.Security] {
	securitiesDurations := make([]resolve.DetailDuration[[]resolve.Security], 0, 1)

	securitiesCount := g.IntRange(0, maxSecurities)
	if securitiesCount == 0 {
		return nil
	}

	initialSecurities := make([]resolve.Security, 0, securitiesCount)

	primary := g.IntRange(0, securitiesCount)
	for i := 0; i < securitiesCount; i++ {
		initialSecurities = append(initialSecurities, resolve.Security{
			Name:        fmt.Sprintf("%s %s", g.Adjective(), g.Noun()),
			Identifiers: g.NewSecurityIdentifiers(),
			IsPrimary:   primary == i,
		})
	}

	securitiesDurations = append(securitiesDurations, resolve.DetailDuration[[]resolve.Security]{
		Detail:   initialSecurities,
		Duration: resolve.Duration{StartDate: from},
	})

	// TODO - add changes to securities

	return securitiesDurations
}

func (g *DataGen) NewSecurityIdentifiers() []resolve.Identifier {
	var identifiers []resolve.Identifier

	// random chance to add identifiers
	if f := g.Float32Range(0, 1); f < 0.8 {
		identifiers = append(identifiers, resolve.Identifier{
			Type:  "asset_id",
			Value: g.newAssetID(),
		})
	}
	if f := g.Float32Range(0, 1); f < 0.8 {
		identifiers = append(identifiers, resolve.Identifier{
			Type:  "isin",
			Value: g.newIsin(),
		})
	}
	if f := g.Float32Range(0, 1); f < 0.8 {
		identifiers = append(identifiers, resolve.Identifier{
			Type:  "cusip",
			Value: g.newCusip(),
		})
	}

	return identifiers
}

func (g *DataGen) srayEntityIDLookup() resolve.Identifier {
	identifierType := "sray_entity_id"
	return resolve.Identifier{
		Type:  resolve.IdentifierType(identifierType),
		Value: fmt.Sprint(g.IntRange(1, g.entityID)),
	}
}

func (g *DataGen) assetIDLookup() resolve.Identifier {
	identifierType := "asset_id"
	return resolve.Identifier{
		Type:  resolve.IdentifierType(identifierType),
		Value: fmt.Sprint(g.IntRange(1, g.assetID)),
	}
}

func (g *DataGen) NewLookups(n, maxEntityCount int, date time.Time) []resolve.Lookup {
	// use map to avoid duplicate identifiers
	lookupsMap := make(map[resolve.Identifier]struct{})

	g.entityID = maxEntityCount
	g.assetID = maxEntityCount

	lookups := make([]resolve.Lookup, 0, n)
	for len(lookups) < n {
		date = date.AddDate(0, 0, g.IntRange(1, 3))

		var identifier resolve.Identifier
		// 50/50 to generate sray entity id vs asset id
		if g.Float32Range(0, 1) > 0.5 {
			identifier = g.srayEntityIDLookup()
		} else {
			identifier = g.assetIDLookup()
		}
		// if _, ok := lookupsMap[identifier]; ok {
		// 	continue
		// }
		lookupsMap[identifier] = struct{}{}

		lookup := resolve.Lookup{
			Date:       &date,
			Identifier: identifier,
		}
		lookups = append(lookups, lookup)
	}
	return lookups
}

func (g *DataGen) newIsin() string {
	isin := g.Isin()
	g.isins = append(g.isins, isin)
	return isin
}

func (g *DataGen) newCusip() string {
	cusip := g.Cusip()
	g.cusips = append(g.cusips, cusip)
	return cusip
}

func (g *DataGen) newResolveID() string {
	id := g.UUID().String()
	g.resolveIDs = append(g.resolveIDs, id)
	return id
}

func (g *DataGen) newFSEntityID(srayEntityID string) string {
	id := fmt.Sprintf("%06s-E", srayEntityID)
	g.fsEntityIDs = append(g.fsEntityIDs, id)
	return id
}

func (g *DataGen) newSrayEntityID() string {
	g.entityID++
	return fmt.Sprint(g.entityID)
}

func (g *DataGen) newAssetID() string {
	g.assetID++
	return fmt.Sprint(g.assetID)
}
