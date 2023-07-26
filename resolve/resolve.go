package resolve

import (
	"fmt"
	"time"

	"github.com/gofrs/uuid"
)

type (
	IdentifierType string
)

type Entity struct {
	ID          uuid.UUID
	Name        []DetailDuration[EntityName]
	Country     []DetailDuration[EntityCountry]
	Identifiers []DetailDuration[[]Identifier]
	Securities  []DetailDuration[[]Security]
}

type Duration struct {
	StartDate time.Time
	EndDate   *time.Time
}

type DetailDuration[T any] struct {
	Detail   T
	Duration Duration
}

type EntityName struct {
	Value string
}

type EntityCountry struct {
	Value string
}

type Identifier struct {
	Type  IdentifierType
	Value string
}

type Security struct {
	Name        string
	Identifiers []Identifier
	IsPrimary   bool
}

type Lookup struct {
	Date       time.Time
	Identifier Identifier
}

type LookupResult struct {
	Success bool
	Entity  Entity
}

func ResolveEntities(request []Lookup) ([]LookupResult, error) {
	// ...
	return nil, fmt.Errorf("not yet implemented")
}
