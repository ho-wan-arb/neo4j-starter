package n4j

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"neo4j-starter/resolve"
	"neo4j-starter/resolve/resolvetest"

	"github.com/stretchr/testify/require"
)

// These tests require a neo4j server to be running on localhost.

func TestAdapter_Cleanup(t *testing.T) {
	ctx := context.Background()

	driver, cleanup, err := Connect(ctx)
	defer cleanup()
	require.NoError(t, err)

	a := NewAdapter(driver)

	err = a.Cleanup(ctx)
	require.NoError(t, err)
}

func TestAdapter_CreateEntities(t *testing.T) {
	ctx := context.Background()

	driver, cleanup, err := Connect(ctx)
	defer cleanup()
	require.NoError(t, err)

	a := NewAdapter(driver)

	err = a.Cleanup(ctx)
	require.NoError(t, err)

	gen := resolvetest.NewDataGen(1)

	entityCount := 100
	testEntities := gen.NewEntities(entityCount)

	batchsize := 10
	var cursor int

	for cursor < entityCount {
		err = a.CreateEntities(ctx, testEntities[cursor:cursor+batchsize])
		require.NoError(t, err, fmt.Sprintf("error at cursor: %d", cursor))

		cursor += batchsize
		fmt.Printf("%s: cursor: %d\n", time.Now().Format("15:04:05"), cursor)
	}

	// fmt.Println(PrettyPrint(testEntities[:1]))
}

// Test lookup requires entities to have been created already.
func TestAdapter_LookupDirectEntities(t *testing.T) {
	ctx := context.Background()

	driver, cleanup, err := Connect(ctx)
	defer cleanup()
	require.NoError(t, err)

	a := NewAdapter(driver)

	lookupDate, err := time.Parse(time.RFC3339, "2023-02-09T00:00:00Z")
	require.NoError(t, err)

	// entity ids should have been created in the DB already
	// const maxSrayEntityID = 1000
	const lookupCount = 1000

	var lookups []resolve.Lookup
	for i := 0; i < lookupCount; i++ {
		lookup := resolve.Lookup{
			Date: lookupDate,
			Identifier: resolve.Identifier{
				Type: "sray_entity_id",
				// Value: fmt.Sprint(rand.Intn(maxSrayEntityID)),
				Value: fmt.Sprint(i),
			},
		}
		lookups = append(lookups, lookup)
	}

	lookupResults, err := a.LookupDirectEntities(ctx, lookups)
	require.NoError(t, err)
	require.Len(t, lookupResults, lookupCount)

	var found int
	for _, res := range lookupResults {
		if res.Success {
			found++
		}
	}
	fmt.Println("found entities:", found)
}

func PrettyPrint(i any) string {
	s, _ := json.MarshalIndent(i, "", "\t")
	return string(s)
}
