package n4j

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

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

	entityCount := 100_000
	testEntities := gen.NewEntities(entityCount)

	batchsize := 1000
	var cursor int

	for cursor < entityCount {
		fmt.Printf("%s: cursor: %d\n", time.Now().Format("15:04:05"), cursor)

		max := int(math.Min(float64(entityCount), float64(cursor+batchsize)))
		err = a.CreateEntities(ctx, testEntities[cursor:max])
		require.NoError(t, err, fmt.Sprintf("error at cursor: %d", cursor))

		cursor += batchsize
	}

	// fmt.Println(PrettyPrint(testEntities[:1]))
}

// Test lookup requires entities to have been created already.
func TestAdapter_LookupEntities(t *testing.T) {
	ctx := context.Background()

	driver, cleanup, err := Connect(ctx)
	defer cleanup()
	require.NoError(t, err)

	a := NewAdapter(driver)

	lookupDate, err := time.Parse(time.RFC3339, "2021-02-09T00:00:00Z")
	require.NoError(t, err)

	const maxEntityCount = 100_000
	const lookupCount = 1000

	gen := resolvetest.NewDataGen(1)

	lookups := gen.NewLookups(lookupCount, maxEntityCount, lookupDate)

	// fmt.Println(PrettyPrint(lookups))

	lookupResults, err := a.LookupEntities(ctx, lookups)
	require.NoError(t, err)
	// count might not be equal when looking up same identifiers
	require.Equal(t, len(lookups), len(lookupResults), "should get same count as in lookup")

	// fmt.Println(PrettyPrint(lookupResults))

	var found int
	for _, res := range lookupResults {
		if res.Success {
			found++
		}
	}
	fmt.Println("found entities:", found)
}
