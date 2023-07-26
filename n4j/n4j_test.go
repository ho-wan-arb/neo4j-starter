package n4j

import (
	"context"
	"encoding/json"
	"fmt"
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

	entityCount := 1000
	testEntities := gen.NewEntities(entityCount)

	batchsize := 100
	var cursor int

	for cursor < entityCount {
		err = a.CreateEntities(ctx, testEntities[cursor:cursor+batchsize])
		require.NoError(t, err)

		cursor += batchsize
		fmt.Printf("%s: cursor: %d\n", time.Now().Format("15:04:05"), cursor)
	}

	// fmt.Println(prettyPrint(testEntities[:1]))
}

func prettyPrint(i any) string {
	s, _ := json.MarshalIndent(i, "", "\t")
	return string(s)
}
