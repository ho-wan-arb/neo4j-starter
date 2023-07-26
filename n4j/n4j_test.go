package n4j

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

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

func TestAdapter_CreateFixedEntity(t *testing.T) {
	ctx := context.Background()

	driver, cleanup, err := Connect(ctx)
	defer cleanup()
	require.NoError(t, err)

	a := NewAdapter(driver)

	err = a.Cleanup(ctx)
	require.NoError(t, err)

	err = a.CreateFixedEntity(ctx)
	require.NoError(t, err)
}

func TestAdapter_CreateEntities(t *testing.T) {
	gen := resolvetest.NewDataGen(1)
	testEntity := gen.NewEntity()

	fmt.Println(prettyPrint(testEntity))
}

func prettyPrint(i interface{}) string {
	s, _ := json.MarshalIndent(i, "", "\t")
	return string(s)
}
