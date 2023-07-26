package main

import (
	"context"
	"fmt"
	"log"

	"neo4j-starter/n4j"
)

func run() error {
	ctx := context.Background()

	_, cleanup, err := n4j.Connect(ctx)
	defer cleanup()
	if err != nil {
		return err
	}

	// TODO - run something

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(fmt.Errorf("failed to run: %w", err))
	}
}
