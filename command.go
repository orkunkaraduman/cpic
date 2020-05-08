package main

import (
	"context"

	"gitlab.com/orkunkaraduman/cpic/catalog"
)

type command struct {
	WorkDir string
	TmpDir string
	Catalog *catalog.Catalog
}

func (c *command) Command() *command {
	return c
}

func (c *command) Prepare() {
	panic("not implemented")
}

func (c *command) Run(ctx context.Context) {
	panic("not implemented")
}
