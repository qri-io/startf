package context

import (
	"context"

	"github.com/google/skylark"
	"github.com/google/skylark/skylarkstruct"
)

// Context carries values across function calls in a transformation
type Context struct {
	// Results carries the return values of special function calls
	Results skylark.StringDict
}

// NewContext creates a new contex
func NewContext() *Context {
	context.Background()
	return &Context{
		Results: skylark.StringDict{},
	}
}

// Struct delivers this context as a skylark struct
func (c *Context) Struct() *skylarkstruct.Struct {
	return skylarkstruct.FromStringDict(skylark.String("context"), skylark.StringDict{
		"results": skylarkstruct.FromStringDict(skylark.String("results"), c.Results),
	})
}
