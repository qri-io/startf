package context

import (
	"fmt"

	starlark "github.com/google/skylark"
	"github.com/google/skylark/skylarkstruct"
)

// Context carries values across function calls in a transformation
type Context struct {
	// Results carries the return values of special function calls
	results starlark.StringDict
	values  starlark.StringDict
}

// NewContext creates a new contex
func NewContext() *Context {
	return &Context{
		results: starlark.StringDict{},
		values:  starlark.StringDict{},
	}
}

// Struct delivers this context as a skylark struct
func (c *Context) Struct() *skylarkstruct.Struct {
	dict := starlark.StringDict{
		"set": starlark.NewBuiltin("set", c.setValue),
		"get": starlark.NewBuiltin("get", c.getValue),
	}

	for k, v := range c.results {
		dict[k] = v
	}

	return skylarkstruct.FromStringDict(starlark.String("context"), dict)
}

// SetResult places the result of a function call in the results stringDict
// any results set here will be placed in the context struct field by name
func (c *Context) SetResult(name string, value starlark.Value) {
	c.results[name] = value
}

func (c *Context) setValue(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		key   starlark.String
		value starlark.Value
	)
	if err := starlark.UnpackArgs("set", args, kwargs, "key", &key, "value", &value); err != nil {
		return starlark.None, err
	}

	c.values[string(key)] = value
	return starlark.None, nil
}

func (c *Context) getValue(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var key starlark.String
	if err := starlark.UnpackArgs("get", args, kwargs, "key", &key); err != nil {
		return starlark.None, err
	}
	if v, ok := c.values[string(key)]; ok {
		return v, nil
	}
	return starlark.None, fmt.Errorf("value %s not set in context", string(key))
}
