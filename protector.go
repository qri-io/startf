package skytf

import (
	"fmt"

	"github.com/google/skylark"
	"github.com/google/skylark/skylarkstruct"
)

// Protector wraps a skylark module with a set of rules that control when a module method can be called
type Protector struct {
	step   string
	Module string
	Rules  []Rule
}

// Rule allows or denies the execution of a method in a step
// empty string functions as a wildcard / match all, for example:
// Rule{"", "", true} // allow all methods to execute on all steps
// Rule{"", "", false} // deny all methods to execute on all steps
// Rule{"foo", "bar", true} // allow method "bar" in step "foo"
// Rule{"foo", "", true} // allow all methods in step "foo"
type Rule struct {
	Step, Method string
	Allow        bool
}

// SetStep updates the current step of execution
func (p *Protector) SetStep(step string) {
	p.step = step
}

// NewProtectedBuiltin wraps a builtin method with a rules check
func (p *Protector) NewProtectedBuiltin(name string, fn *skylark.Builtin) *skylark.Builtin {
	protected := func(thread *skylark.Thread, bi *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
		if !p.allowed(name) {
			return nil, fmt.Errorf("%s.%s cannot be called in %s step", p.Module, name, p.step)
		}
		return fn.Call(thread, args, kwargs)
	}
	return skylark.NewBuiltin(name, protected)
}

func (p *Protector) allowed(method string) (allowed bool) {
	for _, r := range p.Rules {
		if (r.Step == "" || p.step == r.Step) && (r.Method == "" || r.Method == method) {
			allowed = r.Allow
		}
	}
	return
}

// ProtectMethods wraps an input StringDict with protector funcs
func (p *Protector) ProtectMethods(dict skylark.StringDict) {
	for key, x := range dict {
		switch x.Type() {
		case "struct":
			if st, ok := x.(*skylarkstruct.Struct); ok {
				d := skylark.StringDict{}
				st.ToStringDict(d)
				p.ProtectMethods(d)
				dict[key] = skylarkstruct.FromStringDict(skylarkstruct.Default, d)
			} else {
				panic("skylark value claimed to be a struct but wasn't a function pointer")
			}
		case "builtin_function_or_method":
			if bi, ok := x.(*skylark.Builtin); ok {
				dict[key] = p.NewProtectedBuiltin(key, bi)
			} else {
				panic("skylark value claimed to be a builtin but wasn't a function pointer")
			}
			// case "function":
			// 	if fn, ok := x.(*skylark.Function); ok {
			// 		fmt.Printf("protecting: %s.%s", p.Module, key)
			// 		dict[key] = p.NewProtectedBuiltin(key, fn)
			// 	} else {
			// 		panic("skylark value claimed to be a function but wasn't a function pointer")
			// 	}
		}
	}
}
