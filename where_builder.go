package blackvice

import (
	"fmt"
	"strings"
)

type WhereBuilder struct {
	param WhereParam
}

func (b WhereBuilder) IsEmpty() bool {
	return len(b.param) == 0
}

func (b WhereBuilder) Get(key string) (interface{}, bool) {
	if v, ok := b.param[key]; ok {
		return v, true
	}
	return nil, false
}

func (b WhereBuilder) Merge(other WhereParam) WhereBuilder {
	param := WhereParam{}
	for k, v := range b.param {
		param[k] = v
	}

	for k, v := range other {
		param[k] = v
	}

	return WhereBuilder{
		param: param,
	}
}

func (b WhereBuilder) Build() string {
	if len(b.param) == 0 {
		return ""
	}
	param := []string{}

	// TODO: use reflection
	for col := range b.param {
		param = append(param, quote(col)+"="+placeholder(col))
	}

	return fmt.Sprintf("WHERE %s", strings.Join(param, " AND "))
}

func (b WhereBuilder) Params() WhereParam {
	return b.param
}
