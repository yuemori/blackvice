package blackvice

import (
	"fmt"
	"strings"
)

type OrderBuilder struct {
	param OrderParam
}

func (b OrderBuilder) IsEmpty() bool {
	return len(b.param) == 0
}

func (b OrderBuilder) Merge(other OrderParam) OrderBuilder {
	param := OrderParam{}
	for k, v := range b.param {
		param[k] = v
	}

	for k, v := range other {
		param[k] = v
	}

	return OrderBuilder{
		param: param,
	}
}

func (b OrderBuilder) Build() string {
	if len(b.param) == 0 {
		return ""
	}

	param := []string{}

	for col, dir := range b.param {
		param = append(param, fmt.Sprintf("%s %s", col, dir))
	}

	return fmt.Sprintf("ORDER BY %s", strings.Join(param, ", "))
}
