package blackvice

import "strings"

type SelectBuilder struct {
	selects []string
}

func (b SelectBuilder) IsEmpty() bool {
	return len(b.selects) == 0
}

func (b SelectBuilder) Merge(other []string) SelectBuilder {
	selectMap := map[string]string{}
	for _, v := range b.selects {
		selectMap[v] = v
	}

	for _, v := range other {
		selectMap[v] = v
	}

	selects := []string{}

	for _, v := range selectMap {
		selects = append(selects, v)
	}

	return SelectBuilder{
		selects: selects,
	}
}

func (b SelectBuilder) Build() string {
	if len(b.selects) == 0 {
		return "*"
	}
	var selects []string
	for _, s := range b.selects {
		selects = append(selects, quote(s))
	}
	return strings.Join(selects, ", ")
}
