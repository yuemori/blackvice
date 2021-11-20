package blackvice

import (
	"fmt"
	"strings"

	"cloud.google.com/go/spanner"
)

type StatementBuilder struct {
}

func (b StatementBuilder) Insert(target Model) spanner.Statement {
	var values []string
	var columns []string

	for col, _ := range target.Params() {
		columns = append(columns, b.quote(col))
		values = append(values, b.placeholder(col))
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		target.Table(),
		strings.Join(columns, ", "),
		strings.Join(values, ", "),
	)
	stmt := spanner.NewStatement(sql)
	stmt.Params = target.Params()

	return stmt
}

func (b StatementBuilder) Update(target Model) spanner.Statement {
	var columns []string
	params := map[string]interface{}{}

	whereClause, whereParams := b.buildWherePK(target)
	for k, v := range whereParams {
		params[k] = v
	}

	for col, val := range target.Params() {
		// skip primary key
		if _, ok := target.PrimaryKeys()[col]; ok {
			continue
		}

		columns = append(columns, b.quote(col)+"="+b.placeholder(col))
		params[col] = val
	}

	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		target.Table(),
		strings.Join(columns, ", "),
		whereClause,
	)

	stmt := spanner.NewStatement(sql)
	stmt.Params = params

	return stmt
}

func (b StatementBuilder) Delete(target Model) spanner.Statement {
	whereClause, params := b.buildWherePK(target)

	sql := fmt.Sprintf("DELETE FROM %s WHERE %s",
		target.Table(),
		whereClause,
	)
	stmt := spanner.NewStatement(sql)
	stmt.Params = params

	return stmt
}

func (b StatementBuilder) buildWherePK(target Model) (string, map[string]interface{}) {
	var columns []string
	params := map[string]interface{}{}
	for k, val := range target.PrimaryKeys() {
		key := fmt.Sprintf("pk_%s", k)
		columns = append(columns, b.quote(k)+"="+b.placeholder(key))
		params[key] = val
	}

	return strings.Join(columns, " AND "), params
}

func (b StatementBuilder) quote(str string) string {
	return "`" + str + "`"
}

func (b StatementBuilder) placeholder(str string) string {
	return "@" + str
}
