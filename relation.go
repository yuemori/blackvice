package blackvice

import (
	"context"
	"fmt"
	"reflect"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

type Direction string

var (
	ASC  Direction = "ASC"
	DESC Direction = "DESC"
)

type OrderParam map[string]Direction
type WhereParam map[string]interface{}

type QueryContext struct {
	tx            SpannerReader
	selectBuilder SelectBuilder
	model         Model
	index         string
	limit         int
	whereBuilder  WhereBuilder
	orderBuilder  OrderBuilder
}

func NewQueryContext(model Model, tx SpannerReader) *QueryContext {
	return &QueryContext{
		tx:            tx,
		model:         model,
		selectBuilder: SelectBuilder{selects: []string{}},
		whereBuilder:  WhereBuilder{},
		orderBuilder:  OrderBuilder{},
		limit:         0,
	}
}

func (b *QueryContext) Select(other []string) Relation {
	r := b
	r.selectBuilder = b.selectBuilder.Merge(other)
	return r
}

func (b *QueryContext) Where(param WhereParam) Relation {
	r := b
	r.whereBuilder = r.whereBuilder.Merge(param)
	return r
}

func (b *QueryContext) Order(param OrderParam) Relation {
	r := b
	r.orderBuilder = b.orderBuilder.Merge(param)
	return r
}

func (b *QueryContext) Limit(limit int) Relation {
	r := b
	r.limit = limit
	return r
}

func (b *QueryContext) ForceIndex(index string) Relation {
	r := b
	r.index = index
	return r
}

func (b *QueryContext) Table() string {
	return b.model.Table()
}

func (b *QueryContext) SQL() string {
	index := ""
	if b.index != "" {
		index = fmt.Sprintf("{FORCE_INDEX: %s}", b.index)
	}

	limit := ""
	if b.limit != 0 {
		limit = fmt.Sprintf("LIMIT %d", b.limit)
	}

	return fmt.Sprintf(
		"SELECT %s FROM %s%s %s %s %s",
		b.selectBuilder.Build(),
		b.Table(),
		index,
		b.whereBuilder.Build(),
		b.orderBuilder.Build(),
		limit,
	)
}

func (b *QueryContext) All(ctx context.Context) ([]Model, error) {
	return b.Query(ctx, b.SQL(), b.whereBuilder.Params())
}

func (b *QueryContext) FindOne(ctx context.Context) (Model, error) {
	q := b.Limit(1)

	rows, err := q.All(ctx)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errRowNotFound(q.Table(), q.SQL())
	}
	if len(rows) > 1 {
		return nil, errMultipleRowsFound(q.Table(), q.SQL())
	}

	return rows[0], nil
}

func (b *QueryContext) Query(ctx context.Context, query string, params map[string]interface{}) ([]Model, error) {
	stmt := spanner.NewStatement(query)
	stmt.Params = params
	iter := b.tx.Query(ctx, stmt)

	rows, err := b.buildRows(iter)
	if err != nil {
		return nil, err
	}

	res := []Model{}

	for _, row := range rows {
		rt := reflect.TypeOf(b.model)
		ptr := reflect.New(rt)
		rv := reflect.New(rt.Elem())
		ptr.Elem().Set(rv)
		if err := row.ToStruct(rv.Interface()); err != nil {
			return nil, err
		}
		val, ok := rv.Interface().(Model)
		if !ok {
			return nil, errors.New("Internal error occurred")
		}
		res = append(res, val)
	}

	return res, nil
}

func (b *QueryContext) buildRows(iter *spanner.RowIterator) ([]*spanner.Row, error) {
	defer iter.Stop()

	rows := []*spanner.Row{}

	for {
		row, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func (b *QueryContext) columns() []string {
	var columns []string
	for col := range b.model.Params() {
		columns = append(columns, col)
	}
	return columns
}
