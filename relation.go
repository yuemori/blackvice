package blackvice

import (
	"context"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

type ReadTx interface {
	ReadRow(ctx context.Context, table string, key spanner.Key, columns []string) (*spanner.Row, error)
	Read(ctx context.Context, table string, keys spanner.KeySet, columns []string) *spanner.RowIterator
	ReadUsingIndex(ctx context.Context, table, index string, keys spanner.KeySet, columns []string) (ri *spanner.RowIterator)
	Query(ctx context.Context, statement spanner.Statement) *spanner.RowIterator
}

type Relation struct {
	tx    ReadTx
	model Model
}

func NewRelation(model Model, tx ReadTx) *Relation {
	return &Relation{
		tx:    tx,
		model: model,
	}
}

func (r *Relation) SpannerKey() spanner.Key {
	key := spanner.Key{}

	for _, values := range r.model.PrimaryKeys() {
		key = append(key, values)
	}
	return key
}

func (r *Relation) All(ctx context.Context) ([]*spanner.Row, error) {
	var columns []string
	for col := range r.model.Params() {
		columns = append(columns, col)
	}

	iter := r.tx.Read(ctx, r.model.Table(), r.SpannerKey(), columns)
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
