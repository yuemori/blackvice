package blackvice

import (
	"context"

	"cloud.google.com/go/spanner"
)

type SpannerReader interface {
	ReadRow(ctx context.Context, table string, key spanner.Key, columns []string) (*spanner.Row, error)
	Read(ctx context.Context, table string, keys spanner.KeySet, columns []string) *spanner.RowIterator
	ReadWithOptions(ctx context.Context, table string, keys spanner.KeySet, columns []string, opts *spanner.ReadOptions) (ri *spanner.RowIterator)
	Query(ctx context.Context, statement spanner.Statement) *spanner.RowIterator
}

type ReadTx struct {
	tx SpannerReader
}

func NewReadTx(tx SpannerReader) *ReadTx {
	return &ReadTx{tx: tx}
}

func (r *ReadTx) Relation(model Model) *QueryContext {
	return NewQueryContext(model, r.tx)
}

func (r *ReadTx) Find(ctx context.Context, model Model) error {
	var columns []string
	for col := range model.Params() {
		columns = append(columns, col)
	}

	row, err := r.tx.ReadRow(ctx, model.Table(), model.SpannerKey(), columns)
	if err != nil {
		return err
	}

	return row.ToStruct(model)
}
