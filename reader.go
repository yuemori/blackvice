package blackvice

import (
	"context"
)

type Reader struct {
	tx ReadTx
}

func NewReader(tx ReadTx) *Reader {
	return &Reader{tx: tx}
}

func (r *Reader) Relation(model Model) *Relation {
	return NewRelation(model, r.tx)
}

func (r *Reader) Find(ctx context.Context, model Model) error {
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
