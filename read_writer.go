package blackvice

import (
	"context"

	"github.com/pkg/errors"

	"cloud.google.com/go/spanner"
)

type SpannerReadWriter interface {
	SpannerReader

	Update(ctx context.Context, stmt spanner.Statement) (rowCount int64, err error)
}

type ReadWriteTx struct {
	tx      SpannerReadWriter
	builder StatementBuilder
}

func NewReadWriteTx(tx SpannerReadWriter) *ReadWriteTx {
	return &ReadWriteTx{tx: tx}
}

func (rw *ReadWriteTx) Relation(model Model) *QueryContext {
	return NewQueryContext(model, rw.tx)
}

func (rw *ReadWriteTx) Reader() *ReadTx {
	return NewReadTx(rw.tx)
}

func (rw *ReadWriteTx) Find(ctx context.Context, model Model) error {
	return rw.Reader().Find(ctx, model)
}

func (rw *ReadWriteTx) Insert(ctx context.Context, target Model) error {
	cnt, err := rw.tx.Update(ctx, rw.builder.Insert(target))
	if err != nil {
		return err
	}

	if cnt == 0 {
		return errors.Errorf("Failed to insert %v", target)
	}

	return nil
}

func (rw *ReadWriteTx) Update(ctx context.Context, target Model) error {
	cnt, err := rw.tx.Update(ctx, rw.builder.Update(target))
	if err != nil {
		return err
	}

	if cnt == 0 {
		return errors.Errorf("Failed to update %v", target)
	}

	return nil
}

func (rw *ReadWriteTx) Delete(ctx context.Context, target Model) error {
	cnt, err := rw.tx.Update(ctx, rw.builder.Delete(target))
	if err != nil {
		return err
	}

	if cnt == 0 {
		return errors.Errorf("Failed to delete %v", target)
	}

	return nil
}
