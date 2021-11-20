package blackvice

import (
	"context"

	"github.com/pkg/errors"

	"cloud.google.com/go/spanner"
)

type ReadWriteTx interface {
	ReadTx

	Update(ctx context.Context, stmt spanner.Statement) (rowCount int64, err error)
}

type ReadWriter struct {
	tx      ReadWriteTx
	builder StatementBuilder
}

func NewReadWriter(tx ReadWriteTx) *ReadWriter {
	return &ReadWriter{tx: tx}
}

func (rw *ReadWriter) Relation(model Model) *Relation {
	return NewRelation(model, rw.tx)
}

func (rw *ReadWriter) Reader() *Reader {
	return NewReader(rw.tx)
}

func (rw *ReadWriter) Find(ctx context.Context, model Model) error {
	return rw.Reader().Find(ctx, model)
}

func (rw *ReadWriter) Insert(ctx context.Context, target Model) error {
	cnt, err := rw.tx.Update(ctx, rw.builder.Insert(target))
	if err != nil {
		return err
	}

	if cnt == 0 {
		return errors.Errorf("Failed to insert %v", target)
	}

	return nil
}

func (rw *ReadWriter) Update(ctx context.Context, target Model) error {
	cnt, err := rw.tx.Update(ctx, rw.builder.Update(target))
	if err != nil {
		return err
	}

	if cnt == 0 {
		return errors.Errorf("Failed to update %v", target)
	}

	return nil
}

func (rw *ReadWriter) Delete(ctx context.Context, target Model) error {
	cnt, err := rw.tx.Update(ctx, rw.builder.Delete(target))
	if err != nil {
		return err
	}

	if cnt == 0 {
		return errors.Errorf("Failed to delete %v", target)
	}

	return nil
}
