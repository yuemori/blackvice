package blackvice

import (
	"context"

	"cloud.google.com/go/spanner"
)

type ReadOnlyTransaction interface {
	Relation(Model) *Relation
}

type ReadWriteTransaction interface {
	Relation(Model) *Relation

	Insert(context.Context, Model) error
	Update(context.Context, Model) error
	Delete(context.Context, Model) error
}

type DB struct {
	client *spanner.Client
}

func New(client *spanner.Client) *DB {
	return &DB{client: client}
}

func (db *DB) ReadOnlyTransaction(fn func(ReadOnlyTransaction)) {
	fn(NewReader(db.client.ReadOnlyTransaction()))
}

func (db *DB) ReadWriteTransaction(ctx context.Context, fn func(context.Context, ReadWriteTransaction) error) error {
	_, err := db.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		return fn(ctx, NewReadWriter(tx))
	})

	return err
}

func (db *DB) Relation(model Model) *Relation {
	return NewRelation(model, db.client.Single())
}

func (db *DB) Reader() *Reader {
	return NewReader(db.client.Single())
}

func (db *DB) Mutation() *Mutation {
	return NewMutation(db.client)
}

func (db *DB) Find(ctx context.Context, model Model) error {
	return db.Reader().Find(ctx, model)
}

func (db *DB) Close() {
	db.client.Close()
}
