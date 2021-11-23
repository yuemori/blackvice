package blackvice

import (
	"context"

	"cloud.google.com/go/spanner"
)

type Reader interface {
	Relation(Model) *QueryContext

	Find(context.Context, Model) error
}

type ReadWriter interface {
	Relation(Model) *QueryContext

	Insert(context.Context, Model) error
	Update(context.Context, Model) error
	Delete(context.Context, Model) error
}

type Mutator interface {
	Insert(Model)
	Update(Model)
	Delete(Model)
	InsertOrUpdate(Model)

	Do(context.Context, func(context.Context, Mutator) error) error
	Apply(context.Context) error
}

type Relation interface {
	All(ctx context.Context) (rows []Model, err error)
	Query(ctx context.Context, query string, params map[string]interface{}) (rows []Model, err error)
	FindOne(ctx context.Context) (row Model, err error)

	ForceIndex(index string) Relation
	Select(selects []string) Relation
	Where(param WhereParam) Relation
	Order(param OrderParam) Relation
	Limit(limit int) Relation

	SQL() string
	Table() string
}

type DB struct {
	client *spanner.Client
}

func New(client *spanner.Client) *DB {
	return &DB{client: client}
}

func (db *DB) ReadOnlyTransaction(fn func(Reader)) {
	fn(NewReadTx(db.client.ReadOnlyTransaction()))
}

func (db *DB) ReadWriteTransaction(ctx context.Context, fn func(context.Context, ReadWriter) error) error {
	_, err := db.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		return fn(ctx, NewReadWriteTx(tx))
	})

	return err
}

func (db *DB) Relation(model Model) Relation {
	return NewQueryContext(model, db.client.Single())
}

func (db *DB) Reader() Reader {
	return NewReadTx(db.client.Single())
}

func (db *DB) Mutator() Mutator {
	return NewMutation(db.client)
}

func (db *DB) Find(ctx context.Context, model Model) error {
	return db.Reader().Find(ctx, model)
}

func (db *DB) Close() {
	db.client.Close()
}
