package blackvice

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
)

type SpannerApplyer interface {
	Apply(ctx context.Context, ms []*spanner.Mutation, opts ...spanner.ApplyOption) (commitTimestamp time.Time, err error)
}

type Mutation struct {
	ms      []*spanner.Mutation
	mu      sync.RWMutex
	applyer SpannerApplyer
}

func NewMutation(applyer SpannerApplyer) *Mutation {
	return &Mutation{
		ms:      make([]*spanner.Mutation, 0),
		applyer: applyer,
	}
}

func (m *Mutation) Do(ctx context.Context, fn func(context.Context, Mutator) error) error {
	if err := fn(ctx, m); err != nil {
		return err
	}
	return m.Apply(ctx)
}

func (m *Mutation) Apply(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, err := m.applyer.Apply(ctx, m.ms)
	m.ms = []*spanner.Mutation{}

	return err
}

func (m *Mutation) Insert(model Model) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var columns []string
	var values []interface{}
	for col, val := range model.Params() {
		columns = append(columns, col)
		values = append(values, val)
	}
	m.ms = append(m.ms, spanner.Insert(model.Table(), columns, values))
}

func (m *Mutation) Update(model Model) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var columns []string
	var values []interface{}
	for col, val := range model.Params() {
		columns = append(columns, col)
		values = append(values, val)
	}
	m.ms = append(m.ms, spanner.Update(model.Table(), columns, values))
}

func (m *Mutation) Delete(model Model) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ms = append(m.ms, spanner.Delete(model.Table(), model.SpannerKey()))
}

func (m *Mutation) InsertOrUpdate(model Model) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var columns []string
	var values []interface{}
	for col, val := range model.Params() {
		columns = append(columns, col)
		values = append(values, val)
	}
	m.ms = append(m.ms, spanner.InsertOrUpdate(model.Table(), columns, values))
}
