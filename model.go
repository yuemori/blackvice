package blackvice

import "cloud.google.com/go/spanner"

type Model interface {
	Table() string
	SpannerKey() spanner.Key
	Params() map[string]interface{}
	PrimaryKeys() map[string]interface{}
}
