package blackvice

import (
	"fmt"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func IsErrNotFound(err error) bool {
	return spanner.ErrCode(err) == codes.NotFound
}

func errRowNotFound(table string, query string) error {
	msg := fmt.Sprintf("row not found(Table: %v, Query: %v)", table, query)
	wrapped := status.Error(codes.NotFound, msg)

	return spanner.ToSpannerError(wrapped)
}

func errMultipleRowsFound(table string, query string) error {
	msg := fmt.Sprintf("more than one row found(Table: %v, Query: %v)", table, query)
	wrapped := status.Error(codes.FailedPrecondition, msg)

	return spanner.ToSpannerError(wrapped)
}
