package blackvice

import (
	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"
)

func IsErrNotFound(err error) bool {
	return spanner.ErrCode(err) == codes.NotFound
}
