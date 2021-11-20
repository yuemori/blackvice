package testdata

import (
	"time"

	"cloud.google.com/go/spanner"
)

type User struct {
	UserId    string
	Name      string
	Age       int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (u *User) Table() string {
	return "users"
}

func (u *User) Params() map[string]interface{} {
	return map[string]interface{}{
		"UserId":    u.UserId,
		"Name":      u.Name,
		"Age":       u.Age,
		"CreatedAt": u.CreatedAt,
		"UpdatedAt": u.UpdatedAt,
	}
}

func (u *User) SpannerKey() spanner.Key {
	return spanner.Key{u.UserId}
}

func (u *User) PrimaryKeys() map[string]interface{} {
	return map[string]interface{}{
		"UserId": u.UserId,
	}
}
