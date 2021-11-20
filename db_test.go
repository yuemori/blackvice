package blackvice_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/yuemori/blackvice"
	"github.com/yuemori/blackvice/testdata"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	instanceapi "cloud.google.com/go/spanner/admin/instance/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
)

var (
	dsn      string
	project  string
	instance string
	database string
)

func init() {
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}
	emulator_host := env("SPANNER_EMULATOR_HOST", "")
	if emulator_host == "" {
		panic("cannot setup spanner because env 'SPANNER_EMULATOR_HOST' is not set")
	}

	project = env("SPANNER_GCP_PROJECT", "sql-driver-spanner-project")
	instance = env("SPANNER_INSTANCE", "sql-driver-spanner-instance")
	database = env("SPANNER_DATABASE", "testdb")
	dsn = fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database)
}

func runTests(t *testing.T, database string, tests ...func(dbt *blackvice.DB)) {
	ctx := context.Background()
	defer deleteInstance(ctx, t)

	deleteInstance(ctx, t)
	createInstance(ctx, t)

	for _, test := range tests {
		dropDatabase(ctx, database, t)
		createDatabase(ctx, t)
		createTable(ctx, t)

		client, err := spanner.NewClient(ctx, database)
		db := blackvice.New(client)
		if err != nil {
			t.Fatalf("error connecting database: %+v", err)
		}
		defer db.Close()

		test(db)
	}
}

func createDatabase(ctx context.Context, t *testing.T) {
	client, err := adminapi.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatalf("error connecting instance: %+v", err)
	}
	defer client.Close()

	op, err := client.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", project, instance),
		CreateStatement: fmt.Sprintf("CREATE DATABASE %s", database),
	})
	if err != nil {
		t.Fatalf("error create database: %+v", err)
	}

	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("error create database operation: %+v", err)
	}
}

func dropDatabase(ctx context.Context, database string, t *testing.T) {
	client, err := adminapi.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatalf("error connecting instance: %+v", err)
	}
	defer client.Close()

	_, err = client.GetDatabase(ctx, &adminpb.GetDatabaseRequest{
		Name: dsn,
	})
	if status.Code(err) == codes.NotFound {
		return
	}
	if err != nil {
		t.Fatalf("error get database: %+v", err)
	}

	err = client.DropDatabase(ctx, &adminpb.DropDatabaseRequest{
		Database: database,
	})
	if err != nil {
		t.Fatalf("error drop database: %+v", err)
	}
}

func deleteInstance(ctx context.Context, t *testing.T) {
	client, err := instanceapi.NewInstanceAdminClient(ctx)
	if err != nil {
		t.Fatalf("error connecting instance: %+v", err)
	}
	defer client.Close()

	_, err = client.GetInstance(ctx, &instancepb.GetInstanceRequest{
		Name: dsn,
	})
	if status.Code(err) == codes.NotFound {
		return
	} else if err != nil {
		t.Fatalf("error get instance: %+v", err)
	}

	err = client.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{
		Name: fmt.Sprintf("projects/%s/instances/%s", project, instance),
	})
	if err != nil {
		t.Fatalf("error delete instance: %+v", err)
	}
}

func createInstance(ctx context.Context, t *testing.T) {
	client, err := instanceapi.NewInstanceAdminClient(ctx)
	if err != nil {
		t.Fatalf("error connecting instance: %+v", err)
	}
	defer client.Close()

	_, err = client.GetInstance(ctx, &instancepb.GetInstanceRequest{
		Name: fmt.Sprintf("projects/%s/instances/%s", project, instance),
	})
	if err == nil {
		return
	} else if status.Code(err) != codes.NotFound {
		t.Fatalf("error get instance: %+v", err)
	}

	op, err := client.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", project),
		InstanceId: instance,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("/projects/%s/instanceConfigs/test", project),
			DisplayName: "Test Instance",
			NodeCount:   1,
		},
	})
	if err != nil {
		t.Fatalf("error create instance: %+v", err)
	}

	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("error create instance operation: %+v", err)
	}
}

func createTable(ctx context.Context, t *testing.T) {
	client, err := adminapi.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatalf("error connecting database api: %+v", err)
	}
	defer client.Close()

	op, err := client.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   dsn,
		Statements: testdata.CreateTableStatements,
	})
	if err != nil {
		t.Fatalf("error create table: %+v", err)
	}

	if err := op.Wait(ctx); err != nil {
		t.Fatalf("error create table operation: %+v", err)
	}
}

func TestCRUD(t *testing.T) {
	runTests(t, dsn, func(db *blackvice.DB) {
		ctx := context.Background()

		rows, err := db.Relation(&testdata.User{}).All(ctx)
		if err != nil {
			t.Fatalf("Read User failed: %v", err)
		}
		if len(rows) != 0 {
			t.Fatalf("User must be zero: %d", len(rows))
		}

		userId := "userId1"

		user := &testdata.User{
			UserId:    userId,
			Name:      "test",
			Age:       18,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		err = db.ReadWriteTransaction(ctx, func(ctx context.Context, tx blackvice.ReadWriteTransaction) error {
			return tx.Insert(ctx, user)
		})
		if err != nil {
			t.Fatalf("Insert User failed: %v", err)
		}

		res := &testdata.User{UserId: userId}
		err = db.Find(ctx, res)
		if err != nil {
			t.Fatalf("Read User failed: %v", err)
		}

		if res.UserId != user.UserId {
			t.Fatalf("Expected UserId is %s, but %s", user.UserId, res.UserId)
		}

		updatedName := "updated"
		user.Name = updatedName

		err = db.ReadWriteTransaction(ctx, func(ctx context.Context, tx blackvice.ReadWriteTransaction) error {
			return tx.Update(ctx, user)
		})
		if err != nil {
			t.Fatalf("Update User failed: %v", err)
		}

		res2 := &testdata.User{UserId: userId}
		err = db.Find(ctx, res2)
		if err != nil {
			t.Fatalf("Read User failed: %v", err)
		}

		if res2.Name != updatedName {
			t.Fatalf("Expected Name is %s, but %s", user.Name, res.Name)
		}

		err = db.ReadWriteTransaction(ctx, func(ctx context.Context, tx blackvice.ReadWriteTransaction) error {
			return tx.Delete(ctx, user)
		})
		if err != nil {
			t.Fatalf("Delete User failed: %v", err)
		}

		err = db.Find(ctx, &testdata.User{UserId: userId})
		if !blackvice.IsErrNotFound(err) {
			t.Fatalf("Read User failed: %v", err)
		}
	})
}

func TestMutation(t *testing.T) {
	runTests(t, dsn, func(db *blackvice.DB) {
		ctx := context.Background()

		rows, err := db.Relation(&testdata.User{}).All(ctx)
		if err != nil {
			t.Fatalf("Read User failed: %v", err)
		}
		if len(rows) != 0 {
			t.Fatalf("User must be zero: %d", len(rows))
		}

		userId := "userId1"

		user := &testdata.User{
			UserId:    userId,
			Name:      "test",
			Age:       18,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		m := db.Mutation()
		m.Insert(user)

		err = m.Apply(ctx)

		if err != nil {
			t.Fatalf("Insert User failed: %v", err)
		}

		res := &testdata.User{UserId: userId}
		err = db.Find(ctx, res)
		if err != nil {
			t.Fatalf("Read User failed: %v", err)
		}

		if res.UserId != user.UserId {
			t.Fatalf("Expected UserId is %s, but %s", user.UserId, res.UserId)
		}

		updatedName := "updated"
		user.Name = updatedName

		err = db.ReadWriteTransaction(ctx, func(ctx context.Context, tx blackvice.ReadWriteTransaction) error {
			return tx.Update(ctx, user)
		})
		if err != nil {
			t.Fatalf("Update User failed: %v", err)
		}

		res2 := &testdata.User{UserId: userId}
		err = db.Find(ctx, res2)
		if err != nil {
			t.Fatalf("Read User failed: %v", err)
		}

		if res2.Name != updatedName {
			t.Fatalf("Expected Name is %s, but %s", user.Name, res.Name)
		}

		err = db.ReadWriteTransaction(ctx, func(ctx context.Context, tx blackvice.ReadWriteTransaction) error {
			return tx.Delete(ctx, user)
		})
		if err != nil {
			t.Fatalf("Delete User failed: %v", err)
		}

		err = db.Find(ctx, &testdata.User{UserId: userId})
		if !blackvice.IsErrNotFound(err) {
			t.Fatalf("Read User failed: %v", err)
		}
	})
}
