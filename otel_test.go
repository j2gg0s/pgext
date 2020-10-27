package pgext

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/go-pg/pg/v10"

	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/trace"
	"go.opentelemetry.io/otel/exporters/stdout"
)

func BenchmarkOtelWithoutParent(b *testing.B) {
	db := pg.Connect(&pg.Options{
		User:     "otsql_user",
		Password: "otsql_password",
		Database: "otsql_db",
	})
	defer db.Close()
	db.AddQueryHook(&OpenTelemetryHook{Caller: true})
	ctx := context.Background()

	benchOtel(ctx, b, db)
}

func BenchmarkOtel(b *testing.B) {
	db := pg.Connect(&pg.Options{
		User:     "otsql_user",
		Password: "otsql_password",
		Database: "otsql_db",
	})
	defer db.Close()
	db.AddQueryHook(&OpenTelemetryHook{})
	ctx, _ := global.TracerProvider().Tracer("github.com/go-pg/pgext").Start(context.Background(), "root", trace.WithNewRoot())

	benchOtel(ctx, b, db)
}

func BenchmarkOtelWithCaller(b *testing.B) {
	db := pg.Connect(&pg.Options{
		User:     "otsql_user",
		Password: "otsql_password",
		Database: "otsql_db",
	})
	defer db.Close()
	db.AddQueryHook(&OpenTelemetryHook{Caller: true})
	ctx, _ := global.TracerProvider().Tracer("github.com/go-pg/pgext").Start(context.Background(), "root", trace.WithNewRoot())

	benchOtel(ctx, b, db)
}

func benchOtel(ctx context.Context, b *testing.B, db *pg.DB) {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS otel_test(id SERIAL PRIMARY KEY)`)
	if err != nil {
		b.Error(err)
	}

	for i := 0; i < b.N; i++ {
		_, err := db.ExecContext(ctx, `SELECT * FROM otel_test WHERE id = ?`, i)
		if err != nil {
			b.Error(err)
		}
	}
}

func init() {
	stdout.InstallNewPipeline([]stdout.Option{
		stdout.WithWriter(ioutil.Discard),
	}, nil)
}
