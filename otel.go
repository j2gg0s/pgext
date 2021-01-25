package pgext

import (
	"context"
	"runtime"
	"strings"
	"time"

	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

var (
	instrumentationName     = "github.com/j2gg0s/pgext"
	tracer                  = otel.Tracer(instrumentationName)
	meter                   = otel.Meter(instrumentationName)
	latencyValueRecorder, _ = meter.NewInt64ValueRecorder(
		"go.sql.latency",
		metric.WithDescription("The latency of calls in microsecond"),
	)
	instanceKey      = label.Key("sql.instance")
	methodKey        = label.Key("sql.method")
	tableKey         = label.Key("sql.table")
	statusOKLabel    = label.String("sql.status", "OK")
	statusErrorLabel = label.String("sql.status", "Error")
)

type queryOperation interface {
	Operation() orm.QueryOp
}

// OpenTelemetryHook is a pg.QueryHook that adds OpenTelemetry instrumentation.
type OpenTelemetryHook struct {
	// Caller, if set to true, add caller to attribute
	Caller bool
	// AllowMetric, if set to true, statsd operation's latency.
	AllowMetric bool
}

var _ pg.QueryHook = (*OpenTelemetryHook)(nil)

func (h OpenTelemetryHook) BeforeQuery(ctx context.Context, _ *pg.QueryEvent) (context.Context, error) {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx, nil
	}

	ctx, _ = tracer.Start(ctx, "")
	return ctx, nil
}

func (h OpenTelemetryHook) AfterQuery(ctx context.Context, evt *pg.QueryEvent) error {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() && !h.AllowMetric {
		// fastpath
		return nil
	}

	if span.IsRecording() {
		defer span.End()
	}

	metricLabels := make([]label.KeyValue, 0, 4)
	if h.AllowMetric {
		defer func() {
			latencyValueRecorder.Record(
				ctx,
				time.Since(evt.StartTime).Microseconds(),
				metricLabels...,
			)
		}()
	}

	var operation orm.QueryOp

	if v, ok := evt.Query.(queryOperation); ok {
		operation = v.Operation()
	}

	var query string
	if operation == orm.InsertOp {
		b, err := evt.UnformattedQuery()
		if err != nil {
			return err
		}
		query = string(b)
	} else {
		b, err := evt.FormattedQuery()
		if err != nil {
			return err
		}
		query = string(b)
	}

	if operation != "" {
		if span.IsRecording() {
			span.SetName(string(operation))
		}
		metricLabels = append(metricLabels, methodKey.String(string(operation)))
	} else {
		name := query
		if idx := strings.IndexByte(name, ' '); idx > 0 {
			name = name[:idx]
		}
		if len(name) > 20 {
			name = name[:20]
		}
		if span.IsRecording() {
			span.SetName(strings.TrimSpace(name))
		}
		metricLabels = append(metricLabels, methodKey.String(strings.TrimSpace(name)))
	}

	const queryLimit = 5000
	if len(query) > queryLimit {
		query = query[:queryLimit]
	}

	attrs := make([]label.KeyValue, 0, 10)
	if h.Caller {
		fn, file, line := funcFileLine("github.com/go-pg/pg")
		attrs = append(attrs,
			label.String("frame.func", fn),
			label.String("frame.file", file),
			label.Int("frame.line", line),
		)
	}

	attrs = append(attrs,
		label.String("db.system", "postgres"),
		label.String("db.statement", query),
	)

	if db, ok := evt.DB.(*pg.DB); ok {
		opt := db.Options()
		attrs = append(attrs,
			label.String("db.connection_string", opt.Addr),
			label.String("db.user", opt.User),
			label.String("db.name", opt.Database),
		)
		if len(opt.Database) > 0 {
			metricLabels = append(metricLabels, instanceKey.String(opt.Database))
		}
	}

	if len(evt.Params) > 0 {
		if tableModel, ok := evt.Params[0].(orm.TableModel); ok {
			if len(tableModel.Table().ModelName) > 0 {
				metricLabels = append(
					metricLabels,
					tableKey.String(tableModel.Table().ModelName))
			}
		}
	}

	if evt.Err != nil {
		if span.IsRecording() {
			switch evt.Err {
			case pg.ErrNoRows, pg.ErrMultiRows:
				span.SetStatus(codes.Error, "")
			default:
				span.RecordError(evt.Err)
			}
		}
		metricLabels = append(metricLabels, statusErrorLabel)
	} else if evt.Result != nil {
		numRow := evt.Result.RowsAffected()
		if numRow == 0 {
			numRow = evt.Result.RowsReturned()
		}
		attrs = append(attrs, label.Int("db.rows_affected", numRow))
		metricLabels = append(metricLabels, statusOKLabel)
	}

	if span.IsRecording() {
		span.SetAttributes(attrs...)
	}

	return nil
}

func funcFileLine(pkg string) (string, string, int) {
	const depth = 16
	var pcs [depth]uintptr
	n := runtime.Callers(3, pcs[:])
	ff := runtime.CallersFrames(pcs[:n])

	var fn, file string
	var line int
	for {
		f, ok := ff.Next()
		if !ok {
			break
		}
		fn, file, line = f.Function, f.File, f.Line
		if !strings.Contains(fn, pkg) {
			break
		}
	}

	if ind := strings.LastIndexByte(fn, '/'); ind != -1 {
		fn = fn[ind+1:]
	}

	return fn, file, line
}
