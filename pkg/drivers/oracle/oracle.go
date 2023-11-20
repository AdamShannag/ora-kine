package oracle

import (
	"context"
	"time"

	"github.com/AdamShannag/ora-kine/pkg/drivers/generic"
	"github.com/AdamShannag/ora-kine/pkg/drivers/oracle/kine"
	"github.com/AdamShannag/ora-kine/pkg/logstructured"
	"github.com/AdamShannag/ora-kine/pkg/logstructured/sqllog"
	"github.com/AdamShannag/ora-kine/pkg/server"
	"github.com/AdamShannag/ora-kine/pkg/tls"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultDSN = "oracle://oracle:oralce@localhost/"
)

func New(ctx context.Context, dataSourceName string, tlsInfo tls.Config, connPoolConfig generic.ConnectionPoolConfig, metricsRegisterer prometheus.Registerer) (server.Backend, error) {
	parsedDSN, err := prepareDSN(dataSourceName, tlsInfo)
	if err != nil {
		return nil, err
	}

	dialect, err := NewOracleDialect(ctx, parsedDSN, connPoolConfig, metricsRegisterer)
	if err != nil {
		return nil, err
	}

	dialect.FillRetryDuration = time.Millisecond + 5

	if err := kine.Setup(ctx, dialect.DB); err != nil {
		return nil, err
	}

	dialect.Migrate(context.Background())
	return logstructured.New(sqllog.New(dialect)), nil
}
