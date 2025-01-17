package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/AdamShannag/ora-kine/pkg/drivers/generic"
	"github.com/AdamShannag/ora-kine/pkg/drivers/oracle/kine"
	"github.com/AdamShannag/ora-kine/pkg/metrics"
	"github.com/AdamShannag/ora-kine/pkg/server"
	"github.com/AdamShannag/ora-kine/pkg/util"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/sirupsen/logrus"
)

type OracleDialect struct {
	DB *sql.DB

	afterSQL              string
	insertLastInsertIDSQL string
	listRevisionStartSQL  string
	listCurrentSQL        string
	getRevisionAfterSQL   string
	getRevisionSQL        string
	countSQL              string
	deleteSQL             string
	updateCompactSQL      string
	compactSQL            string
	PostCompactSQL        string
	GetSizeSQL            string
	fillSQL               string
	insertSQL             string
	updateSQL             string
	lastInsertID          bool
	FillRetryDuration     time.Duration

	Retry        generic.ErrRetry
	InsertRetry  generic.ErrRetry
	TranslateErr generic.TranslateErr
	ErrCode      generic.ErrCode
}

var (
	revSQL = `
	SELECT MAX(rkv.id) AS id
	FROM KINE rkv`

	compactRevSQL = `
	SELECT MAX(crkv.prev_revision) AS prev_revision
	FROM KINE crkv
	WHERE crkv.name = 'compact_rev_key'`
)

func NewOracleDialect(ctx context.Context, dataSourceName string, connPoolConfig generic.ConnectionPoolConfig, metricsRegisterer prometheus.Registerer) (*OracleDialect, error) {
	var (
		db  *sql.DB
		err error
	)

	for i := 0; i < 300; i++ {
		db, err = openAndTest(dataSourceName)
		if err == nil {
			break
		}

		logrus.Errorf("failed to ping connection: %v", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	configureConnectionPooling(connPoolConfig, db)

	if metricsRegisterer != nil {
		metricsRegisterer.MustRegister(collectors.NewDBStatsCollector(db, "KINE"))
	}

	return &OracleDialect{
		DB: db,
		TranslateErr: func(err error) error {
			if kine.ErrorIs(err, kine.UniqueViolation) {
				return server.ErrKeyExists
			}
			return err
		},
		ErrCode: func(err error) string {
			if err == nil {
				return ""
			}
			if code := kine.ExtractErrorCode(err); code != "-999" {
				return code
			}
			return err.Error()
		},
		afterSQL: `SELECT
	    (
	        SELECT
	            MAX(RKV.ID) AS ID
	        FROM
	            KINE "RKV"
	    ),
	    (
	        SELECT
	            MAX(CRKV.PREV_REVISION) AS PREV_REVISION
	        FROM
	            KINE "CRKV"
	        WHERE
	            CRKV.NAME = 'compact_rev_key'
	    ),
	    KV.ID AS THEID,
	    KV.NAME,
	    KV.CREATED,
	    KV.DELETED,
	    KV.CREATE_REVISION,
	    KV.PREV_REVISION,
	    KV.LEASE,
	    KV.VALUE,
	    KV.OLD_VALUE
	FROM
	    KINE "KV"
	WHERE
	    KV.NAME LIKE :1
	    AND KV.ID > :2
	ORDER BY
	    KV.ID ASC`,

		listRevisionStartSQL: `SELECT
*
FROM
    (
        SELECT
            (
                SELECT
                    MAX(RKV.ID)
                FROM
                    KINE RKV
            ) AS ID,
            (
                SELECT
                    MAX(CRKV.PREV_REVISION)
                FROM
                    KINE CRKV
                WHERE
                    CRKV.NAME = 'compact_rev_key'
            ) AS OUTER_PREV_REVISION,
            KV.ID AS THEID,
            KV.NAME,
            KV.CREATED,
            KV.DELETED,
            KV.CREATE_REVISION,
            KV.PREV_REVISION,
            KV.LEASE,
            KV.VALUE,
            KV.OLD_VALUE
        FROM
            KINE KV
            JOIN (
                SELECT
                    MAX(MKV.ID) AS ID
                FROM
                    KINE MKV
                WHERE
                    MKV.NAME LIKE :1
                    AND MKV.ID <= :2
                GROUP BY
                    MKV.NAME
            ) MAXKV
            ON MAXKV.ID = KV.ID
        WHERE
            KV.DELETED = 0
            OR (:3 = 'y')
    ) LKV
ORDER BY
    LKV.THEID ASC`,
		listCurrentSQL: `SELECT
*
FROM
    (
        SELECT
            (
                SELECT
                    MAX(RKV.ID)
                FROM
                    KINE RKV
            ) AS ID,
            (
                SELECT
                    MAX(CRKV.PREV_REVISION)
                FROM
                    KINE CRKV
                WHERE
                    CRKV.NAME = 'compact_rev_key'
            ) AS OUTER_PREV_REVISION,
            KV.ID AS THEID,
            KV.NAME,
            KV.CREATED,
            KV.DELETED,
            KV.CREATE_REVISION,
            KV.PREV_REVISION,
            KV.LEASE,
            KV.VALUE,
            KV.OLD_VALUE
        FROM
            KINE KV
            JOIN (
                SELECT
                    MAX(MKV.ID) AS ID
                FROM
                    KINE MKV
                WHERE
                    MKV.NAME LIKE :1
                GROUP BY
                    MKV.NAME
            ) MAXKV
            ON MAXKV.ID = KV.ID
        WHERE
            KV.DELETED = 0
            OR (:2 = 'y')
    ) LKV
ORDER BY
    LKV.THEID ASC`,
		getRevisionAfterSQL: `SELECT
*
FROM
    (
        SELECT
            (
                SELECT
                    MAX(RKV.ID)
                FROM
                    KINE RKV
            ) AS ID,
            (
                SELECT
                    MAX(CRKV.PREV_REVISION)
                FROM
                    KINE CRKV
                WHERE
                    CRKV.NAME = 'compact_rev_key'
            ) AS OUTER_PREV_REVISION,
            KV.ID AS THEID,
            KV.NAME,
            KV.CREATED,
            KV.DELETED,
            KV.CREATE_REVISION,
            KV.PREV_REVISION,
            KV.LEASE,
            KV.VALUE,
            KV.OLD_VALUE
        FROM
            KINE KV
            JOIN (
                SELECT
                    MAX(MKV.ID) AS ID
                FROM
                    KINE MKV
                WHERE
                    MKV.NAME LIKE :1
                    AND MKV.ID <= :2
                    AND MKV.ID > (
                        SELECT
                            MAX(IKV.ID) AS ID
                        FROM
                            KINE IKV
                        WHERE
                            IKV.NAME = :3
                            AND IKV.ID <= :4
                    )
                GROUP BY
                    MKV.NAME
            ) MAXKV
            ON MAXKV.ID = KV.ID
        WHERE
            KV.DELETED = 0
            OR (:5 = 'y')
    ) LKV
ORDER BY
    LKV.THEID ASC`,
		countSQL: `SELECT
    (
        SELECT
            MAX(RKV.ID) AS ID
        FROM
            KINE RKV
    ) AS MAX_ID,
    (
        SELECT
            COUNT(*)
        FROM
            (
                SELECT
                    LKV.ID
                FROM
                    (
                        SELECT
                            (
                                SELECT
                                    MAX(RKV.ID)
                                FROM
                                    KINE RKV
                            ) AS ID,
                            (
                                SELECT
                                    MAX(CRKV.PREV_REVISION)
                                FROM
                                    KINE CRKV
                                WHERE
                                    CRKV.NAME = 'compact_rev_key'
                            ) AS OUTER_PREV_REVISION,
                            KV.ID AS THEID,
                            KV.NAME,
                            KV.CREATED,
                            KV.DELETED,
                            KV.CREATE_REVISION,
                            KV.PREV_REVISION,
                            KV.LEASE,
                            KV.VALUE,
                            KV.OLD_VALUE
                        FROM
                            KINE KV
                            JOIN (
                                SELECT
                                    MAX(MKV.ID) AS ID
                                FROM
                                    KINE MKV
                                WHERE
                                    MKV.NAME LIKE :1
                                GROUP BY
                                    MKV.NAME
                            ) MAXKV
                            ON MAXKV.ID = KV.ID
                        WHERE
                            KV.DELETED = 0
                            OR (:2 = 'y')
                    ) LKV
                ORDER BY
                    LKV.THEID ASC
            )
    ) AS ROW_COUNT
FROM
    DUAL`,
		getRevisionSQL: `
			SELECT
			0, 0, KV.ID AS THEID,
			KV.NAME,
			KV.CREATED,
			KV.DELETED,
			KV.CREATE_REVISION,
			KV.PREV_REVISION,
			KV.LEASE,
			KV.VALUE,
			KV.OLD_VALUE
			FROM
			KINE "KV"
			WHERE KV.ID = :1`,
		deleteSQL: `
		DELETE FROM KINE "KV"
		WHERE KV.ID = :1`,
		updateCompactSQL: `
		UPDATE KINE
		SET PREV_REVISION = :1
		WHERE NAME = 'compact_rev_key'`,
		compactSQL: `DELETE FROM KINE KV
WHERE
    EXISTS (
        SELECT
            1
        FROM
            (
                SELECT
                    KP.PREV_REVISION AS ID
                FROM
                    KINE KP
                WHERE
                    KP.NAME != 'compact_rev_key'
                    AND KP.PREV_REVISION != 0
                    AND KP.ID <= :1
                UNION
                SELECT
                    KD.ID            AS ID
                FROM
                    KINE KD
                WHERE
                    KD.DELETED != 0
                    AND KD.ID <= :2
            )    KS
        WHERE
            KV.ID = KS.ID
    )`,
		GetSizeSQL: `SELECT
		SUM(BYTES)/1024/1024
		FROM
		USER_SEGMENTS
		WHERE
		SEGMENT_NAME='KINE'`,
		fillSQL: `INSERT INTO KINE(ID, NAME, CREATED, DELETED, CREATE_REVISION, PREV_REVISION, LEASE, VALUE, OLD_VALUE)
		VALUES(:1, :2, :3, :4, :5, :6, :7, :8, :9)`,
		insertSQL: `INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
	VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
	RETURNING id INTO :9`,
		updateSQL: `UPDATE kine SET value = :7, old_value = :8 WHERE id = :9`,
	}, err

}

func (o *OracleDialect) Migrate(ctx context.Context) {
	if countKV, err := o.countRow(ctx, "key_value"); err != nil || countKV == 0 {
		return
	}

	if countKine, err := o.countRow(ctx, "KINE"); err != nil || countKine != 0 {
		return
	}

	logrus.Infof("Migrating content from old table")
	_, err := o.execute(ctx,
		`INSERT INTO KINE(deleted, create_revision, prev_revision, name, value, created, lease)
					SELECT 0, 0, 0, kv.name, kv.value, 1, CASE WHEN kv.ttl > 0 THEN 15 ELSE 0 END
					FROM key_value kv
						WHERE kv.id IN (SELECT MAX(kvd.id) FROM key_value kvd GROUP BY kvd.name)`)
	if err != nil {
		logrus.Errorf("Migration failed: %v", err)
	}
}

func (o OracleDialect) ListCurrent(ctx context.Context, prefix string, limit int64, includeDeleted bool) (*sql.Rows, error) {
	sql := o.listCurrentSQL

	if limit > 0 {
		sql = fmt.Sprintf("%s FETCH FIRST %d ROWS ONLY", sql, limit)
	}

	y := 'y'
	if !includeDeleted {
		y = 'n'
	}
	return o.query(ctx, sql, prefix, y)
}
func (o OracleDialect) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted bool) (*sql.Rows, error) {
	y := 'y'
	if !includeDeleted {
		y = 'n'
	}
	if startKey == "" {
		sql := o.listRevisionStartSQL
		if limit > 0 {
			sql = fmt.Sprintf("%s FETCH FIRST %d ROWS ONLY", sql, limit)
		}
		return o.query(ctx, sql, prefix, revision, y)
	}

	sql := o.getRevisionAfterSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s FETCH FIRST %d ROWS ONLY", sql, limit)
	}
	return o.query(ctx, sql, prefix, revision, startKey, revision, y)
}
func (o OracleDialect) Count(ctx context.Context, prefix string) (int64, int64, error) {
	var (
		rev sql.NullInt64
		id  int64
	)

	row := o.queryRow(ctx, o.countSQL, prefix, 'n')
	err := row.Scan(&rev, &id)
	log.Println(o.countSQL)
	log.Println(err)
	return rev.Int64, id, err
}
func (o OracleDialect) CurrentRevision(ctx context.Context) (int64, error) {
	var id int64
	row := o.queryRow(ctx, revSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}
func (o OracleDialect) After(ctx context.Context, prefix string, rev, limit int64) (*sql.Rows, error) {
	sql := o.afterSQL

	if limit > 0 {
		sql = fmt.Sprintf("%s FETCH FIRST %d ROWS ONLY", sql, limit)
	}

	return o.query(ctx, sql, prefix, rev)
}
func (o OracleDialect) Insert(ctx context.Context, key string, create, delete bool, createRevision, previousRevision int64, ttl int64, value, prevValue []byte) (id int64, err error) {
	if o.TranslateErr != nil {
		defer func() {
			if err != nil {
				err = o.TranslateErr(err)
			}
		}()
	}

	var cVal int64 = 0
	var dVal int64 = 0
	if create {
		cVal = 1
	}
	if delete {
		dVal = 1
	}

	if o.lastInsertID {
		row, err := o.execute(ctx, o.insertLastInsertIDSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue)
		if err != nil {
			return 0, err
		}
		return row.LastInsertId()
	}

	wait := strategy.Backoff(backoff.Linear(100 + time.Millisecond))

	for i := uint(0); i < 20; i++ {
		_, err = o.execute(ctx, o.insertSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue, &id)
		if kine.ErrorIs(err, kine.CannotBindLong) {
			_, err = o.execute(ctx, o.insertSQL, key, cVal, dVal, createRevision, previousRevision, ttl, nil, nil, &id)
			if err != nil {
				log.Println(err)
			} else {
				_, err = o.execute(ctx, o.updateSQL, value, prevValue, id)
				if err != nil {
					log.Println(err)
				}
			}
		}
		if err != nil && o.InsertRetry != nil && o.InsertRetry(err) {
			wait(i)
			continue
		}
		return id, err
	}
	return
}
func (o OracleDialect) GetRevision(ctx context.Context, revision int64) (*sql.Rows, error) {
	return o.query(ctx, o.getRevisionSQL, revision)

}
func (o OracleDialect) DeleteRevision(ctx context.Context, revision int64) error {
	logrus.Tracef("DELETEREVISION %v", revision)
	_, err := o.execute(ctx, o.deleteSQL, revision)
	return err
}
func (o OracleDialect) GetCompactRevision(ctx context.Context) (int64, error) {
	var id int64
	row := o.queryRow(ctx, compactRevSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}
func (o OracleDialect) SetCompactRevision(ctx context.Context, revision int64) error {
	logrus.Tracef("SETCOMPACTREVISION %v", revision)
	_, err := o.execute(ctx, o.updateCompactSQL, revision)
	return err
}
func (o OracleDialect) Compact(ctx context.Context, revision int64) (int64, error) {
	logrus.Tracef("COMPACT %v", revision)
	res, err := o.execute(ctx, o.compactSQL, revision, revision)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()

}
func (o OracleDialect) PostCompact(ctx context.Context) error {
	logrus.Trace("POSTCOMPACT")
	if o.PostCompactSQL != "" {
		_, err := o.execute(ctx, o.PostCompactSQL)
		return err
	}
	return nil
}
func (o OracleDialect) Fill(ctx context.Context, revision int64) error {
	_, err := o.execute(ctx, o.fillSQL, revision, fmt.Sprintf("gap-%d", revision), 0, 1, 0, 0, 0, nil, nil)
	return err
}
func (o OracleDialect) IsFill(key string) bool {
	return strings.HasPrefix(key, "gap-")
}
func (o OracleDialect) GetSize(ctx context.Context) (int64, error) {
	if o.GetSizeSQL == "" {
		return 0, errors.New("driver does not support size reporting")
	}
	var size int64
	row := o.queryRow(ctx, o.GetSizeSQL)
	if err := row.Scan(&size); err != nil {
		return 0, err
	}
	return size, nil

}
func (o OracleDialect) FillRetryDelay(ctx context.Context) {
	time.Sleep(o.FillRetryDuration)
}

func (o *OracleDialect) query(ctx context.Context, sql string, args ...interface{}) (result *sql.Rows, err error) {
	logrus.Tracef("QUERY %v : %s", args, util.Stripped(sql))
	startTime := time.Now()
	defer func() {
		metrics.ObserveSQL(startTime, o.ErrCode(err), util.Stripped(sql), args)
	}()
	return o.DB.QueryContext(ctx, sql, args...)
}

func (o *OracleDialect) queryRow(ctx context.Context, sql string, args ...interface{}) (result *sql.Row) {
	logrus.Tracef("QUERY ROW %v : %s", args, util.Stripped(sql))
	startTime := time.Now()
	defer func() {
		metrics.ObserveSQL(startTime, o.ErrCode(result.Err()), util.Stripped(sql), args)
	}()
	return o.DB.QueryRowContext(ctx, sql, args...)
}

func (o *OracleDialect) countRow(ctx context.Context, table string) (int64, error) {
	sql := "SELECT COUNT(*) FROM :1"
	logrus.Tracef("QUERY ROW %v : %s", table, util.Stripped(sql))
	startTime := time.Now()
	var count int64
	err := o.DB.QueryRowContext(ctx, sql, table).Scan(&count)
	if err != nil {
		return 0, err
	}
	defer func() {
		metrics.ObserveSQL(startTime, o.ErrCode(err), util.Stripped(sql), table)
	}()

	return count, nil
}

func (d *OracleDialect) execute(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	wait := strategy.Backoff(backoff.Linear(100 + time.Millisecond))
	for i := uint(0); i < 20; i++ {
		logrus.Tracef("EXEC (try: %d) %v : %s", i, args, util.Stripped(sql))
		startTime := time.Now()
		result, err = d.DB.ExecContext(ctx, sql, args...)
		metrics.ObserveSQL(startTime, d.ErrCode(err), util.Stripped(sql), args)
		if err != nil && d.Retry != nil && d.Retry(err) {
			wait(i)
			continue
		}
		return result, err
	}
	return
}
