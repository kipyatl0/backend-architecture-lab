package main

// Прямой разговор с узлами базы. До m08 сцене хватало приложения: всё, что она
// хотела узнать, знал сервис. В m08 вопросы другие — «что лежит на этой копии»
// и «кто из вас сейчас первичный», — и отвечать на них должен сам узел.

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgConnect подключается к узлу, дожидаясь его: реплика после пересборки
// поднимается не мгновенно, а сцена не должна об это спотыкаться.
func pgConnect(ctx context.Context, dsn string, wait time.Duration) (*pgx.Conn, error) {
	deadline := time.Now().Add(wait)
	for {
		conn, err := pgx.Connect(ctx, dsn)
		if err == nil {
			if perr := conn.Ping(ctx); perr == nil {
				return conn, nil
			}
			conn.Close(ctx)
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("узел базы не ответил за %s: %w", wait, err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// pgPool — пул соединений к узлу базы. Одного соединения хватало всем сценам до
// m10: они разговаривали с базой по очереди. Шардам в сцене 32 нужно
// одновременно — у каждого свой работник, — и одно соединение на всех означало
// бы общий ресурс ровно там, где сцена показывает, что общего ресурса нет.
func pgPool(ctx context.Context, dsn string, conns int, wait time.Duration) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = int32(conns)
	deadline := time.Now().Add(wait)
	for {
		pool, perr := pgxpool.NewWithConfig(ctx, cfg)
		if perr == nil {
			if perr = pool.Ping(ctx); perr == nil {
				return pool, nil
			}
			pool.Close()
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("узел базы не ответил за %s: %w", wait, perr)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// inRecovery — вопрос, ответом на который узел и определяется: копия он или
// первичный. Ровно его задаёт сторож в сцене про разрыв сети.
func inRecovery(ctx context.Context, conn *pgx.Conn) (bool, error) {
	var v bool
	err := conn.QueryRow(ctx, `SELECT pg_is_in_recovery()`).Scan(&v)
	return v, err
}

// replicaLag — на сколько секунд копия отстала от первичного узла по времени
// применённых изменений.
func replicaLag(ctx context.Context, conn *pgx.Conn) (float64, error) {
	var v float64
	err := conn.QueryRow(ctx,
		`SELECT coalesce(extract(epoch FROM (now() - pg_last_xact_replay_timestamp())), 0)`).Scan(&v)
	return v, err
}

// orderStatus — что этот узел думает про заказ. Второе значение — false, если
// заказа на нём нет вовсе.
func orderStatus(ctx context.Context, conn *pgx.Conn, id int64) (string, bool, error) {
	var status string
	err := conn.QueryRow(ctx, `SELECT status FROM orders WHERE id = $1`, id).Scan(&status)
	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return status, true, nil
}

// waitCaughtUp ждёт, пока копия применит всё, что первичный узел успел
// записать. Без этого сцена измеряла бы не отставание копии, а хвост
// собственной подготовки: TRUNCATE в подготовке доезжает до копии с той же
// задержкой и держит на таблице исключительную блокировку — чтение под ней
// не «не находит заказ», а честно ждёт.
func waitCaughtUp(ctx context.Context, primary, replica *pgx.Conn, limit time.Duration) error {
	var lsn string
	if err := primary.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&lsn); err != nil {
		return err
	}
	deadline := time.Now().Add(limit)
	for {
		var caught bool
		err := replica.QueryRow(ctx,
			`SELECT pg_last_wal_replay_lsn() >= $1::pg_lsn`, lsn).Scan(&caught)
		if err == nil && caught {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("копия не догнала первичный узел за %s", limit)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// streaming — идёт ли поток изменений на копию прямо сейчас. Спрашивается у
// первичного узла: это он видит своих читателей.
func streaming(ctx context.Context, conn *pgx.Conn) (bool, error) {
	var n int
	err := conn.QueryRow(ctx,
		`SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'`).Scan(&n)
	return n > 0, err
}
