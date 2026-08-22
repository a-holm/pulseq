package store

import (
	"context"
	"fmt"
)

// The read-only checks the crash harness (#75) runs after every restart.
// They live here, beside the rest of the SQL, because the architecture test
// keeps query strings inside this package and because a checker that writes
// would be part of whatever it is checking.

// IntegrityReport is what SQLite itself says about the database file after a
// crash. WAL recovery on open fixes torn transactions; these two pragmas are
// the proof that it did.
type IntegrityReport struct {
	// Integrity is pragma integrity_check's verdict, "ok" when the file
	// is sound.
	Integrity string

	// ForeignKeyIssues counts foreign_key_check rows: references that
	// point at nothing. Zero is the only healthy number.
	ForeignKeyIssues int
}

// IntegrityCheck runs both pragmas and reports what they said. It reads only;
// SQLite answers integrity_check from the same connection pool every other
// reader uses, so nothing here disturbs a writer.
func (s *Store) IntegrityCheck(ctx context.Context) (IntegrityReport, error) {
	var out IntegrityReport

	if err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		return r.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&out.Integrity)
	}); err != nil {
		return IntegrityReport{}, fmt.Errorf("integrity check: %w", err)
	}

	if err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `PRAGMA foreign_key_check`)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			out.ForeignKeyIssues++
		}
		return rows.Err()
	}); err != nil {
		return IntegrityReport{}, fmt.Errorf("foreign key check: %w", err)
	}

	return out, nil
}

// AbandonedChains names every half written trigger chain: a tick that claims
// it triggered but has no trigger row, an accepted trigger with no run, a run
// without its steps, a run without its queued event. Each pattern is a chain
// a SIGKILL could have cut in the middle if materialisation were not one
// transaction. An empty result says no aborted chain left anything behind.
//
// Skipped ticks and deduped triggers are deliberately not findings: standing
// down is a decision those rows record, not a broken promise.
func (s *Store) AbandonedChains(ctx context.Context) ([]string, error) {
	var out []string

	collect := func(query, format string) error {
		return s.withRead(ctx, func(ctx context.Context, r reader) error {
			rows, err := r.QueryContext(ctx, query)
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					return err
				}
				out = append(out, fmt.Sprintf(format, id))
			}
			return rows.Err()
		})
	}

	checks := []struct {
		query  string
		format string
	}{
		{
			query: `SELECT t.id FROM ticks t
				WHERE t.outcome = 'triggered'
				AND NOT EXISTS (SELECT 1 FROM triggers g WHERE g.tick_id = t.id)`,
			format: "tick %s says triggered but has no trigger row",
		},
		{
			query: `SELECT g.id FROM triggers g
				WHERE g.outcome = 'accepted'
				AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.trigger_id = g.id)`,
			format: "trigger %s was accepted but produced no run",
		},
		{
			query: `SELECT r.id FROM runs r
				WHERE NOT EXISTS (SELECT 1 FROM steps s WHERE s.run_id = r.id)`,
			format: "run %s has no step rows",
		},
		{
			query: `SELECT r.id FROM runs r
				WHERE NOT EXISTS (SELECT 1 FROM run_events e
					WHERE e.run_id = r.id AND e.kind = 'run.queued')`,
			format: "run %s has no queued event",
		},
	}

	for _, c := range checks {
		if err := collect(c.query, c.format); err != nil {
			return nil, fmt.Errorf("abandoned chain sweep: %w", err)
		}
	}
	return out, nil
}
