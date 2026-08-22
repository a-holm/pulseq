package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/a-holm/paceq/internal/faults"
	"github.com/a-holm/paceq/internal/id"
)

// JobApplyResult says what apply did with one job spec.
type JobApplyResult struct {
	// JobName is the job the input named.
	JobName string

	// VersionID is the id of the version this spec maps to after the apply:
	// the new row when one was written, the existing row otherwise.
	VersionID string

	// Version is that row's number.
	Version int

	// Created reports whether this apply wrote a new job_versions row. A false
	// here is the idempotent case: the file was already loaded as it stands,
	// and the database came out of the transaction byte for byte the same.
	Created bool
}

// ApplyJobs records a batch of job specs and leaves every job pointing at the
// version its spec hashes to. The whole batch is one transaction on purpose:
// apply either lands complete or not at all, because a half applied catalog is
// the one state an operator cannot reason about. Any failure rolls every job
// in the batch back.
//
// Idempotency lives in the schema, not in a check: job_versions carries
// UNIQUE (job_name, spec_hash), the insert comes first, and a conflict means
// the version was already there. Two applies racing on the same file therefore
// end in one row no matter which commit lands first.
//
// The inputs are finished facts: parsing happened before this was called, and
// nothing inside the transaction reads a file or a clock. Ids are minted
// before the transaction opens, for the same reason UpsertJobVersion mints
// its own there.
func (s *Store) ApplyJobs(ctx context.Context, inputs []JobVersionInput) ([]JobApplyResult, error) {
	now := s.clk.Now().UTC()
	at := now.UnixMilli()

	ids := make([]string, len(inputs))
	for i := range inputs {
		id, err := id.New(now)
		if err != nil {
			return nil, fmt.Errorf("mint a job version id for %s: %w", inputs[i].JobName, err)
		}
		ids[i] = id
	}

	results := make([]JobApplyResult, 0, len(inputs))
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		// The transaction can run more than once on a snapshot conflict, so
		// the batch starts from zero every time.
		results = results[:0]
		for i, in := range inputs {
			version, created, err := writeJobVersion(tx, ids[i], at, in)
			if err != nil {
				return err
			}
			results = append(results, JobApplyResult{
				JobName:   in.JobName,
				VersionID: version.ID,
				Version:   version.Version,
				Created:   created,
			})
			// The crash window under apply: the batch's writes so far
			// are inside one uncommitted transaction, and a kill here
			// must leave none of them. This is the all-or-nothing half
			// of the apply promise: no job of the batch can survive
			// alone.
			faults.Point("M1:apply:after_job")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}
