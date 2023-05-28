// EnqueueIt
// Copyright © 2023 Cyber Cloud Systems LLC

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package postgres

import (
	"time"

	"database/sql"

	"github.com/cybercloudsys/enqueueit-go"
	pq "github.com/lib/pq"
)

type Storage struct {
	Db     *sql.DB
	Config *enqueueit.Configuration
}

func (sqlStorage *Storage) Connect(config *enqueueit.Configuration) error {
	db, err := sql.Open("postgres", config.LongTermStorageConfig)
	if err != nil {
		return err
	}
	sqlStorage.Db = db
	sqlStorage.Config = config
	return nil
}

func (sqlStorage *Storage) SaveBackgroundJobs(bgJobs []*enqueueit.BackgroundJob) error {
	tx, err := sqlStorage.Db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(`CREATE TEMP TABLE temp_jobs
		(id char(36) NOT NULL, name text, queue text, app_name text, argument text,
		created_at timestamp without time zone NOT NULL, is_recurring boolean NOT NULL,
		start_at timestamp without time zone, active boolean NOT NULL, recurring text,
		tries int, type smallint, after_background_job_ids text);
		CREATE TEMP TABLE temp_bg_jobs
		(id char(36) NOT NULL, job_id char(36), processed_by char(36), server text,
		created_at timestamp without time zone NOT NULL, status smallint NOT NULL,
		job_error text, started_at timestamp without time zone,
		completed_at timestamp without time zone,
		last_activity timestamp without time zone, logs text);`)
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(pq.CopyIn("temp_jobs", "id", "name", "queue", "app_name", "argument", "created_at",
		"is_recurring", "start_at", "active", "recurring", "tries", "type", "after_background_job_ids"))
	if err != nil {
		return err
	}

	for _, v := range bgJobs {
		_, err = stmt.Exec(v.Job.Id, v.Job.Name, v.Job.Queue, v.Job.AppName, v.Job.Argument, v.Job.CreatedAt, v.Job.IsRecurring,
			v.Job.StartAt, v.Job.Active, v.Job.Recurring, v.Job.Tries, v.Job.Type, v.Job.AfterBackgroundJobIds)
		if err != nil {
			return err
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	stmt, err = tx.Prepare(pq.CopyIn("temp_bg_jobs", "id", "job_id", "processed_by", "server", "created_at",
		"status", "job_error", "started_at", "completed_at", "last_activity", "logs"))
	if err != nil {
		return err
	}

	for _, v := range bgJobs {
		_, err = stmt.Exec(v.Id, v.JobId, v.ProcessedBy, v.Server, v.CreatedAt, v.Status,
			v.JobError, v.StartedAt, v.CompletedAt, v.LastActivity, v.Logs)
		if err != nil {
			return err
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO "EnqueueIt".jobs (id,name,queue,app_name,argument,created_at,is_recurring,start_at,active,recurring,tries,type,after_background_job_ids)
		SELECT id,name,queue,app_name,argument,created_at, is_recurring,start_at,active,recurring,tries,type,after_background_job_ids FROM temp_jobs
		ON CONFLICT DO NOTHING;
		INSERT INTO "EnqueueIt".background_jobs (id,job_id,processed_by,server,created_at,status,job_error,started_at,completed_at,last_activity,logs)
		SELECT id,job_id,processed_by,server,created_at,status,job_error,started_at,completed_at,last_activity,logs FROM temp_bg_jobs
		ON CONFLICT (id) DO UPDATE SET job_id=excluded.job_id,processed_by=excluded.processed_by,server=excluded.server,status=excluded.status,
			job_error=excluded.job_error,started_at=excluded.started_at,completed_at=excluded.completed_at,last_activity=excluded.last_activity,logs=excluded.logs;
		DROP TABLE temp_jobs; DROP TABLE temp_bg_jobs;`)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (sqlStorage *Storage) DeleteExpired() error {
	stmt, err := sqlStorage.Db.Prepare(`DELETE FROM "EnqueueIt".background_jobs WHERE completed_at < $1`)
	if err != nil {
		return err
	}
	_, err = stmt.Exec(time.Now().UTC().AddDate(0, 0, -sqlStorage.Config.StorageExpirationInDays))
	if err != nil {
		return err
	}
	_, err = sqlStorage.Db.Exec(`DELETE FROM "EnqueueIt".jobs jobs WHERE NOT EXISTS(SELECT id FROM "EnqueueIt".background_jobs WHERE job_id = jobs.id)`)
	if err != nil {
		return err
	}
	return nil
}
