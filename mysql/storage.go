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

package mysql

import (
	"database/sql"
	"time"

	"github.com/cybercloudsys/enqueueit-go"
	_ "github.com/go-sql-driver/mysql"
)

type Storage struct {
	Db     *sql.DB
	Config *enqueueit.Configuration
}

func (sqlStorage *Storage) Connect(config *enqueueit.Configuration) error {
	db, err := sql.Open("mysql", config.LongTermStorageConfig)
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

	_, err = tx.Exec(`CREATE TEMPORARY TABLE TempJobs (id char(36) NOT NULL, name text NULL,
		queue text NULL, app_name text NULL, argument text NULL, created_at datetime(6) NOT NULL,
		is_recurring tinyint(1) NOT NULL, start_at datetime(6) NULL, active tinyint(1) NOT NULL, recurring text NULL, tries int NOT NULL,
		type int NOT NULL, after_background_job_ids text NULL);`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`CREATE TEMPORARY TABLE TempBgJobs (id char(36) NOT NULL, job_id char(36) NOT NULL,
		processed_by char(36) NULL, server text NULL, created_at datetime(6) NOT NULL,
		status int NOT NULL, job_error text NULL, started_at datetime(6) NULL, completed_at datetime(6) NULL,
		last_activity datetime(6) NULL, logs text NULL);`)
	if err != nil {
		return err
	}

	args := make([]interface{}, 13)
	stmt, err := tx.Prepare(`INSERT INTO TempJobs (id,name,queue,app_name,argument,created_at,is_recurring,start_at,
		active,recurring, tries,type,after_background_job_ids) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`)
	if err != nil {
		return err
	}
	for _, v := range bgJobs {
		args[0] = v.JobId
		args[1] = v.Job.Name
		args[2] = v.Job.Queue
		args[3] = v.Job.AppName
		args[4] = v.Job.Argument
		args[5] = v.Job.CreatedAt
		args[6] = v.Job.IsRecurring
		args[7] = v.Job.StartAt
		args[8] = v.Job.Active
		args[9] = v.Job.Recurring
		args[10] = v.Job.Tries
		args[11] = v.Job.Type
		args[12] = v.Job.AfterBackgroundJobIds

		_, err = stmt.Exec(args...)
		if err != nil {
			return err
		}
	}

	args = make([]interface{}, 11)
	stmt, err = tx.Prepare(`INSERT INTO TempBgJobs (id,job_id,processed_by,server,created_at,status,
		job_error,started_at, completed_at,last_activity,logs) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`)
	if err != nil {
		return err
	}

	for _, v := range bgJobs {
		args[0] = v.Id
		args[1] = v.JobId
		args[2] = v.ProcessedBy
		args[3] = v.Server
		args[4] = v.CreatedAt
		args[5] = v.Status
		args[6] = v.JobError
		args[7] = v.StartedAt
		args[8] = v.CompletedAt
		args[9] = v.LastActivity
		args[10] = v.Logs

		_, err = stmt.Exec(args...)
		if err != nil {
			return err
		}
	}

	_, err = tx.Exec(`INSERT IGNORE INTO jobs (id,name,queue,app_name,argument,created_at,is_recurring,start_at,active,recurring,tries,type,after_background_job_ids)
		SELECT id,name,queue,app_name,argument,created_at,is_recurring,start_at,active,recurring,tries,type,after_background_job_ids FROM TempJobs;`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`INSERT INTO background_jobs (id,job_id,processed_by,server,created_at,status,job_error,started_at,completed_at,last_activity,logs)
		SELECT id,job_id,processed_by,server,created_at, status,job_error,started_at, completed_at,last_activity,logs FROM TempBgJobs tb
		ON DUPLICATE KEY UPDATE job_id=tb.job_id,processed_by=tb.processed_by,server=tb.server,status=tb.status,job_error=tb.job_error,
			started_at=tb.started_at,completed_at=tb.completed_at,last_activity=tb.last_activity,logs=tb.logs;`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`DROP TABLE TempJobs;`)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`DROP TABLE TempBgJobs;`)
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
	stmt, err := sqlStorage.Db.Prepare(`DELETE FROM background_jobs WHERE completed_at < ?`)
	if err != nil {
		return err
	}
	_, err = stmt.Exec(time.Now().UTC().AddDate(0, 0, -sqlStorage.Config.StorageExpirationInDays))
	if err != nil {
		return err
	}
	_, err = sqlStorage.Db.Exec(`DELETE FROM jobs WHERE NOT EXISTS (SELECT id FROM background_jobs WHERE job_id = jobs.id)`)
	if err != nil {
		return err
	}
	return nil
}
