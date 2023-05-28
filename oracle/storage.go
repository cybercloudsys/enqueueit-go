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

package oracle

import (
	"database/sql"
	"time"

	"github.com/cybercloudsys/enqueueit-go"
	_ "github.com/godror/godror"
)

type Storage struct {
	Db     *sql.DB
	Config *enqueueit.Configuration
}

func (sqlStorage *Storage) Connect(config *enqueueit.Configuration) error {
	db, err := sql.Open("godror", config.LongTermStorageConfig)
	if err != nil {
		return err
	}
	sqlStorage.Db = db
	sqlStorage.Config = config
	return nil
}

func (sqlStorage *Storage) SaveBackgroundJobs(bgJobs []*enqueueit.BackgroundJob) error {
	args := make([]interface{}, 13)
	tx, err := sqlStorage.Db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(`CREATE GLOBAL TEMPORARY TABLE temp_jobs (id CHAR(36), name NVARCHAR2(2000),
		queue NVARCHAR2(2000), app_name NVARCHAR2(2000), argument NVARCHAR2(2000), created_at TIMESTAMP(7),
		is_recurring NUMBER(1), start_at TIMESTAMP(7), active NUMBER(1), recurring NVARCHAR2(2000), tries NUMBER(10),
		after_background_job_ids NVARCHAR2(2000), "type" NUMBER(10))`)
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`INSERT INTO temp_jobs (id,name,queue,app_name,argument,created_at,is_recurring,
		start_at,active,recurring,tries,"type",after_background_job_ids)
		VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)`)

	for _, v := range bgJobs {
		args[0] = v.JobId.String()
		args[1] = v.Job.Name
		args[2] = v.Job.Queue
		args[3] = v.Job.AppName
		args[4] = v.Job.Argument
		args[5] = v.Job.CreatedAt
		if v.Job.IsRecurring {
			args[6] = 1
		} else {
			args[6] = 0
		}
		args[7] = v.Job.StartAt
		if v.Job.Active {
			args[8] = 1
		} else {
			args[8] = 0
		}
		args[9] = v.Job.Recurring
		args[10] = v.Job.Tries
		args[11] = uint8(v.Job.Type)
		args[12] = v.Job.AfterBackgroundJobIds
		_, err = stmt.Exec(args...)
		if err != nil {
			return err
		}
	}

	_, err = tx.Exec(`MERGE INTO "jobs" bj USING(SELECT id,name,queue,app_name,argument,
			created_at,is_recurring,start_at,active,recurring,tries,"type",after_background_job_ids
		FROM temp_jobs) src ON (bj."id" = src.id)
		WHEN NOT MATCHED THEN
			INSERT ("id","name","queue","app_name","argument","created_at","is_recurring",
				"start_at","active","recurring","tries","type","after_background_job_ids")
			VALUES (src.id,src.name,src.queue,src.app_name,src.argument,src.created_at,src.is_recurring,
				src.start_at,src.active,src.recurring,src.tries,src."type",src.after_background_job_ids)`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`DROP TABLE temp_jobs`)
	if err != nil {
		return err
	}

	args = make([]interface{}, 11)

	_, err = tx.Exec(`CREATE GLOBAL TEMPORARY TABLE temp_bg_jobs (id CHAR(36), job_id CHAR(36),
		processed_by CHAR(36), server NVARCHAR2(2000), created_at TIMESTAMP(7),
		status NUMBER(10), job_error NVARCHAR2(2000), started_at TIMESTAMP(7), completed_at TIMESTAMP(7),
		last_activity TIMESTAMP(7), logs NVARCHAR2(2000))`)
	if err != nil {
		return err
	}

	stmt, err = tx.Prepare(`INSERT INTO temp_bg_jobs (id,job_id,processed_by,server,created_at,status,
		job_error,started_at, completed_at,last_activity,logs)
		VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)`)

	for _, v := range bgJobs {
		args[0] = v.Id.String()
		args[1] = v.JobId.String()
		args[2] = v.ProcessedBy.String()
		args[3] = v.Server
		args[4] = v.CreatedAt
		args[5] = uint8(v.Status)
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

	_, err = tx.Exec(`MERGE INTO "background_jobs" bj USING(SELECT id,job_id,processed_by,server,created_at,status,
			job_error,started_at, completed_at,last_activity,logs FROM temp_bg_jobs) src ON (bj."id" = src.id)
		WHEN MATCHED THEN
			UPDATE SET bj."job_id"=src.job_id,bj."processed_by"=src.processed_by,bj."server"=src.server,
				bj."created_at"=src.created_at,bj."status"=src.status,bj."job_error"=src.job_error,
				bj."started_at"=src.started_at,bj."completed_at"=src.completed_at,
				bj."last_activity"=src.last_activity,bj."logs"=src.logs
		WHEN NOT MATCHED THEN
			INSERT ("id","job_id","processed_by","server","created_at","status",
				"job_error","started_at","completed_at","last_activity","logs")
			VALUES (src.id,src.job_id,src.processed_by,src.server,src.created_at,src.status,
				src.job_error,src.started_at,src.completed_at,src.last_activity,src.logs)`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`DROP TABLE temp_bg_jobs`)
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
	stmt, err := sqlStorage.Db.Prepare(`DELETE FROM "background_jobs" WHERE "completed_at" < :1`)
	if err != nil {
		panic(err)
	}
	_, err = stmt.Exec(time.Now().UTC().AddDate(0, 0, -sqlStorage.Config.StorageExpirationInDays))
	if err != nil {
		panic(err)
	}
	_, err = sqlStorage.Db.Exec(`DELETE FROM "jobs" WHERE NOT EXISTS (SELECT "id" FROM "background_jobs" WHERE "job_id" = "jobs"."id")`)
	if err != nil {
		panic(err)
	}
	return nil
}
