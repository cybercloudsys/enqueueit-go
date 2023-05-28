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

package sqlserver

import (
	"time"

	"database/sql"

	"github.com/cybercloudsys/enqueueit-go"
	mssql "github.com/denisenkom/go-mssqldb"
)

type Storage struct {
	Db     *sql.DB
	Config *enqueueit.Configuration
}

func (sqlStorage *Storage) Connect(config *enqueueit.Configuration) error {
	db, err := sql.Open("sqlserver", config.LongTermStorageConfig)
	if err != nil {
		return err
	}
	sqlStorage.Db = db
	sqlStorage.Config = config
	return nil
}

func (sqlStorage *Storage) SaveBackgroundJobs(bgJobs []*enqueueit.BackgroundJob) error {
	trans, err := sqlStorage.Db.Begin()
	if err != nil {
		return err
	}

	_, err = trans.Exec(`CREATE TABLE #TempJobs ([id] char(36) NOT NULL, [name] nvarchar(max) NULL,
		[queue] nvarchar(max) NULL, [app_name] nvarchar(max) NULL, [argument] nvarchar(max) NULL, [created_at] datetime2 NOT NULL,
		[is_recurring] bit NOT NULL, [start_at] datetime2 NULL, [active] bit NOT NULL, [recurring] nvarchar(max) NULL, [tries] int NOT NULL,
		[type] int NOT NULL, [after_background_job_ids] nvarchar(max) NULL);
		CREATE TABLE #TempBgJobs ([id] char(36) NOT NULL, [job_id] char(36) NOT NULL,
		[processed_by] char(36) NULL, [server] nvarchar(max) NULL, [created_at] datetime2 NOT NULL,
		[status] int NOT NULL, [job_error] nvarchar(max) NULL, [started_at] datetime2 NULL, [completed_at] datetime2 NULL,
		[last_activity] datetime2 NULL, [logs] nvarchar(max) NULL);`)

	if err != nil {
		return err
	}

	stmtJobs, err := trans.Prepare(mssql.CopyIn("#TempJobs", mssql.BulkOptions{}, "id", "name", "queue",
		"app_name", "argument", "created_at", "is_recurring", "start_at", "active", "recurring",
		"tries", "type", "after_background_job_ids"))

	for _, v := range bgJobs {
		_, err = stmtJobs.Exec(v.Job.Id, v.Job.Name, v.Job.Queue, v.Job.AppName, v.Job.Argument,
			v.Job.CreatedAt, v.Job.IsRecurring, v.Job.StartAt, v.Job.Active, v.Job.Recurring,
			v.Job.Tries, v.Job.Type, v.Job.AfterBackgroundJobIds)
		if err != nil {
			return err
		}
	}
	stmtJobs.Exec()
	err = stmtJobs.Close()
	if err != nil {
		return err
	}

	stmtBgJobs, err := trans.Prepare(mssql.CopyIn("#TempBgJobs", mssql.BulkOptions{}, "id", "job_id",
		"processed_by", "server", "created_at", "status", "job_error", "started_at", "completed_at",
		"last_activity", "logs"))

	for _, v := range bgJobs {
		_, err = stmtBgJobs.Exec(v.Id, v.JobId, v.ProcessedBy, v.Server, v.CreatedAt, v.Status,
			v.JobError, v.StartedAt, v.CompletedAt, v.LastActivity, v.Logs)
		if err != nil {
			return err
		}
	}
	stmtBgJobs.Exec()
	err = stmtBgJobs.Close()
	if err != nil {
		return err
	}

	_, err = trans.Exec(`MERGE INTO EnqueueIt.jobs jobs USING (SELECT id,name,queue,app_name,argument,created_at,
		is_recurring,start_at,active,recurring,tries,type,after_background_job_ids FROM #TempJobs) AS dt ON dt.id = jobs.id
		WHEN NOT MATCHED BY TARGET THEN INSERT (id,name,queue,app_name,argument,created_at,is_recurring,start_at,
			active,recurring,tries,type,after_background_job_ids)
		VALUES (id,name,queue,app_name,argument,created_at,is_recurring,start_at,active,recurring,tries,type,after_background_job_ids);
		DROP TABLE #TempJobs;
		MERGE INTO EnqueueIt.background_jobs bgjobs USING (SELECT id,job_id,processed_by,server,
		created_at, status,job_error,started_at, completed_at,last_activity,logs FROM #TempBgJobs) AS dt ON dt.id = bgjobs.id
		WHEN MATCHED THEN UPDATE SET job_id=dt.job_id,processed_by=dt.processed_by,server=dt.server,status=dt.status,
			job_error=dt.job_error,started_at=dt.started_at,completed_at=dt.completed_at,last_activity=dt.last_activity,logs=dt.logs
		WHEN NOT MATCHED BY TARGET THEN INSERT (id,job_id,processed_by,server,created_at,status,job_error,started_at,completed_at,last_activity,logs)
		VALUES (id,job_id,processed_by,server,created_at,status,job_error,started_at,completed_at,last_activity,logs);
		DROP TABLE #TempBgJobs;`)

	if err != nil {
		return err
	}

	err = trans.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (sqlStorage *Storage) DeleteExpired() error {
	stmt, err := sqlStorage.Db.Prepare(`DELETE FROM EnqueueIt.Background_Jobs WHERE completed_at < @date`)
	if err != nil {
		return err
	}
	_, err = stmt.Exec(sql.Named("date", time.Now().UTC().AddDate(0, 0, -sqlStorage.Config.StorageExpirationInDays)))
	if err != nil {
		return err
	}
	_, err = sqlStorage.Db.Exec(`DELETE FROM EnqueueIt.Jobs WHERE NOT EXISTS(SELECT id FROM EnqueueIt.Background_Jobs WHERE job_id = EnqueueIt.Jobs.id)`)
	if err != nil {
		return err
	}
	return nil
}
