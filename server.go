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

package enqueueit

import (
	"encoding/json"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

	log "github.com/amoghe/distillog"
)

type ServerStatus uint8

const (
	Running ServerStatus = iota
	Stopped
)

type StorageType uint8

const (
	Redis StorageType = iota
)

type LongTermStorageType uint8

const (
	SqlServer LongTermStorageType = iota
	PostgresSQL
	MySql
	Oracle
)

type PassArgumentType uint8

const (
	JobId PassArgumentType = iota
	Base64
)

type Server struct {
	Id           uuid.UUID
	Hostname     string
	Queues       []*Queue
	Status       ServerStatus
	StartedAt    time.Time
	LastActivity time.Time
	HasDataSync  bool
	WorkersCount int
	procWorkers  int
	queueWorkers map[string]int
	queueMutex   sync.Mutex
	syncThreads  int
}

type Application struct {
	Name          string
	BaseDirectory string
	LauncherApp   *string
}

type Configuration struct {
	StorageConfig           string
	LongTermStorageConfig   string
	Applications            []*Application
	Servers                 []*Server
	OffDays                 []time.Weekday
	StorageType             StorageType
	LongTermStorageType     LongTermStorageType
	EnableStopServers       bool
	EnableDeleteAll         bool
	ConnectionRetries       int
	ConnectionRetryInterval int
	JobHeartbeatInterval    int
	InactiveJobTimeout      int
	ServerHeartbeatInterval int
	InactiveServerTimeout   int
	LockHeartbeatInterval   int
	InactiveLockTimeout     int
	StorageExpirationInDays int
	StorageSyncInterval     int
	StorageSyncBatchSize    int
	CleanStorageInterval    int
}

func (server *Server) StartService(redisStorage *RedisStorage, sqlStorage SqlStorage) {
	server.Status = Running
	server.StartedAt = time.Now().UTC()
	server.LastActivity = server.StartedAt
	server.HasDataSync = sqlStorage != nil
	redisStorage.SaveServer(server)
	for _, queue := range server.Queues {
		go server.dequeueJobs(queue, redisStorage)
		go server.enqueueJobs(queue, redisStorage)
	}
	log.Infoln("Enqueue It server started.")
	go server.cleanStorage(redisStorage)
	if sqlStorage != nil {
		go server.syncJobs(redisStorage, sqlStorage)
		go server.deleteExpiredJobs(sqlStorage, redisStorage)
	}
	for server.Status == Running {
		redisStorage.GetServer(server)
		server.LastActivity = time.Now().UTC()
		redisStorage.SaveServer(server)
		time.Sleep(time.Duration(redisStorage.Config.ServerHeartbeatInterval) * time.Second)
	}
	log.Infoln("EnqueueIt server stopped.")
}

func (server *Server) waitToStop(redisStorage *RedisStorage) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	redisStorage.GetServer(server)
	log.Infoln("Stopping EnqueueIt server...")
	for _, queue := range server.Queues {
		for _, job := range redisStorage.GetServerJobs(server.Id, queue.Name) {
			if job != nil && job.Status == Processing {
				job.Status = Interrupted
				now := time.Now().UTC()
				job.CompletedAt = &now
				redisStorage.SaveBackgroundJob(job)
			}
		}
	}
	if server.Status == Running {
		server.Status = Stopped
		redisStorage.SaveServer(server)
	}
	time.Sleep(time.Duration(redisStorage.Config.ServerHeartbeatInterval) + 2*time.Second)
	os.Exit(0)
}

func (server *Server) dequeueJobs(queue *Queue, redisStorage *RedisStorage) {
	interval := time.Microsecond
	server.queueWorkers[queue.Name] = 0
	for server.Status == Running {
		server.queueMutex.Lock()
		if server.procWorkers < server.WorkersCount && server.queueWorkers[queue.Name] < queue.WorkersCount {
			server.queueMutex.Unlock()
			jobId := redisStorage.Dequeue(queue.Name)
			if jobId != uuid.Nil {
				server.workerStarted(queue.Name)
				queue.StartJob(jobId, server, redisStorage)
				interval = time.Microsecond
			} else {
				time.Sleep(interval)
				if interval < time.Second {
					interval *= 10
				}
			}
		} else {
			server.queueMutex.Unlock()
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (server *Server) enqueueJobs(queue *Queue, redisStorage *RedisStorage) {
	lastCheck := time.Now().UTC()
	lastCheck = lastCheck.Truncate(time.Second)
	var jobs []*Job
	for server.Status == Running {
		current := time.Now().UTC().Truncate(time.Second)
		if current.Second() != lastCheck.Second() {
			if redisStorage.ScheduleChanged(server.Id, queue.Name) {
				jobs = redisStorage.GetJobs(server.Id, queue.Name)
			}
			for _, job := range jobs {
				var jobStartAt *time.Time
				if job.Active && job.StartAt != nil {
					newTime := job.StartAt.Truncate(time.Second)
					jobStartAt = &newTime
				}
				if (jobStartAt != nil && (jobStartAt.Before(current) || jobStartAt.Equal(current))) ||
					(job.IsRecurring && job.RecurringPattern.IsMatching(current)) {
					job.Enqueue(redisStorage)
				}
			}
			lastCheck = current
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (server *Server) cleanStorage(redisStorage *RedisStorage) {
	waitTime := time.Duration(redisStorage.Config.CleanStorageInterval) * time.Second
	for server.Status == Running {
		distLock := NewDistributedLock("CleanStorage", false, redisStorage)
		if distLock.TryEnter() {
			server.deleteInactiveLocks(redisStorage)
			server.stopInactiveJobs(redisStorage)
			distLock.Exit()
		}
		time.Sleep(waitTime)
	}
}

func (server *Server) stopInactiveJobs(redisStorage *RedisStorage) {
	bgJobs, _ := redisStorage.GetAllJobs(Processing)
	for _, bgJob := range bgJobs {
		if (bgJob.LastActivity != nil && time.Now().UTC().Sub(*bgJob.LastActivity).
			Seconds() > float64(redisStorage.Config.InactiveJobTimeout)) ||
			(bgJob.StartedAt != nil && time.Now().UTC().Sub(*bgJob.StartedAt).
				Seconds() > float64(redisStorage.Config.InactiveJobTimeout)) {
			bgJob.Status = Interrupted
			redisStorage.SaveBackgroundJob(bgJob)
		}
	}
}

func (server *Server) deleteInactiveLocks(redisStorage *RedisStorage) {
	for _, distLock := range redisStorage.GetAllDistributedLocks() {
		if time.Now().UTC().Sub(distLock.LastActivity).
			Seconds() > float64(redisStorage.Config.InactiveLockTimeout) {
			redisStorage.DeleteDistributedLock(distLock.Key, distLock.Id)
		}
	}
}

func (server *Server) deleteExpiredJobs(sqlStorage SqlStorage, redisStorage *RedisStorage) {
	waitTime := time.Second * time.Duration(redisStorage.Config.CleanStorageInterval)
	for server.Status == Running {
		distLock := NewDistributedLock("DeleteExpiredJobs", false, redisStorage)
		if distLock.TryEnter() {
			if sqlStorage != nil {
				sqlStorage.DeleteExpired()
			} else {
				redisStorage.DeleteExpired()
			}
			distLock.Exit()
		}
		time.Sleep(waitTime)
	}
}

func (server *Server) syncJobs(redisStorage *RedisStorage, sqlStorage SqlStorage) {
	for server.Status == Running {
		distLock := NewDistributedLock("SyncJobs", true, redisStorage)
		processed, processedCount := redisStorage.GetAllJobs(Processed)
		failed, failedCount := redisStorage.GetAllJobs(Failed)
		jobsCount := processedCount + failedCount
		allJobs := make([]*BackgroundJob, jobsCount)
		batchSize := redisStorage.Config.StorageSyncBatchSize
		copy(allJobs, processed[:processedCount])
		copy(allJobs[processedCount:], failed[:failedCount])
		if jobsCount > batchSize {
			for i := 0; i < jobsCount; i += batchSize {
				end := i + batchSize
				if end > jobsCount {
					end = jobsCount
				}
				err := server.moveJobs(allJobs[i:end], redisStorage, sqlStorage)
				if err != nil {
					log.Errorln(err)
				}
			}
		} else if jobsCount > 0 {
			err := server.moveJobs(allJobs, redisStorage, sqlStorage)
			if err != nil {
				log.Errorln(err)
			}
		}
		distLock.Exit()
		time.Sleep(time.Duration(redisStorage.Config.StorageSyncInterval) * time.Second)
	}
}

func (server *Server) moveJobs(jobsBatch []*BackgroundJob, redisStorage *RedisStorage, sqlStorage SqlStorage) error {
	batchLen := len(jobsBatch)
	log.Debugf("Start syncing a batch of %d\n", batchLen)
	for _, bgJob := range jobsBatch {
		if bgJob != nil {
			if bgJob.Error != nil {
				data, err := json.Marshal(bgJob.Error)
				if err != nil {
					return err
				}
				strData := string(data)
				bgJob.JobError = &strData
			}
			if len(bgJob.JobLogs) > 0 {
				data, err := json.Marshal(bgJob.JobLogs)
				if err != nil {
					return err
				}
				strData := string(data)
				bgJob.Logs = &strData
			}
			if bgJob.Job.JobArgument != nil {
				arg, err := json.Marshal(bgJob.Job.JobArgument)
				if err != nil {
					return err
				}
				str := string(arg)
				bgJob.Job.Argument = &str
			}
			if bgJob.Job.RecurringPattern != nil {
				data, err := json.Marshal(bgJob.Job.RecurringPattern)
				if err != nil {
					return err
				}
				strData := string(data)
				bgJob.Job.Recurring = &strData
			}
		}
	}
	err := sqlStorage.SaveBackgroundJobs(jobsBatch)
	log.Debugf("%d jobs saved in sql db.\n", batchLen)
	if err != nil {
		return err
	}
	redisStorage.DeleteBackgroundJobs(jobsBatch)
	log.Debugf("The batch of %d is synced.\n", batchLen)
	return nil
}

func (server *Server) workerStarted(queue string) {
	server.queueMutex.Lock()
	server.procWorkers++
	server.queueWorkers[queue]++
	server.queueMutex.Unlock()
}

func (server *Server) workerDisposed(queue string) {
	server.queueMutex.Lock()
	server.procWorkers--
	server.queueWorkers[queue]--
	server.queueMutex.Unlock()
}
