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
	"errors"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/amoghe/distillog"
	"github.com/cybercloudsys/recur-go"
	"github.com/google/uuid"
	"github.com/struCoder/pidusage"
)

func LoadConfiguration(configPath *string) *Configuration {
	config := &Configuration{
		StorageConfig:           "localhost",
		LongTermStorageConfig:   "",
		ConnectionRetries:       10,
		ConnectionRetryInterval: 3,
		JobHeartbeatInterval:    1,
		InactiveJobTimeout:      15,
		ServerHeartbeatInterval: 1,
		InactiveServerTimeout:   15,
		LockHeartbeatInterval:   1,
		InactiveLockTimeout:     15,
		StorageExpirationInDays: 30,
		StorageSyncInterval:     1,
		StorageSyncBatchSize:    10000,
		CleanStorageInterval:    60,
	}

	if configPath == nil {
		exePath, err := os.Executable()
		if err != nil {
			return config
		}
		filePath := path.Join(path.Dir(exePath), "enqueueit.json")
		configPath = &filePath
	}

	content, err := os.ReadFile(*configPath)

	if err == nil {
		json.Unmarshal([]byte(content), config)
	}
	return config
}

func StartServer(sqlStorage SqlStorage, config *Configuration) error {
	if config == nil {
		config = LoadConfiguration(nil)
	}
	redisStorage, err := Connect(config)
	if err != nil {
		return err
	}
	if sqlStorage != nil {
		err = sqlStorage.Connect(config)
		if err != nil {
			return err
		}
	}
	if config.Applications == nil || len(config.Applications) == 0 {
		return errors.New("the configuration must having at least one application added in applications section")
	}
	if config.Servers == nil || len(config.Servers) == 0 {
		config.Servers = []*Server{{
			Queues:       []*Queue{},
			Status:       0,
			StartedAt:    time.Time{},
			LastActivity: time.Time{},
			WorkersCount: 50,
			procWorkers:  0,
		}}
	}
	var thisServer *Server
	hostname, _ := os.Hostname()
	lHostname := strings.ToLower(hostname)
	for _, server := range config.Servers {
		if strings.TrimSpace(server.Hostname) == "" ||
			strings.ToLower(server.Hostname) == lHostname {
			if server.WorkersCount < 1 {
				server.WorkersCount = 50
			}
			thisServer = server
			server.queueWorkers = map[string]int{}
			server.queueMutex = sync.Mutex{}
		}
	}
	if thisServer != nil {
		if thisServer.Queues == nil || len(thisServer.Queues) == 0 {
			thisServer.Queues = []*Queue{{Name: "services", WorkersCount: 20}}
		}
		thisServer.Id, _ = uuid.NewUUID()
		thisServer.Hostname = hostname
		go thisServer.waitToStop(redisStorage)
		thisServer.StartService(redisStorage, sqlStorage)
	}
	return nil
}

func Enqueue(appName string, argument interface{}, queue string, redisStorage *RedisStorage) (uuid.UUID, error) {
	job := NewJob(appName, argument, nil, nil, queue)
	err := redisStorage.SaveJob(job, true)
	if err != nil {
		return uuid.Nil, err
	}
	return job.Enqueue(redisStorage)
}

func Schedule(appName string, argument interface{}, startAt time.Time, queue string, redisStorage *RedisStorage) (*Job, error) {
	job := NewJob(appName, argument, &startAt, nil, queue)
	err := redisStorage.SaveJob(job, true)
	if err != nil {
		return nil, err
	}
	return job, nil
}

func Subscribe(appName string, argument interface{}, recurPattern recur.RecurringPattern,
	queue string, redisStorage *RedisStorage) (*Job, error) {
	job := NewJob(appName, argument, nil, &recurPattern, queue)
	err := redisStorage.SaveJob(job, true)
	if err != nil {
		return nil, err
	}
	return job, nil
}

func EnqueueAfter(appName string, argument interface{}, bgJobId uuid.UUID,
	queue string, redisStorage *RedisStorage) (*Job, error) {
	job := NewJob(appName, argument, nil, nil, queue)
	err := redisStorage.SaveJob(job, true)
	if err != nil {
		return nil, err
	}
	err = redisStorage.EnqueueAfter(job.Id, bgJobId)
	return job, err
}

func trackJobProcess(bgJobId uuid.UUID, cmd *exec.Cmd, queue string, server *Server, redisStorage *RedisStorage) {
	var data []byte
	var err error
	go func() {
		data, err = cmd.CombinedOutput()
		if err == nil {
			err = errors.New("Completed")
		}
	}()
	var currentLog *JobLog
	lastLog := &JobLog{
		Time: time.Now().UTC().Add(-time.Second),
	}
	lastAdded := false
	for err == nil {
		now := time.Now().UTC()
		milli := now.Nanosecond() / int(time.Millisecond)
		for milli < 500 || now.Second() == lastLog.Time.Second() {
			time.Sleep(time.Millisecond * 100)
			now = time.Now().UTC()
			milli = now.Nanosecond() / int(time.Millisecond)
		}
		if cmd.Process != nil {
			sysInfo, err := pidusage.GetStat(cmd.Process.Pid)
			if err == nil {
				currentLog = &JobLog{
					Time:        now,
					CpuUsage:    sysInfo.CPU,
					MemoryUsage: sysInfo.Memory / 1024 / 1024,
				}
				lastAdded = lastLog.CpuUsage != currentLog.CpuUsage ||
					lastLog.MemoryUsage != currentLog.MemoryUsage
				if lastAdded {
					redisStorage.AddJobLog(bgJobId, currentLog)
				}
				lastLog = currentLog
				distLock := NewDistributedLock(bgJobId.String(), true, redisStorage)
				bgJob := redisStorage.GetBackgroundJob(bgJobId, false)
				if bgJob == nil || bgJob.Status == Processed || bgJob.Status == Failed || bgJob.Status == Interrupted {
					distLock.Exit()
					cmd.Process.Kill()
					server.workerDisposed(queue)
					log.Debugf("Microservice %v is %v", bgJobId, bgJob.Status)
					return
				}
				bgJob.CompletedAt = &now
				redisStorage.SaveBackgroundJob(bgJob)
				distLock.Exit()
			}
		}
		time.Sleep((time.Duration(redisStorage.Config.JobHeartbeatInterval) * time.Second) - (time.Millisecond * 105))
	}
	if currentLog == nil && cmd.ProcessState != nil {
		currentLog = &JobLog{
			Time:        time.Now().UTC(),
			CpuUsage:    100 * (cmd.ProcessState.SystemTime().Seconds() + cmd.ProcessState.UserTime().Seconds()) / 1,
			MemoryUsage: float64(cmd.ProcessState.SysUsage().(*syscall.Rusage).Maxrss) / 1024,
		}
		lastLog = currentLog
	}
	if !lastAdded && lastLog.MemoryUsage > 0 {
		redisStorage.AddJobLog(bgJobId, currentLog)
	}
	distLock := NewDistributedLock(bgJobId.String(), true, redisStorage)
	bgJob := redisStorage.GetBackgroundJob(bgJobId, true)
	if bgJob != nil && bgJob.Status == Processing {
		now := time.Now().UTC()
		bgJob.CompletedAt = &now
		if cmd.ProcessState != nil && cmd.ProcessState.Success() {
			bgJob.Status = Processed
		} else {
			bgJob.Status = Failed
			if data != nil {
				bgJob.Error = NewJobError(string(data))
			} else if err != nil {
				bgJob.Error = NewJobError(err.Error())
			} else {
				bgJob.Error = &JobError{Message: "Unknow error"}
			}
			for _, q := range server.Queues {
				if q.Name == bgJob.Job.Queue {
					if q.Retries > bgJob.Job.Tries {
						bgJob.Job.Active = true
						nextTime := time.Now().UTC().Add(time.Second * time.Duration(q.RetryInterval))
						bgJob.Job.StartAt = &nextTime
					}
					break
				}
			}
		}
		redisStorage.SaveBackgroundJob(bgJob)
		if bgJob.Status == Failed {
			log.Errorf("Microservice %v is Failed, reason: %v", bgJobId, bgJob.Error.Message)
		} else {
			log.Debugf("Microservice %v is %v", bgJobId, bgJob.Status)
		}

	}
	server.workerDisposed(queue)
	distLock.Exit()
}

func serialize(argument interface{}) *string {
	if argument != nil {
		if str, ok := argument.(string); ok {
			return &str
		} else {
			data, err := json.Marshal(argument)
			if err == nil {
				jsonStr := string(data)
				return &jsonStr
			}
		}
	}
	return nil
}
