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
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

type RedisStorage struct {
	options *redis.Options
	client  *redis.Client
	context context.Context
	Config  *Configuration
}

func Connect(config *Configuration) (*RedisStorage, error) {
	if !strings.HasPrefix(config.StorageConfig, "redis://") {
		config.StorageConfig = "redis://" + config.StorageConfig
	}
	options, err := redis.ParseURL(config.StorageConfig)
	if err != nil {
		return nil, err
	}
	redisStorage := &RedisStorage{
		options: options,
		client:  redis.NewClient(options),
		context: context.Background(),
		Config:  config,
	}
	return redisStorage, nil
}

func (redisStorage *RedisStorage) SaveServer(server *Server) {
	data, _ := json.Marshal(server)
	redisStorage.client.Set(redisStorage.context, server.Id.String(), string(data),
		time.Duration(redisStorage.Config.InactiveServerTimeout*int(time.Second)))
	redisStorage.client.SAdd(redisStorage.context, "Servers", server.Id.String())
}

func (redisStorage *RedisStorage) SaveDistributedLock(distLock *DistributedLock) {
	_, err := redisStorage.client.Get(redisStorage.context, "DistLock:"+distLock.Id).Result()
	if err != nil {
		redisStorage.client.RPush(redisStorage.context, "DistLockKey:"+distLock.Key, distLock.Id)
	}
	data, _ := json.Marshal(distLock)
	redisStorage.client.Set(redisStorage.context, "DistLock:"+distLock.Id, string(data), 0)
}

func (redisStorage *RedisStorage) IsDistributedLockEntered(key string, id string) bool {
	for {
		firstId, err := redisStorage.client.LIndex(redisStorage.context, "DistLockKey:"+key, 0).Result()
		if err != nil {
			return false
		}
		data, err := redisStorage.client.Get(redisStorage.context, "DistLock:"+firstId).Result()
		if err != nil {
			redisStorage.client.LRem(redisStorage.context, "DistLockKey:"+key, 0, firstId)
			continue
		}
		var distLock DistributedLock
		json.Unmarshal([]byte(data), &distLock)
		if time.Since(distLock.LastActivity).Seconds() >= float64(redisStorage.Config.InactiveLockTimeout) {
			redisStorage.DeleteDistributedLock(distLock.Key, distLock.Id)
		} else if firstId != id {
			return false
		} else {
			return true
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func (redisStorage *RedisStorage) DistributedLocksCount(key string) int64 {
	count, err := redisStorage.client.LLen(redisStorage.context, "DistLockKey:"+key).Result()
	if err == nil {
		return count
	}
	return 0
}

func (redisStorage *RedisStorage) DeleteDistributedLock(key string, id string) {
	del, _ := redisStorage.client.Del(redisStorage.context, "DistLock:"+id).Result()
	if del == 1 {
		redisStorage.client.LRem(redisStorage.context, "DistLockKey:"+key, 0, id).Result()
	}
}

func (redisStorage *RedisStorage) GetAllDistributedLocks() []*DistributedLock {
	locks := []*DistributedLock{}
	keys, err := redisStorage.client.Keys(redisStorage.context, "DistLock:*").Result()
	if err == nil {
		for _, key := range keys {
			data, err := redisStorage.client.Get(redisStorage.context, key).Result()
			if err != nil {
				continue
			}
			var distLock DistributedLock
			err = json.Unmarshal([]byte(data), &distLock)
			if err != nil {
				continue
			}
			locks = append(locks, &distLock)
		}
	}
	return locks
}

func (redisStorage *RedisStorage) GetServer(server *Server) {
	result, err := redisStorage.client.Get(redisStorage.context, server.Id.String()).Result()
	if err == nil {
		json.Unmarshal([]byte(result), server)
	}
}

func (redisStorage *RedisStorage) Dequeue(queue string) uuid.UUID {
	val, err := redisStorage.client.LPop(redisStorage.context, "Queue:"+queue).Result()
	if err == nil {
		id, err := uuid.Parse(val)
		if err == nil {
			return id
		}
	}
	return uuid.Nil
}

func (redisStorage *RedisStorage) GetJobs(serverId uuid.UUID, queue string) []*Job {
	jobIds, err := redisStorage.client.LRange(redisStorage.context, "QueueSchedule:"+queue, 0, -1).Result()
	var jobs []*Job
	if err == nil {
		jobs = make([]*Job, len(jobIds))
		for i, strJobId := range jobIds {
			jobId, err := uuid.Parse(strJobId)
			if err == nil {
				jobs[i] = redisStorage.GetJob(jobId, false)
			}
		}
	}
	redisStorage.client.SAdd(redisStorage.context, "LatestPulled:"+queue, serverId.String())
	return jobs
}

func (redisStorage *RedisStorage) GetAllJobs(status JobStatus) ([]*BackgroundJob, int) {
	strStatus := strconv.Itoa(int(status))
	bgJobIds, err := redisStorage.client.LRange(redisStorage.context, strStatus, 0, -1).Result()
	i := 0
	var bgJobs []*BackgroundJob
	if err == nil {
		bgJobs = make([]*BackgroundJob, len(bgJobIds))
		for _, strBgJobId := range bgJobIds {
			bgJobId, err := uuid.Parse(strBgJobId)
			if err == nil {
				bgJobs[i] = redisStorage.GetBackgroundJob(bgJobId, true)
				if bgJobs[i] != nil {
					bgJobs[i].JobLogs = redisStorage.GetJobLogs(bgJobId)
					i++
				}
			}
		}
	}
	return bgJobs, i
}

func (redisStorage *RedisStorage) ScheduleChanged(serverId uuid.UUID, queue string) bool {
	result, _ := redisStorage.client.SIsMember(redisStorage.context,
		"LatestPulled:"+queue, serverId.String()).Result()
	return !result
}

func (redisStorage *RedisStorage) GetJob(jobId uuid.UUID, loadLatest bool) *Job {
	jobData, err := redisStorage.client.Get(redisStorage.context, "Job:"+jobId.String()).Result()
	if err == nil {
		var job Job
		err = json.Unmarshal([]byte(jobData), &job)
		if err == nil {
			if loadLatest {
				strBgJobId, err := redisStorage.client.LIndex(redisStorage.context, "S:"+jobId.String(), 0).Result()
				if err == nil {
					bgJobId, err := uuid.Parse(strBgJobId)
					if err == nil {
						bgJob := redisStorage.GetBackgroundJob(bgJobId, false)
						if bgJob != nil {
							bgJob.Job = &job
							job.BackgroundJobs = []*BackgroundJob{bgJob}
						}
					}
				}
				if job.IsRecurring {
					nextTime := job.RecurringPattern.NextTime()
					job.StartAt = &nextTime
				}
			}
			return &job
		}
	}
	return nil
}

func (redisStorage *RedisStorage) GetServerJobs(serverId uuid.UUID, queue string) []*BackgroundJob {
	var bgJobs []*BackgroundJob
	bgJobIds, _ := redisStorage.client.LRange(redisStorage.context, serverId.String()+":"+queue, 0, -1).Result()
	bgJobs = make([]*BackgroundJob, len(bgJobIds))
	for i, strBgJobId := range bgJobIds {
		bgJobId, err := uuid.Parse(strBgJobId)
		if err == nil {
			bgJobs[i] = redisStorage.GetBackgroundJob(bgJobId, true)
		}
	}
	return bgJobs
}

func (redisStorage *RedisStorage) GetBackgroundJob(backgroundJobId uuid.UUID,
	includeDetails bool) *BackgroundJob {
	jobData, err := redisStorage.client.Get(redisStorage.context,
		"BackgroundJob:"+backgroundJobId.String()).Result()
	if err == nil {
		bgJob := &BackgroundJob{}
		if json.Unmarshal([]byte(jobData), bgJob) == nil {
			if includeDetails {
				bgJob.Job = redisStorage.GetJob(bgJob.JobId, false)
			}
			return bgJob
		}
	}
	return nil
}

func (redisStorage *RedisStorage) JobEnqueued(jobId uuid.UUID, queue string) {
	redisStorage.client.LRem(redisStorage.context, "QueueSchedule:"+queue, 0, jobId.String())
	redisStorage.client.LRem(redisStorage.context, "Scheduled", 0, jobId.String())
	redisStorage.client.Del(redisStorage.context, "LatestPulled:"+queue)
}

func (redisStorage *RedisStorage) SaveJob(job *Job, forceUpdate bool) error {
	if job.Name != nil && job.Id == uuid.Nil {
		jobId, err := redisStorage.client.Get(redisStorage.context, "RecurringJob:"+*job.Name).Result()
		if err != nil {
			return err
		}
		job.Id, err = uuid.Parse(jobId)
	}
	if job.Id == uuid.Nil {
		job.Id, _ = uuid.NewUUID()
	}
	oldJob := redisStorage.GetJob(job.Id, forceUpdate)
	if oldJob == nil || forceUpdate {
		data, err := json.Marshal(job)
		if err != nil {
			return err
		}
		redisStorage.client.Set(redisStorage.context, "Job:"+job.Id.String(), string(data), 0)
		if job.Active && (oldJob == nil ||
			((job.StartAt == nil || oldJob.StartAt == nil) && job.StartAt != oldJob.StartAt ||
				!job.StartAt.Equal(*oldJob.StartAt))) {
			if job.IsRecurring || job.StartAt != nil {
				if job.IsRecurring {
					redisStorage.client.LPush(redisStorage.context, "Recurring", job.Id.String())
					redisStorage.client.Set(redisStorage.context, "RecurringJob:"+*job.Name, job.Id.String(), 0)
				} else {
					redisStorage.client.LPush(redisStorage.context, "Scheduled", job.Id.String())
					redisStorage.client.Set(redisStorage.context, "QueueSchedule:"+job.Queue, job.Id.String(), 0)
				}
				redisStorage.client.Del(redisStorage.context, "LatestPulled:"+job.Queue)
			} else if job.AfterBackgroundJobIds != nil {
				redisStorage.client.LPush(redisStorage.context, "Waiting", job.Id.String())
			}
		}
	}
	return nil
}

func (redisStorage *RedisStorage) SaveBackgroundJob(bgJob *BackgroundJob) error {
	oldJob := redisStorage.GetBackgroundJob(bgJob.Id, true)
	data, err := json.Marshal(bgJob)
	if err != nil {
		return err
	}
	if bgJob.Job != nil {
		redisStorage.SaveJob(bgJob.Job, false)
	} else {
		bgJob.Job = redisStorage.GetJob(bgJob.JobId, false)
	}
	redisStorage.client.Set(redisStorage.context, "BackgroundJob:"+bgJob.Id.String(), string(data), 0)
	if oldJob == nil {
		redisStorage.client.LPush(redisStorage.context, "S:"+bgJob.JobId.String(), bgJob.Id.String())
	}
	if oldJob == nil || oldJob.Status != bgJob.Status {
		status := strconv.Itoa(int(bgJob.Status))
		if oldJob != nil {
			oldStatus := strconv.Itoa(int(oldJob.Status))
			redisStorage.client.LRem(redisStorage.context, oldStatus, 0, bgJob.Id.String())
			if bgJob.Job.IsRecurring || (bgJob.Job.StartAt != nil &&
				bgJob.Job.StartAt.After(time.Now().UTC())) {
				redisStorage.client.LRem(redisStorage.context,
					oldStatus+":"+bgJob.JobId.String(), 0, bgJob.Id.String())
			}
			if bgJob.Status == Processed || bgJob.Status == Failed {
				redisStorage.client.HIncrBy(redisStorage.context,
					bgJob.CompletedAt.Format("2006-01-02"), status, 1)
			}
			if oldJob.Status == Enqueued {
				redisStorage.client.LRem(redisStorage.context,
					"Queue:"+oldJob.Job.Queue, 0, oldJob.Id.String())
			}
		}
		redisStorage.client.LPush(redisStorage.context, status, bgJob.Id.String())
		if bgJob.Job.IsRecurring || (bgJob.Job.StartAt != nil &&
			bgJob.Job.StartAt.After(time.Now().UTC())) {
			redisStorage.client.LPush(redisStorage.context, status+":"+bgJob.JobId.String(), bgJob.Id.String())
		}
		if bgJob.Status == Enqueued {
			redisStorage.client.LPush(redisStorage.context,
				"Queue:"+bgJob.Job.Queue, bgJob.Id.String())
		}
		if bgJob.Status == Processing {
			redisStorage.client.LPush(redisStorage.context, bgJob.ProcessedBy.String()+":"+bgJob.Job.Queue, bgJob.Id.String())
		}
		if bgJob.Status == Processed || bgJob.Status == Failed {
			result, err := redisStorage.client.Exists(redisStorage.context, "After:"+bgJob.Id.String()).Result()
			if err != nil {
				return err
			}
			if result > 0 {
				strJobId, err := redisStorage.client.SPop(redisStorage.context, "After:"+bgJob.Id.String()).Result()
				if err != nil {
					return err
				}
				jobId, err := uuid.Parse(strJobId)
				if err != nil {
					return err
				}
				redisStorage.enqueuedAfter(jobId)
			}
		}
		if (bgJob.Status == Processed || bgJob.Status == Failed ||
			bgJob.Status == Interrupted) && bgJob.ProcessedBy != nil {
			redisStorage.client.LRem(redisStorage.context,
				bgJob.ProcessedBy.String()+":"+bgJob.Job.Queue, 0, bgJob.Id.String())
		}
	}
	return nil
}

func (redisStorage *RedisStorage) EnqueueAfter(jobId uuid.UUID, bgJobId uuid.UUID) error {
	strJobId := jobId.String()
	key := "After:" + jobId.String()
	redisStorage.client.SAdd(redisStorage.context, key, strJobId)
	bgJob := redisStorage.GetBackgroundJob(bgJobId, false)
	res, err := redisStorage.client.SRem(redisStorage.context, key, strJobId).Result()
	if err != nil {
		return err
	}
	if (bgJob.Status == Processed || bgJob.Status == Failed) && res > 0 {
		redisStorage.enqueuedAfter(jobId)
	}
	return nil
}

func (redisStorage *RedisStorage) enqueuedAfter(jobId uuid.UUID) error {
	job := redisStorage.GetJob(jobId, false)
	if job != nil {
		if !job.IsRecurring {
			job.Active = false
			redisStorage.JobEnqueued(job.Id, job.Queue)
		}
		newId, _ := uuid.NewUUID()
		bgJob := &BackgroundJob{
			JobId:     job.Id,
			Id:        newId,
			Job:       job,
			CreatedAt: time.Now().UTC(),
			Status:    Enqueued,
		}
		err := redisStorage.SaveBackgroundJob(bgJob)
		if err != nil {
			return err
		}
		redisStorage.client.LRem(redisStorage.context, "Waiting", 0, jobId.String())
	}
	return nil
}

func (redisStorage *RedisStorage) HasRunningJobs(serverId uuid.UUID) bool {
	keys, err := redisStorage.client.Keys(redisStorage.context, serverId.String()+":*").Result()
	if err == nil {
		for _, key := range keys {
			jobs, err := redisStorage.client.LLen(redisStorage.context, key).Result()
			if err == nil && jobs > 0 {
				return true
			}
		}
	}
	return false
}

func (redisStorage *RedisStorage) AddJobLog(BackgroundJobId uuid.UUID, log *JobLog) {
	data, err := json.Marshal(log)
	if err == nil {
		redisStorage.client.RPush(redisStorage.context, "Logs:"+BackgroundJobId.String(), string(data))
	}
}

func (redisStorage *RedisStorage) GetJobLogs(backgroundJobId uuid.UUID) []*JobLog {
	logs, err := redisStorage.client.LRange(redisStorage.context, "Logs:"+backgroundJobId.String(), 0, -1).Result()
	var logsArr []*JobLog
	if err == nil {
		logsArr = make([]*JobLog, len(logs))
		for i, logStr := range logs {
			log := &JobLog{}
			if json.Unmarshal([]byte(logStr), log) == nil {
				logsArr[i] = log
			}
		}
	}
	return logsArr
}

func (redisStorage *RedisStorage) DeleteBackgroundJob(bgJob *BackgroundJob) {
	strStatus := strconv.Itoa(int(bgJob.Status))
	redisStorage.client.LRem(redisStorage.context, strStatus, 0, bgJob.Id.String())
	if bgJob.Job.IsRecurring {
		redisStorage.client.LRem(redisStorage.context, strStatus+":"+bgJob.JobId.String(), 0, bgJob.Id.String())
	}
	redisStorage.client.Del(redisStorage.context, "BackgroundJob:"+bgJob.Id.String())
	redisStorage.client.Del(redisStorage.context, "Logs:"+bgJob.Id.String())
	redisStorage.client.LRem(redisStorage.context, "S:"+bgJob.JobId.String(), 0, bgJob.Id.String())
	length, err := redisStorage.client.LLen(redisStorage.context, "S:"+bgJob.JobId.String()).Result()
	if !bgJob.Job.IsRecurring && (err != nil || length == 0) {
		redisStorage.DeleteJob(bgJob.Job)
	}
}

func (redisStorage *RedisStorage) DeleteJob(job *Job) {
	redisStorage.client.Del(redisStorage.context, "RecurringJob:"+*job.Name)
	redisStorage.client.LRem(redisStorage.context, "Scheduled", 0, job.Id.String())
	redisStorage.client.LRem(redisStorage.context, "Recurring", 0, job.Id.String())
	redisStorage.client.LRem(redisStorage.context, "QueueSchedule:"+job.Queue, 0, job.Id.String())
	if job.Active {
		redisStorage.client.Del(redisStorage.context, "LatestPulled:"+job.Queue)
	}
	redisStorage.client.Del(redisStorage.context, "Job:"+job.Id.String())
	redisStorage.client.Del(redisStorage.context, "S:"+job.Id.String())
}

func (redisStorage *RedisStorage) DeleteBackgroundJobs(bgJobs []*BackgroundJob) {
	for _, bgJob := range bgJobs {
		strStatus := strconv.Itoa(int(bgJob.Status))
		redisStorage.client.Del(redisStorage.context, "BackgroundJob:"+bgJob.Id.String())
		redisStorage.client.LRem(redisStorage.context, strStatus, 0, bgJob.Id.String())
	}
	go func() {
		for _, bgJob := range bgJobs {
			strStatus := strconv.Itoa(int(bgJob.Status))
			if bgJob.Job.IsRecurring {
				redisStorage.client.LRem(redisStorage.context, strStatus+":"+bgJob.JobId.String(), 0, bgJob.Id.String())
			}
			redisStorage.client.Del(redisStorage.context, "Logs:"+bgJob.Id.String())
			redisStorage.client.LRem(redisStorage.context, "S:"+bgJob.JobId.String(), 0, bgJob.Id.String())
			length, err := redisStorage.client.LLen(redisStorage.context, "S:"+bgJob.JobId.String()).Result()
			if !bgJob.Job.IsRecurring && (err != nil || length == 0) {
				redisStorage.client.Unlink(redisStorage.context, "S:"+bgJob.JobId.String())
				redisStorage.client.LRem(redisStorage.context, "Scheduled", 0, bgJob.JobId.String())
				redisStorage.client.LRem(redisStorage.context, "Recurring", 0, bgJob.JobId.String())
				redisStorage.client.LRem(redisStorage.context, "QueueSchedule:"+bgJob.Job.Queue, 0, bgJob.JobId.String())
				if bgJob.Job.Active {
					redisStorage.client.Del(redisStorage.context, "LatestPulled:"+bgJob.Job.Queue)
				}
				redisStorage.client.Del(redisStorage.context, "Job:"+bgJob.JobId.String())
			}
		}
	}()
}

func (redisStorage *RedisStorage) DeleteExpired() error {
	expiryDate := time.Now().UTC().AddDate(0, 0, -redisStorage.Config.StorageExpirationInDays)
	for _, status := range [2]JobStatus{Processed, Failed} {
		key := status.String()
		i, err := redisStorage.client.LLen(redisStorage.context, key).Result()
		if err != nil {
			return err
		}
		i--
		for i >= 0 {
			bgJobIdStr, err := redisStorage.client.LIndex(redisStorage.context, key, i).Result()
			if err != nil {
				break
			}
			bgJobId, err := uuid.Parse(bgJobIdStr)
			if err == nil {
				bgJob := redisStorage.GetBackgroundJob(bgJobId, true)
				if bgJob.CompletedAt.Before(expiryDate) {
					redisStorage.DeleteBackgroundJob(bgJob)
				} else {
					break
				}
			} else {
				break
			}
			i, err = redisStorage.client.LLen(redisStorage.context, key).Result()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
