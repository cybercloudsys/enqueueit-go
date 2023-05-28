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
	"time"

	"github.com/cybercloudsys/recur-go"
	"github.com/google/uuid"
)

type JobType uint8

const (
	Thread JobType = iota
	Microservice
)

type Argument struct {
	Name  string
	Value string
	Type  string
}

type JobArgument struct {
	Assembly      string
	ClassType     string
	MethodName    string
	MetadataToken int
	IsStatic      bool
	Arguments     []*Argument
}

type Job struct {
	Id                    uuid.UUID
	Name                  *string `json:",omitempty"`
	Queue                 string
	AppName               string
	JobArgument           *JobArgument
	Argument              *string `json:",omitempty"`
	CreatedAt             time.Time
	IsRecurring           bool
	StartAt               *time.Time `json:",omitempty"`
	Active                bool
	Recurring             *string                 `json:"-"`
	RecurringPattern      *recur.RecurringPattern `json:",omitempty"`
	Tries                 int
	Type                  JobType
	AfterBackgroundJobIds *string          `json:",omitempty"`
	BackgroundJobs        []*BackgroundJob `json:"-"`
}

func NewJob(appName string, argument interface{}, startAt *time.Time, recurPattern *recur.RecurringPattern, queue string) *Job {
	newId, _ := uuid.NewUUID()
	job := &Job{
		Id:               newId,
		CreatedAt:        time.Now().UTC(),
		Queue:            queue,
		Active:           true,
		AppName:          appName,
		Argument:         serialize(argument),
		StartAt:          startAt,
		IsRecurring:      recurPattern != nil,
		RecurringPattern: recurPattern,
		Type:             Microservice,
	}
	return job
}

func (job *Job) Enqueue(redisStorage *RedisStorage) (uuid.UUID, error) {
	job = redisStorage.GetJob(job.Id, false)
	if job != nil {
		if !job.IsRecurring {
			job.Active = false
			redisStorage.JobEnqueued(job.Id, job.Queue)
		}
		newId, _ := uuid.NewUUID()
		bgJob := &BackgroundJob{
			JobId:     job.Id,
			Id:        newId,
			CreatedAt: time.Now().UTC(),
			Status:    Enqueued,
			Job:       job,
		}
		err := redisStorage.SaveBackgroundJob(bgJob)
		if err != nil {
			return uuid.Nil, err
		}
		return bgJob.Id, nil
	}
	return uuid.Nil, nil
}
