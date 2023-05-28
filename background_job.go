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
	"strings"
	"time"

	"github.com/google/uuid"
)

type JobStatus uint8

const (
	Scheduled JobStatus = iota
	Enqueued
	Processing
	Processed
	Canceled
	Interrupted
	Failed
)

func (status JobStatus) String() string {
	switch status {
	case Scheduled:
		return "Scheduled"
	case Enqueued:
		return "Enqueued"
	case Processing:
		return "Processing"
	case Processed:
		return "Processed"
	case Canceled:
		return "Canceled"
	case Interrupted:
		return "Interrupted"
	}
	return "Failed"
}

type JobError struct {
	Message    string
	StackTrace *string
	InnerError *JobError
}

func NewJobError(message string) *JobError {
	jobErr := &JobError{}
	if message != "" {
		txtErr := string(message)
		eol := strings.Index(txtErr, "\n")
		if eol > 0 {
			jobErr.Message = txtErr[:eol]
			details := txtErr[eol+1:]
			jobErr.StackTrace = &details
		} else {
			jobErr.Message = txtErr
		}
		if strings.HasPrefix(jobErr.Message, "Unhandled exception. ") {
			jobErr.Message = jobErr.Message[21:]
		}
		jobErr.StackTrace = &txtErr
	} else {
		jobErr.Message = "Unknow error"
	}
	return jobErr
}

type JobLog struct {
	Time        time.Time
	CpuUsage    float64
	MemoryUsage float64
}

type BackgroundJob struct {
	Id           uuid.UUID
	JobId        uuid.UUID
	Job          *Job `json:"-"`
	ProcessedBy  *uuid.UUID
	Server       string
	CreatedAt    time.Time
	Status       JobStatus
	JobError     *string    `json:"-"`
	Error        *JobError  `json:",omitempty"`
	StartedAt    *time.Time `json:",omitempty"`
	CompletedAt  *time.Time `json:",omitempty"`
	LastActivity *time.Time `json:",omitempty"`
	JobLogs      []*JobLog  `json:"-"`
	Logs         *string    `json:"-"`
}
