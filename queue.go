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
	"encoding/base64"
	"encoding/json"
	"os/exec"
	"path"
	"time"

	log "github.com/amoghe/distillog"
	"github.com/google/uuid"
)

type Queue struct {
	Name          string
	WorkersCount  int
	Retries       int
	RetryInterval int
}

func (queue *Queue) StartJob(bgJobId uuid.UUID, server *Server, redisStorage *RedisStorage) {
	distLock := NewDistributedLock(bgJobId.String(), true, redisStorage)
	bgJob := redisStorage.GetBackgroundJob(bgJobId, true)
	if bgJob != nil {
		bgJob.Job.Tries++
		bgJob.ProcessedBy = &(server.Id)
		bgJob.Server = server.Hostname
		now := time.Now().UTC()
		bgJob.StartedAt = &now
		var app *Application
		for _, ap := range redisStorage.Config.Applications {
			if ap.Name == bgJob.Job.AppName {
				app = ap
				break
			}
		}
		if app == nil {
			bgJob.Status = Failed
			bgJob.Error = &JobError{Message: "The application " + bgJob.Job.AppName + " is not configured to be excuted by this server"}
			now := time.Now().UTC()
			bgJob.CompletedAt = &now
			redisStorage.SaveBackgroundJob(bgJob)
			server.workerDisposed(queue.Name)
			log.Errorf("Microservice %v failed to start, reason: %v\n", bgJob.Error.Message, bgJobId)
			return
		}
		bgJob.Status = Processing
		redisStorage.SaveBackgroundJob(bgJob)
		distLock.Exit()
		log.Debugf("Microservice %v is start processing...\n", bgJobId)
		for _, app := range redisStorage.Config.Applications {
			if app.Name == bgJob.Job.AppName {
				path := path.Join(app.BaseDirectory, app.Name)
				args := []string{}
				if app.LauncherApp != nil {
					args = append(args, path)
					path = *app.LauncherApp
				}
				arg := ""
				if bgJob.Job.JobArgument != nil {
					ag, err := json.Marshal(bgJob.Job.JobArgument)
					if err == nil {
						arg = base64.StdEncoding.EncodeToString(ag)
					}
				} else if bgJob.Job.Argument != nil {
					arg = base64.StdEncoding.EncodeToString([]byte(*bgJob.Job.Argument))
				}
				args = append(args, "EnqueueIt.Base64:"+arg)
				cmd := exec.Command(path, args...)
				go trackJobProcess(bgJobId, cmd, queue.Name, server, redisStorage)
				break
			}
		}
	}
}
