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
	"sync"
	"time"

	"github.com/google/uuid"
)

type DistributedLock struct {
	Id           string
	Key          string
	StartedAt    time.Time
	LastActivity time.Time
	isAlive      bool
	mutex        sync.Mutex
	redisStorage *RedisStorage
}

func NewDistributedLock(key string, start bool, redisStorage *RedisStorage) *DistributedLock {
	distLock := &DistributedLock{Key: key, redisStorage: redisStorage}
	if start {
		distLock.Enter()
	}
	return distLock
}

func (distLock *DistributedLock) enter(timeout *time.Duration) bool {
	startedAt := time.Now().UTC()
	waitTime := time.Duration(distLock.redisStorage.Config.LockHeartbeatInterval) * time.Second
	distLock.Id = uuid.NewString() + ":" + distLock.Key
	distLock.StartedAt = startedAt
	distLock.LastActivity = startedAt
	distLock.redisStorage.SaveDistributedLock(distLock)
	for {
		if distLock.redisStorage.IsDistributedLockEntered(distLock.Key, distLock.Id) {
			go distLock.started()
			break
		} else {
			if timeout != nil && timeout.Nanoseconds() > 0 && time.Now().UTC().Sub(startedAt) >= *timeout {
				return false
			}
			distLock.alive()
			time.Sleep(waitTime)
		}
	}
	return true
}

func (distLock *DistributedLock) Enter() {
	distLock.enter(nil)
}

func (distLock *DistributedLock) TryEnter() bool {
	return distLock.TryEnterTill(time.Duration(0))
}

func (distLock *DistributedLock) TryEnterTill(timeout time.Duration) bool {
	return distLock.enter(&timeout)
}

func (distLock *DistributedLock) TryEnterTillMS(millisecondsTimeout int) bool {
	return distLock.TryEnterTill(time.Duration(millisecondsTimeout) * time.Millisecond)
}

func (distLock *DistributedLock) alive() {
	distLock.mutex.Lock()
	if distLock.isAlive {
		distLock.LastActivity = time.Now().UTC()
		distLock.redisStorage.SaveDistributedLock(distLock)
	}
	distLock.mutex.Unlock()
}

func (distLock *DistributedLock) started() {
	waitTime := time.Duration(distLock.redisStorage.Config.LockHeartbeatInterval) * time.Second
	for distLock.isAlive {
		distLock.alive()
		time.Sleep(waitTime)
	}
}

func (distLock *DistributedLock) Exit() {
	distLock.isAlive = false
	distLock.mutex.Lock()
	distLock.redisStorage.DeleteDistributedLock(distLock.Key, distLock.Id)
	distLock.mutex.Unlock()
}
