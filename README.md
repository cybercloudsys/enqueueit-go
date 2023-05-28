# Enqueue It
Easy and scalable solution for managing and executing background tasks and microservices seamlessly in .NET applications. It allows you to schedule, queue, and process your jobs and microservices efficiently.

Designed to support distributed systems, enabling you to scale your background processes and microservices across multiple servers. With advanced features like performance monitoring, exception logging, and integration with various storage types, providing complete control and visibility over your workflow.

Provides a user-friendly web dashboard that allows you to monitor and manage your jobs and microservices from a centralized location. You can easily check the status of your tasks, troubleshoot issues, and optimize performance.

## Benefits and Features
- Schedule and queue microservices
- Run multiple servers for increased performance and reliability
- Monitor CPU and memory usage of microservices
- Log exceptions to help find bugs and memory leaks
- Connect to multiple storage types for optimal performance:
  - Main storage (Redis) for active jobs and services
  - Long-term storage (SQL databases such as SQL Server, PostgreSQL, MySQL, and more) for completed jobs and job history

## Installation

You can install the EnqueueIt Go package by running the following command:

```bash
go get github.com/cybercloudsys/enqueueit-go
```

## Usage

To use EnqueueIt go package you may need to create a configuration file named enqueueIt.json in your project and specify the connection strings and settings for your storage servers and queues. Here is an example of a configuration file:
  ```
  {
    "StorageConfig": "localhost:6379",
    "StorageType": "Redis",
    "LongTermStorageConfig": "Server=localhost;Database=JobsDb;User ID=sa;Password=P@ssw0rd;",
    "LongTermStorageType": "SqlServer",
    "Servers": [
      {
        "Queues": [
          {
            "Name": "jobs",
            "WorkersCount": 50,
            "Retries": 0,
            "RetryDelay": 5
          },
          {
            "Name": "services",
            "WorkersCount": 50,
            "Retries": 0,
            "RetryDelay": 5
          }
        ]
      }
    ]
  }
  ```

The package will automaticly load the config file from the working directory, but in some cases you may need to load the config by it is full path

```
configPath := "enqueueit.json" // full path goes here
enqueueit.LoadConfiguration(&configPath)
```

### Start EnqueueIt Server
To start EnqueueIt server, you can use `enqueueit.Start` method to start new server instance that will connect to Redis and start processing queued microservices.
```
	err := enqueueit.StartServer(nil, nil)
	if err != nil {
		log.Panic(err)
	}
```

### Reading Microservice Argument
To read microservice argument, you can import "github.com/cybercloudsys/enqueueit-go/microservice" and use `GetJobArgument` method to get the sent argument as string, if the argument was sent as an instance of the struct or object from .NET app then the value will be as json string that can be deserialized to instance of a struct.

```
import (
  "encoding/json"

  "github.com/cybercloudsys/enqueueit-go/microservice"
)
```

```
// read the microservice argument.
arg, err := microservice.GetJobArgument()
if err != nil {
  panic(err)
}
// deserialize json string to instance of a struct
data := &TestData{}
json.Unmarshal([]byte(*arg), data)
```

### Running Microservices
To run a microservice, you can use `Enqueue` method and pass the name of the microservice and a value that represents the input for the microservice. The value will be passed as a command-line argument to the microservice executable file. For example, to run a microservice named microservice1 with an instance of struct that has a string field called Message, you can write:
  ```
  // connect to redis storage.
	redis, err := enqueueit.Connect(enqueueit.LoadConfiguration(nil))
	if err != nil {
		panic(err)
	}
	// enqueue a microservice
	_, err := enqueueit.Enqueue("microservice1", &TestData{ Message: "Hello World" }, "services", redis);
	if err != nil {
		panic(err)
	}
  ```
  This will add the microservice to the services queue and it will be executed as soon as possible by the EnqueueIt server.

### Scheduling Microservices
EnqueueIt allows you to schedule microservices to run at a specific time or after another job has completed. There are three types of scheduled microservices you can create with EnqueueIt:

- One-time microservice: This is a microservice that will run only once at a given time. You can use the `Schedule` method and pass the name of the microservice, the input object and the time as parameters. For example, to run a microservice after 5 minutes, named microservice1 and pass an instance of struct that has a field called Message, you can write:
  ```
  enqueueit.Schedule("microservice1", &TestData{ Message: "Hello World" }, time.Now().Add(time.Minute * 5), "services", redis)
  ```

- Recurring microservice: This is a microservice that will run repeatedly according to a specified frequency. You can use the `Subscribe` method and pass the name of the microservice, the input object and the recurring pattern as parameters. The recurring pattern is an instance of the `RecurringPattern` struct from [Recur](https://github.com/cybercloudsys/recur-go) package that defines how often the microservice should run. For example, to run a microservice named microservice1 with an object that has a property called Message every day at 06:00 AM, you can write:
  ```
  // you need to import recur to use RecurringPattren
  import "github.com/cybercloudsys/recur-go"
  ```
  ```
  enqueueit.Subscribe("microservice1", new { Message = "Run this later" }, recur.Daily(6))
  ```

- Microservice dependent on another job: This is a microservice that will run only after another job has finished successfully. You can use the `EnqueueAfter` method and pass the name of the microservice, the input argument and the ID of the job that need to be finished first as parameters. For example, to run two microservices in sequence, you can write:
  ```
  //run the first microservice immediately
  jobId, err = enqueueit.Enqueue("microservice1", &TestData{ Message: "Hello World" }, "services", redis);
  if err != nil {
    panic(err)
  }
  
  //this is a microservice to be run after the previous microservice is being completed
  enqueueit.EnqueueAfter("microservice2", new { Message = "Run this after the first job!" }, jobId);
  ```

## Links

- [Homepage](https://www.enqueueit.com)
- [Examples](https://github.com/cybercloudsys/enqueueit/tree/master/Examples)
- [Go Package](https://pkg.go.dev/github.com/cybercloudsys/enqueueit-go)
- [NuGet Packages](https://www.nuget.org/profiles/CyberCloudSystems)

## License

EnqueueIt\
Copyright © 2023 [Cyber Cloud Systems LLC](https://www.cybercloudsys.com)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.

Any user of this program who modifies its source code and distributes
the modified version must make the source code available to all
recipients of the software, under the terms of the license.

If you have any questions about this agreement, You can contact us
through this email info@cybercloudsys.com