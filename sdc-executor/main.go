/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"flag"
	"fmt"
	"strings"
	osexec "os/exec"

	exec "github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
)

type exampleExecutor struct {
	tasksLaunched int
}

func newExampleExecutor() *exampleExecutor {
	return &exampleExecutor{tasksLaunched: 0}
}

func (exec *exampleExecutor) Registered(driver exec.ExecutorDriver, execInfo *mesos.ExecutorInfo, fwinfo *mesos.FrameworkInfo, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Registered Executor on slave ", slaveInfo.GetHostname())
}

func (exec *exampleExecutor) Reregistered(driver exec.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Re-registered Executor on slave ", slaveInfo.GetHostname())
}

func (exec *exampleExecutor) Disconnected(exec.ExecutorDriver) {
	fmt.Println("Executor disconnected.")
}

func (exec *exampleExecutor) LaunchTask(driver exec.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	// *
	// Describes a task. Passed from the scheduler all the way to an
	// executor (see SchedulerDriver::launchTasks and
	// Executor::launchTask). Either ExecutorInfo or CommandInfo should be set.
	// A different executor can be used to launch this task, and subsequent tasks
	// meant for the same executor can reuse the same ExecutorInfo struct.
	// type TaskInfo struct {
	//
	// TaskInfoではExecutor か Commandを設定しないといけない。
	// example実装だとExecutorが設定されているがValueがうまく渡っていないように見える
	// Executorの中のCommandInfoに入っていた
	//

	fmt.Println("<------------------ Executor start ------------------>") 
        // https://github.com/mesos/mesos-go/blob/master/mesosproto/mesos.pb.go
        // type CommandInfoあたり
	// fmt.Println("Launching task", taskInfo.GetName(), "with command", taskInfo.Command.GetUris())
	// fmt.Println("Launching task", taskInfo.GetName(), "with command", taskInfo.Command.GetValue())
	// fmt.Println("Launching task", taskInfo.GetName(), "with command", taskInfo.Command.GetShell())

	// ExecutorInfo
	cmd := taskInfo.Executor.GetCommand()

	fmt.Println("Launching task", taskInfo.GetName(), "with command[GetUris]", cmd.GetUris())
	fmt.Println("Launching task", taskInfo.GetName(), "with command[GetValue]", cmd.GetValue())
	fmt.Println("Launching task", taskInfo.GetName(), "with command[GetShell]", cmd.GetShell())
	fmt.Println("Launching task", taskInfo.GetName(), "with command[GetArguments]", cmd.GetArguments())

	execcmd := strings.Join(cmd.GetArguments(), " ")
	fmt.Printf("execcmd = %s\n", execcmd)

	runStatus := &mesos.TaskStatus{
		TaskId: taskInfo.GetTaskId(),
		State:  mesos.TaskState_TASK_RUNNING.Enum(),
	}
	_, err := driver.SendStatusUpdate(runStatus)
	if err != nil {
		fmt.Println("Got error", err)
	}

	exec.tasksLaunched++
	fmt.Println("Total tasks launched ", exec.tasksLaunched)
	//
	// this is where one would perform the requested task
	//

	output, err := osexec.Command("sh", "-c", execcmd).Output()

	if err != nil {
		fmt.Println("Command exec error", err)
	}
	fmt.Println("Exec output>")
	fmt.Println(string(output))

	//
	// this is where one would perform the requested task
	//

	// finish task
	fmt.Println("Finishing task", taskInfo.GetName())
	finStatus := &mesos.TaskStatus{
		TaskId: taskInfo.GetTaskId(),
		State:  mesos.TaskState_TASK_FINISHED.Enum(),
	}
	_, err = driver.SendStatusUpdate(finStatus)
	if err != nil {
		fmt.Println("Got error", err)
	}
	fmt.Println("Task finished", taskInfo.GetName())
	fmt.Println("<------------------ Executor finish ------------------>\n") 
}

func (exec *exampleExecutor) KillTask(exec.ExecutorDriver, *mesos.TaskID) {
	fmt.Println("Kill task")
}

func (exec *exampleExecutor) FrameworkMessage(driver exec.ExecutorDriver, msg string) {
	fmt.Println("Got framework message: ", msg)
}

func (exec *exampleExecutor) Shutdown(exec.ExecutorDriver) {
	fmt.Println("Shutting down the executor")
}

func (exec *exampleExecutor) Error(driver exec.ExecutorDriver, err string) {
	fmt.Println("Got error message:", err)
}

// -------------------------- func inits () ----------------- //
func init() {
	flag.Parse()
}

func main() {
	fmt.Println("Starting Example Executor (Go)")

	dconfig := exec.DriverConfig{
		Executor: newExampleExecutor(),
	}
	driver, err := exec.NewMesosExecutorDriver(dconfig)

	if err != nil {
		fmt.Println("Unable to create a ExecutorDriver ", err.Error())
	}

	_, err = driver.Start()
	if err != nil {
		fmt.Println("Got error:", err)
		return
	}
	fmt.Println("Executor process has started and running.")
	_, err = driver.Join()
	if err != nil {
		fmt.Println("driver failed:", err)
	}
	fmt.Println("executor terminating")
}
