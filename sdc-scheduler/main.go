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
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
//	"time"

	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/auth"
	"github.com/mesos/mesos-go/auth/sasl"
	"github.com/mesos/mesos-go/auth/sasl/mech"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
	"golang.org/x/net/context"
)

const (
	CPUS_PER_TASK       = 1
	MEM_PER_TASK        = 128
	defaultArtifactPort = 12345
)

var (
	address      = flag.String("address", "127.0.0.1", "Binding address for artifact server")
	artifactPort = flag.Int("artifactPort", defaultArtifactPort, "Binding port for artifact server")
	authProvider = flag.String("mesos_authentication_provider", sasl.ProviderName,
		fmt.Sprintf("Authentication provider to use, default is SASL that supports mechanisms: %+v", mech.ListSupported()))
	master              = flag.String("master", "127.0.0.1:5050", "Master address <ip:port>")
	executorPath        = flag.String("executor", "./executor", "Path to test executor")
	// taskCount           = flag.String("task-count", "3", "Total task count to run.")
	taskCount           = flag.String("task-count", "5", "Total task count to run.")
	mesosAuthPrincipal  = flag.String("mesos_authentication_principal", "", "Mesos authentication principal.")
	mesosAuthSecretFile = flag.String("mesos_authentication_secret_file", "", "Mesos authentication secret file.")

	// files  = []string{"/var/tmp/1.txt", "/var/tmp/2.txt", "/var/tmp/3.txt"}
	files  = []string{}
	target = NewTargetFile(files)
)

type ExampleScheduler struct {
	executor      *mesos.ExecutorInfo
	tasksLaunched int
	tasksFinished int
	totalTasks    int
}

func newExampleScheduler(exec *mesos.ExecutorInfo) *ExampleScheduler {
	total, err := strconv.Atoi(*taskCount)
	if err != nil {
		total = 5
	}
	return &ExampleScheduler{
		executor:      exec,
		tasksLaunched: 0,
		tasksFinished: 0,
		totalTasks:    total,
	}
}

func (sched *ExampleScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Registered with Master ", masterInfo)
}

func (sched *ExampleScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Re-Registered with Master ", masterInfo)
}

func (sched *ExampleScheduler) Disconnected(sched.SchedulerDriver) {
	log.Fatalf("disconnected from master, aborting")
}

func (sched *ExampleScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {

	if sched.tasksLaunched >= sched.totalTasks {
		log.Info("decline all of the offers since all of our tasks are already launched")
		ids := make([]*mesos.OfferID, len(offers))
		for i, offer := range offers {
			ids[i] = offer.Id
		}
		driver.LaunchTasks(ids, []*mesos.TaskInfo{}, &mesos.Filters{RefuseSeconds: proto.Float64(120)})
		return
	}


	log.Info("prepare pass args: ", sched.executor.Command.Arguments)
        // [/bin/cat /var/tmp/1.txt /var/tmp/2.txt /var/tmp/3.txt | /bin/grep abe > /var/tmp/grep-result.txt]
	// 
	// rebuild args
	// 1. /bin/cat /var/tmp/1.txt >> /var/tmp/intermediate.txt
	// 2. /bin/cat /var/tmp/2.txt >> /var/tmp/intermediate.txt
	// 3. /bin/cat /var/tmp/3.txt >> /var/tmp/intermediate.txt
	// 4. /bin/grep abe > /var/tmp/grep-result.txt
	// 5. /bin/rm /var/tmp/intermediate.txt

	for _, offer := range offers {
		cpuResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == "cpus"
		})
		cpus := 0.0
		for _, res := range cpuResources {
			cpus += res.GetScalar().GetValue()
		}

		memResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == "mem"
		})
		mems := 0.0
		for _, res := range memResources {
			mems += res.GetScalar().GetValue()
		}

		log.Infoln("Received Offer <", offer.Id.GetValue(), "> with cpus=", cpus, " mem=", mems)

		remainingCpus := cpus
		remainingMems := mems

		var tasks []*mesos.TaskInfo

                // $ cat 1.txt 2.txt. 3.txt | wc -lの場合
                // 先に、後ろのタスクを上げておく必要がある？
                // 
                // コンセプト実装はシンプルに中間ファイル方式で行く
                // 遅いけど

		for sched.tasksLaunched < sched.totalTasks &&
			CPUS_PER_TASK <= remainingCpus &&
			MEM_PER_TASK <= remainingMems {

			sched.tasksLaunched++

			taskId := &mesos.TaskID{
				Value: proto.String(strconv.Itoa(sched.tasksLaunched)),
			}

			// executionidの書き換え
			sched.executor.ExecutorId = util.NewExecutorID(taskId.GetValue())

			// file := fmt.Sprintf("/var/tmp/%s.txt", taskId.GetValue())
			log.Infof("sched.tasksLaunched = %d\n", sched.tasksLaunched)
			log.Infof("sched.totalTasks = %d\n", sched.totalTasks)

			file := target.Shift()
			log.Infof("file name = %s\n", file)
			log.Infof("files len = %d\n", target.GetLen())

			// sched.executor.Command.Arguments で書き換えても保持されている
			// 値はポインタなので、LaunchTasksするときに、複数のタスクでまとめられているので、
			// 値は最後に上書きされた物になる
			// そこで、Argumentsがタスクごとにまとめられないように個別にオブジェクトを生成してタスクを
			// 起動する
			exec := &mesos.ExecutorInfo{
				ExecutorId: sched.executor.GetExecutorId(),
				Name:	    proto.String(sched.executor.GetName()),
				Source:	    proto.String(sched.executor.GetSource()),
	                	Command: &mesos.CommandInfo{
               				Value: proto.String(sched.executor.Command.GetValue()),
                    	 		Uris:  sched.executor.Command.GetUris(),
               			},
			}
			// exec.Command.Arguments = []string{"/bin/cat", file}

                        if sched.tasksLaunched == sched.totalTasks {
				exec.Command.Arguments = []string{"/bin/rm", "/var/tmp/intermediate.txt"}
			} else if file == "" {
				// exec.Command.Arguments = []string{"wc -l", "/var/tmp/intermediate.txt", ">", "/var/tmp/wc-result.txt"}
				exec.Command.Arguments = []string{"/bin/grep", "abe", "/var/tmp/intermediate.txt", ">", "/var/tmp/grep-result.txt"}
			} else {
				exec.Command.Arguments = []string{"/bin/cat", file, ">>", "/var/tmp/intermediate.txt"}
			}

			task := &mesos.TaskInfo{
				Name:     proto.String("go-task-" + taskId.GetValue()),
				TaskId:   taskId,
				SlaveId:  offer.SlaveId,
				// Executor: sched.executor,
				Executor: exec,
				Resources: []*mesos.Resource{
					util.NewScalarResource("cpus", CPUS_PER_TASK),
					util.NewScalarResource("mem", MEM_PER_TASK),
				},
			}
			log.Infof("Prepared task: %s with offer %s for launch\n", task.GetName(), offer.Id.GetValue())

			tasks = append(tasks, task)
			remainingCpus -= CPUS_PER_TASK
			remainingMems -= MEM_PER_TASK
		}

		log.Infoln("Launching ", len(tasks), "tasks for offer", offer.Id.GetValue())
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(5)})
	}

        // time.Sleep(30 * time.Second)
}

func (sched *ExampleScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())
	if status.GetState() == mesos.TaskState_TASK_FINISHED {
		sched.tasksFinished++
	}

	if sched.tasksFinished >= sched.totalTasks {
		log.Infoln("Total tasks completed, stopping framework.")
		driver.Stop(false)
	}

	if status.GetState() == mesos.TaskState_TASK_LOST ||
		status.GetState() == mesos.TaskState_TASK_KILLED ||
		status.GetState() == mesos.TaskState_TASK_FAILED ||
		status.GetState() == mesos.TaskState_TASK_ERROR {
		log.Infoln(
			"Aborting because task", status.TaskId.GetValue(),
			"is in unexpected state", status.State.String(),
			"with message", status.GetMessage(),
		)
		driver.Abort()
	}
}

func (sched *ExampleScheduler) OfferRescinded(_ sched.SchedulerDriver, oid *mesos.OfferID) {
	log.Errorf("offer rescinded: %v", oid)
}
func (sched *ExampleScheduler) FrameworkMessage(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, msg string) {
	log.Errorf("framework message from executor %q slave %q: %q", eid, sid, msg)
}
func (sched *ExampleScheduler) SlaveLost(_ sched.SchedulerDriver, sid *mesos.SlaveID) {
	log.Errorf("slave lost: %v", sid)
}
func (sched *ExampleScheduler) ExecutorLost(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, code int) {
	log.Errorf("executor %q lost on slave %q code %d", eid, sid, code)
}
func (sched *ExampleScheduler) Error(_ sched.SchedulerDriver, err string) {
	log.Errorf("Scheduler received error:", err)
}

// ----------------------- func init() ------------------------- //

func init() {
	flag.Parse()
	log.Infoln("Initializing the Example Scheduler...")
}

// returns (downloadURI, basename(path))
func serveExecutorArtifact(path string) (*string, string) {
	serveFile := func(pattern string, filename string) {
		http.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filename)
		})
	}

	// Create base path (http://foobar:5000/<base>)
	pathSplit := strings.Split(path, "/")
	var base string
	if len(pathSplit) > 0 {
		base = pathSplit[len(pathSplit)-1]
	} else {
		base = path
	}
	serveFile("/"+base, path)

	hostURI := fmt.Sprintf("http://%s:%d/%s", *address, *artifactPort, base)
	log.V(2).Infof("Hosting artifact '%s' at '%s'", path, hostURI)

	return &hostURI, base
}

func prepareExecutorInfo(args []string) *mesos.ExecutorInfo {
	executorUris := []*mesos.CommandInfo_URI{}
	uri, executorCmd := serveExecutorArtifact(*executorPath)
	executorUris = append(executorUris, &mesos.CommandInfo_URI{Value: uri, Executable: proto.Bool(true)})
	log.Infof("uri = %s, executorcmd = %s\n", uri, executorCmd)
	log.Infof("executorUris = %s\n", executorUris)

	// forward the value of the scheduler's -v flag to the executor
	v := 0
	if f := flag.Lookup("v"); f != nil && f.Value != nil {
		if vstr := f.Value.String(); vstr != "" {
			if vi, err := strconv.ParseInt(vstr, 10, 32); err == nil {
				v = int(vi)
			}
		}
	}
	executorCommand := fmt.Sprintf("./%s -logtostderr=true -v=%d", executorCmd, v)

	go http.ListenAndServe(fmt.Sprintf("%s:%d", *address, *artifactPort), nil)
	log.V(2).Info("Serving executor artifacts...")

	// Create mesos scheduler driver.
	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID("default"),
		Name:       proto.String("Test Executor (Go)"),
		Source:     proto.String("go_test"),
		Command: &mesos.CommandInfo{
			Value: proto.String(executorCommand),
			Uris:  executorUris,
			Arguments: args,
		},
	}
}

func parseIP(address string) net.IP {
	addr, err := net.LookupIP(address)
	if err != nil {
		log.Fatal(err)
	}
	if len(addr) < 1 {
		log.Fatalf("failed to parse IP from address '%v'", address)
	}
	return addr[0]
}

type TargetFile struct {
        files []string
}

func NewTargetFile(files []string) *TargetFile {
        targetFile := &TargetFile{files: files}
        return targetFile
}

func (target *TargetFile) GetLen() int {
        return len(target.files)
}

func (target *TargetFile) Shift() string {
	if target.GetLen() != 0 {
        	val := target.files[0]
        	target.files = target.files[1:]
        	return val
        } else {
		return ""
        }
}

// ----------------------- func main() ------------------------- //

func main() {
	// target files
	files  = []string{"/var/tmp/1.txt", "/var/tmp/2.txt", "/var/tmp/3.txt"}
	target = NewTargetFile(files)

	// build command executor
	args := []string{"/bin/cat", "/var/tmp/1.txt", "/var/tmp/2.txt", "/var/tmp/3.txt", "|", "/bin/grep", "abe", ">", "/var/tmp/grep-result.txt"}
	exec := prepareExecutorInfo(args)

	// the framework
	fwinfo := &mesos.FrameworkInfo{
		User: proto.String(""), // Mesos-go will fill in user.
		Name: proto.String("Test Framework (SDC)"),
	}

	cred := (*mesos.Credential)(nil)
	if *mesosAuthPrincipal != "" {
		fwinfo.Principal = proto.String(*mesosAuthPrincipal)
		secret, err := ioutil.ReadFile(*mesosAuthSecretFile)
		if err != nil {
			log.Fatal(err)
		}
		cred = &mesos.Credential{
			Principal: proto.String(*mesosAuthPrincipal),
			Secret:    secret,
		}
	}
	bindingAddress := parseIP(*address)
	config := sched.DriverConfig{
		Scheduler:      newExampleScheduler(exec),
		Framework:      fwinfo,
		Master:         *master,
		Credential:     cred,
		BindingAddress: bindingAddress,
		WithAuthContext: func(ctx context.Context) context.Context {
			ctx = auth.WithLoginProvider(ctx, *authProvider)
			ctx = sasl.WithBindingAddress(ctx, bindingAddress)
			return ctx
		},
	}
	driver, err := sched.NewMesosSchedulerDriver(config)

	if err != nil {
		log.Errorln("Unable to create a SchedulerDriver ", err.Error())
	}

	if stat, err := driver.Run(); err != nil {
		log.Infof("Framework stopped with status %s and error: %s\n", stat.String(), err.Error())
	}
	log.Infof("framework terminating")
}
