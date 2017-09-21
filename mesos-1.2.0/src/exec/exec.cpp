// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <signal.h>

#include <sys/types.h>

#include <atomic>
#include <iostream>
#include <string>
#include <sstream>

#include <mesos/executor.hpp>
#include <mesos/mesos.hpp>
#include <mesos/type_utils.hpp>

#include <process/delay.hpp>
#include <process/dispatch.hpp>
#include <process/id.hpp>
#include <process/latch.hpp>
#include <process/process.hpp>
#include <process/protobuf.hpp>

#include <stout/duration.hpp>
#include <stout/linkedhashmap.hpp>
#include <stout/hashset.hpp>
#include <stout/numify.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/stopwatch.hpp>
#include <stout/stringify.hpp>
#include <stout/synchronized.hpp>
#include <stout/uuid.hpp>

#include "common/protobuf_utils.hpp"

#include "logging/flags.hpp"
#include "logging/logging.hpp"

#include "messages/messages.hpp"

#include "slave/constants.hpp"
#include "slave/state.hpp"

#include "version/version.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::slave;

using namespace process;

using std::string;

using process::Latch;
using process::wait; // Necessary on some OS's to disambiguate.

using mesos::Executor; // Necessary on some OS's to disambiguate.

namespace mesos {
namespace internal {

// The ShutdownProcess is a relic of the pre-cgroup process isolation
// days. It ensures that the executor process tree is killed after a
// shutdown has been sent.
//
// TODO(bmahler): Update 'delay' to handle deferred callbacks without
// needing a Process. This would eliminate the need for an explicit
// Process here, see: MESOS-4729.
class ShutdownProcess : public Process<ShutdownProcess> 
//下面的spawn(new ShutdownProcess(shutdownGracePeriod), true);中构造   //继承libprocess的Process类，
{
public:
  explicit ShutdownProcess(const Duration& _gracePeriod)
    : ProcessBase(ID::generate("exec-shutdown")),
      gracePeriod(_gracePeriod) {

      LOG(INFO) << "ShutdownProcess";
	  system(" echo ShutdownProcess >> /yyz2.log");
  }

protected:
  virtual void initialize()  //从下面的new ShutdownProcess(shutdownGracePeriod)执行这里
  {
    LOG(INFO) << "Scheduling shutdown of the executor in " << gracePeriod;

	system("echo received--event-initialize >> /yyz2.log");
    delay(gracePeriod, self(), &Self::kill);
  }

  void kill() //从上面的initialize调用kill，延迟gracePeriod(5S)执行
  {
    char buf[100];
    /*
	root	  6166 14199  5 13:20 pts/1    00:00:00 /home/yangyazhou/mesos-1.2.0/src/.libs/lt-mesos-agent --master=172.23.133.32:5050 --work_dir=./work-mesos --log_dir=/var/log/mesos
	root	  6206	6166  1 13:20 ? 	   00:00:00 /home/yangyazhou/mesos-1.2.0/src/.libs/lt-mesos-containerizer launch
	root	  6247	6206  0 13:20 ? 	   00:00:00 sh -c java -cp /vagrant/example-framework-1.0-SNAPSHOT-jar-with-dependencies.jar org.opencredo.mesos.ExampleExecutor > /yyz.log
	root	  6248	6247  0 13:20 ? 	   00:00:00 java -cp /vagrant/example-framework-1.0-SNAPSHOT-jar-with-dependencies.jar org.opencredo.mesos.ExampleExecutor
	yangyazhou-test--process-group-execute32-pid:6248-pgid:6206
	*/

	snprintf(buf, 100, "echo yangyazhou-test--process-group-execute32233-pid:%d-pgid:%d >> /yyz2.log", 
		getpid(), getpgid(0));
    system("echo yangyazhou-test-Committing-suicide-by-killing-the-process-group::execute3-333 >> /yyz2.log");
    system(buf);
	
    LOG(INFO) << "Committing suicide by killing the process group";

    // TODO(vinod): Invoke killtree without killing ourselves.
    // Kill the process group (including ourself).
#ifndef __WINDOWS__
    /*
    killpg() sends the signal sig to the process group pgrp. See signal(7) for a list of signals. 
    If pgrp is 0, killpg() sends the signal to the sending process’s process group.
    (POSIX says: If pgrp is less than or equal to 1, the behaviour is undefined.)
    */
    killpg(0, SIGKILL); //yang add test   ps  xao pid,ppid,pgid,sid  | grep 13916 查看进程组
#else
    LOG(WARNING) << "Shutting down process group. Windows does not support "
                    "`killpg`, so we simply call `exit` on the assumption "
                    "that the process was generated with the "
                    "`WindowsContainerizer`, which uses the 'close on exit' "
                    "feature of job objects to make sure all child processes "
                    "are killed when a parent process exits";
    exit(0);
#endif // __WINDOWS__

    // The signal might not get delivered immediately, so sleep for a
    // few seconds. Worst case scenario, exit abnormally.
    os::sleep(Seconds(5));
    exit(EXIT_FAILURE);
  }

private:
  const Duration gracePeriod;
};

/* 
Apache Mesos的任务分配过程: http://dongxicheng.org/apache-mesos/apache-mesos-task-assignment/
步骤1 当出现以下几种事件中的一种时，会触发资源分配行为：新框架注册、框架注销、增加节点、出现空闲资源等；
步骤2 Mesos Master中的Allocator模块为某个框架分配资源，并将资源封装到ResourceOffersMessage（Protocal Buffer Message）中，SchedulerProcess通过网络传输给用户；
步骤3 SchedulerProcess调用用户编写的Scheduler中的resourceOffers函数（不能版本可能有变动），告之有新资源可用；
步骤4 用户的Scheduler调用MesosSchedulerDriver中的launchTasks()函数，告之将要启动的任务；
步骤5 SchedulerProcess将待启动的任务封装到LaunchTasksMessage（Protocal Buffer Message）中，通过网络传输给Mesos Master；
步骤6 Mesos Master将待启动的任务封装成RunTaskMessage发送给各个Mesos Slave；
步骤7 Mesos Slave收到RunTaskMessage消息后，将之进一步发送给对应的ExecutorProcess；
步骤8 ExecutorProcess收到消息后，进行资源本地化，并准备任务运行环境，最终调用用户编写的Executor中的launchTask启动任务（如果Executor尚未启动，则先要启动Executor）。
*/

/*
  Executor注册过程  http://dongxicheng.org/apache-mesos/apache-mesos-framework-executor-registering/
本节描述框架frameworkX在某个slaveX上注册executor executorX的过程：
（1）Master第一次向slaveX发送执行frameworkX中task的消息 RunTaskMessage
（2）slave收到该消息后，运行相应的消息处理函数runTask()
（3）该函数发现该slave上未启动frameworkX对应的executorX，则调用IsolationModule的lauchExecutor()函数
（4）该函数创建一个FrameworkExecutor对象，并调用ExecutorProcess的Initialize()函数进行初始化，同时启动TaskTracker
（5）Initialize()函数创建消息RegisterExecutorMessage，并发送给slave
（6）Slave收到该消息后，调用对象的消息处理函数registerExecutor，该函数创建ExecutorRegisteredMessage消息，返回给ExecutorProcess
（7）ExecutorProcess收到该消息后，调用对应的消息处理函数registered()，该函数再进一步调用FrameworkExecutor的registered()函数
*/

//SchedulerProcess线程对应MesosSchedulerDriver处理  ExecutorProcess线程对应MesosExecutorDriver处理
//参考http://dongxicheng.org/apache-mesos/apache-mesos-task-assignment/
class ExecutorProcess : public ProtobufProcess<ExecutorProcess>
{  //MesosExecutorDriver::start 中new该对象，该对象中的报文处理是scheduler->mesos-master->mesos-slave->executor调用走到这里解析到
public:
  ExecutorProcess(
      const UPID& _slave,
      MesosExecutorDriver* _driver,
      Executor* _executor,
      const SlaveID& _slaveId,
      const FrameworkID& _frameworkId,
      const ExecutorID& _executorId,
      bool _local,
      const string& _directory,
      bool _checkpoint,
      const Duration& _recoveryTimeout,
      const Duration& _shutdownGracePeriod,
      std::recursive_mutex* _mutex,
      Latch* _latch)
    : ProcessBase(ID::generate("executor")),
      slave(_slave),
      driver(_driver),
      executor(_executor),
      slaveId(_slaveId),
      frameworkId(_frameworkId),
      executorId(_executorId),
      connected(false),
      connection(UUID::random()),
      local(_local),
      aborted(false),
      mutex(_mutex),
      latch(_latch),
      directory(_directory),
      checkpoint(_checkpoint),
      recoveryTimeout(_recoveryTimeout),
      shutdownGracePeriod(_shutdownGracePeriod)
  {
    char buf[100];
	snprintf(buf, 100, "echo ExecutorProcess-checkpoint:%d >> /yyz2.log", checkpoint); //scheduler如果设置了checkpoint，这里为1打印
    system(buf);
	snprintf(buf, 100, "echo ExecutorProcess-local:%d >> /yyz2.log", local); //scheduler如果设置了checkpoint，这里为1打印
    system(buf);
	
    LOG(INFO) << "Version: " << MESOS_VERSION;
	//http://dongxicheng.org/apache-mesos/apache-mesos-communications/
	//scheduler的消息类型及处理函数(SchedulerProcess::initialize)
	  //Executor的消息类型及处理函数(ExecutorProcess)
	  // Mesos-Slave的消息类型及处理函数(Slave::initialize)
	  // Mesos-master的消息类型及处理函数(Master::initialize)

	//报文处理是scheduler->mesos-master->mesos-slave->executor调用走到这里解析到

	//executor注册报文
	//报文处理是scheduler->mesos-master->mesos-slave->executor调用走到这里解析到
    install<ExecutorRegisteredMessage>( 
        &ExecutorProcess::registered,
        &ExecutorRegisteredMessage::executor_info,
        &ExecutorRegisteredMessage::framework_id,
        &ExecutorRegisteredMessage::framework_info,
        &ExecutorRegisteredMessage::slave_id,
        &ExecutorRegisteredMessage::slave_info);

    install<ExecutorReregisteredMessage>(
        &ExecutorProcess::reregistered,
        &ExecutorReregisteredMessage::slave_id,
        &ExecutorReregisteredMessage::slave_info);

    install<ReconnectExecutorMessage>(
        &ExecutorProcess::reconnect,
        &ReconnectExecutorMessage::slave_id);

	//运行某个task
    install<RunTaskMessage>(
        &ExecutorProcess::runTask,
        &RunTaskMessage::task);
    //杀死task报文
    install<KillTaskMessage>(
        &ExecutorProcess::killTask,
        &KillTaskMessage::task_id);

    install<StatusUpdateAcknowledgementMessage>(
        &ExecutorProcess::statusUpdateAcknowledgement,
        &StatusUpdateAcknowledgementMessage::slave_id,
        &StatusUpdateAcknowledgementMessage::framework_id,
        &StatusUpdateAcknowledgementMessage::task_id,
        &StatusUpdateAcknowledgementMessage::uuid);
    //framework向executor发送的报文，比如杀掉某个task等
    install<FrameworkToExecutorMessage>(
        &ExecutorProcess::frameworkMessage,
        &FrameworkToExecutorMessage::slave_id,
        &FrameworkToExecutorMessage::framework_id,
        &FrameworkToExecutorMessage::executor_id,
        &FrameworkToExecutorMessage::data);

	//某个executor关闭
    install<ShutdownExecutorMessage>(
        &ExecutorProcess::shutdown);
  }

  virtual ~ExecutorProcess() {}

protected:
  virtual void initialize()
  {
    VLOG(1) << "Executor started at: " << self()
            << " with pid " << getpid();

    link(slave);

    // Register with slave.
    RegisterExecutorMessage message;
    message.mutable_framework_id()->MergeFrom(frameworkId);
    message.mutable_executor_id()->MergeFrom(executorId);
    send(slave, message);
  }

  void registered(
      const ExecutorInfo& executorInfo,
      const FrameworkID& frameworkId,
      const FrameworkInfo& frameworkInfo,
      const SlaveID& slaveId,
      const SlaveInfo& slaveInfo)
  {
    if (aborted.load()) {
      VLOG(1) << "Ignoring registered message from agent " << slaveId
              << " because the driver is aborted!";
      return;
    }

    LOG(INFO) << "Executor registered on agent " << slaveId;

    connected = true;
    connection = UUID::random();

    Stopwatch stopwatch;
    if (FLAGS_v >= 1) {
      stopwatch.start();
    }

    executor->registered(driver, executorInfo, frameworkInfo, slaveInfo);

    VLOG(1) << "Executor::registered took " << stopwatch.elapsed();
  }

  void reregistered(const SlaveID& slaveId, const SlaveInfo& slaveInfo)
  {
    if (aborted.load()) {
      VLOG(1) << "Ignoring re-registered message from agent " << slaveId
              << " because the driver is aborted!";
      return;
    }

    LOG(INFO) << "Executor re-registered on agent " << slaveId;

    connected = true;
    connection = UUID::random();

    Stopwatch stopwatch;
    if (FLAGS_v >= 1) {
      stopwatch.start();
    }

    executor->reregistered(driver, slaveInfo);

    VLOG(1) << "Executor::reregistered took " << stopwatch.elapsed();
  }

  void reconnect(const UPID& from, const SlaveID& slaveId)
  {
    if (aborted.load()) {
      VLOG(1) << "Ignoring reconnect message from agent " << slaveId
              << " because the driver is aborted!";
      return;
    }

    LOG(INFO) << "Received reconnect request from agent " << slaveId;

    // Update the slave link.
    slave = from;

    // We force a reconnect here to avoid sending on a stale "half-open"
    // socket. We do not detect a disconnection in some cases when the
    // connection is terminated by a netfilter module e.g., iptables
    // running on the agent (see MESOS-5332).
    link(slave, RemoteConnection::RECONNECT);

    // Re-register with slave.
    ReregisterExecutorMessage message;
    message.mutable_executor_id()->MergeFrom(executorId);
    message.mutable_framework_id()->MergeFrom(frameworkId);

    // Send all unacknowledged updates.
    foreachvalue (const StatusUpdate& update, updates) {
      message.add_updates()->MergeFrom(update);
    }

    // Send all unacknowledged tasks.
    foreachvalue (const TaskInfo& task, tasks) {
      message.add_tasks()->MergeFrom(task);
    }

    send(slave, message);
  }

  void runTask(const TaskInfo& task)
  {
    if (aborted.load()) {
      VLOG(1) << "Ignoring run task message for task " << task.task_id()
              << " because the driver is aborted!";
      return;
    }

    CHECK(!tasks.contains(task.task_id()))
      << "Unexpected duplicate task " << task.task_id();

    tasks[task.task_id()] = task;

    VLOG(1) << "Executor asked to run task '" << task.task_id() << "'";

    Stopwatch stopwatch;
    if (FLAGS_v >= 1) {
      stopwatch.start();
    }

    executor->launchTask(driver, task);

    VLOG(1) << "Executor::launchTask took " << stopwatch.elapsed();
  }

  void killTask(const TaskID& taskId)
  {
    if (aborted.load()) {
      LOG(INFO) << "Ignoring kill task message for task " << taskId
              << " because the driver is aborted!";
      return;
    }

    LOG(INFO) << "Executor asked to kill task '" << taskId << "'";

    Stopwatch stopwatch;
    if (FLAGS_v >= 1) {
      stopwatch.start();
    }

    executor->killTask(driver, taskId);

    LOG(INFO) << "Executor::killTask took " << stopwatch.elapsed();
  }

  void statusUpdateAcknowledgement(
      const SlaveID& slaveId,
      const FrameworkID& frameworkId,
      const TaskID& taskId,
      const string& uuid)
  {
    Try<UUID> uuid_ = UUID::fromBytes(uuid);
    CHECK_SOME(uuid_);

    if (aborted.load()) {
      VLOG(1) << "Ignoring status update acknowledgement "
              << uuid_.get() << " for task " << taskId
              << " of framework " << frameworkId
              << " because the driver is aborted!";
      return;
    }

    VLOG(1) << "Executor received status update acknowledgement "
            << uuid_.get() << " for task " << taskId
            << " of framework " << frameworkId;

    // Remove the corresponding update.
    updates.erase(uuid_.get());

    // Remove the corresponding task.
    tasks.erase(taskId);
  }

  void frameworkMessage(
      const SlaveID& slaveId,
      const FrameworkID& frameworkId,
      const ExecutorID& executorId,
      const string& data)
  {
    if (aborted.load()) {
      VLOG(1) << "Ignoring framework message because the driver is aborted!";
      return;
    }

    LOG(INFO) << "Executor received framework message";

    Stopwatch stopwatch;
    if (FLAGS_v >= 1) {
      stopwatch.start();
    }

    executor->frameworkMessage(driver, data);

    LOG(INFO)  << "Executor::frameworkMessage took " << stopwatch.elapsed();
  }

  void shutdown()
  {
    
	LOG(INFO) << "Executor::shutdown took";
    if (aborted.load()) {
      VLOG(1) << "Ignoring shutdown message because the driver is aborted!";
      return;
    }

    LOG(INFO) << "Executor asked to shutdown";

	system("echo exec-cpp-shutdown >> /yyz2.log");
    if (!local) {
      // Start the Shutdown Process.
      spawn(new ShutdownProcess(shutdownGracePeriod), true);
    }

    Stopwatch stopwatch;
    if (FLAGS_v >= 1) {
      stopwatch.start();
    }

    // TODO(benh): Any need to invoke driver.stop?
    executor->shutdown(driver);

    LOG(INFO)  << "Executor::shutdown took " << stopwatch.elapsed();

    aborted.store(true); // To make sure not to accept any new messages.

    if (local) {
      terminate(this);
    }
  }

  void stop()
  {
    terminate(self());

    synchronized (mutex) {
      CHECK_NOTNULL(latch)->trigger();
    }
  }

  void abort()
  {
    LOG(INFO) << "Deactivating the executor libprocess";
    CHECK(aborted.load());

    synchronized (mutex) {
      CHECK_NOTNULL(latch)->trigger();
    }
  }

  void _recoveryTimeout(UUID _connection)
  {
    // If we're connected, no need to shut down the driver!
    if (connected) {
      return;
    }

    // We need to compare the connections here to ensure there have
    // not been any subsequent re-registrations with the slave in the
    // interim.
    if (connection == _connection) {
      LOG(INFO) << "Recovery timeout of " << recoveryTimeout << " exceeded; "
                << "Shutting down";
      shutdown();
    }
  }

/*
   * Invoked when a linked process has exited.
   *
   * For local linked processes (i.e., when the linker and linkee are
   * part of the same OS process), this can be used to reliably detect
   * when the linked process has exited.
   *
   * For remote linked processes, this indicates that the persistent
   * TCP connection between the linker and the linkee has failed
   * (e.g., linkee process died, a network error occurred). In this
   * situation, the remote linkee process might still be running.
   *
   * @see process::ProcessBase::link
  virtual void exited(const UPID&) {}
参考libprocess exited说明，如果tcp链接失败，则会触发执行exited，test-executor程序是由mesos-containerizer launch进程骑起来，但是
test-executor进程是直接和mesos-slave通信的，如果mesos-slave挂掉，或者网络问题，就会触发test-executor执行exited函数
*/

  //exec和相关的日志记录在work-mesos/slaves/fba6e290-5566-420e-8489-1dc87ddfdc93-S19/frameworks
   // /fba6e290-5566-420e-8489-1dc87ddfdc93-0006/executors/ExampleExecutor/runs/a170553e-be2b-4cb8-a953-e7630459c22f/ stderr  stdout 
   //见https://issues.apache.org/jira/browse/MESOS-7885?filter=-2

  virtual void exited(const UPID& pid) //和mesos-slave TCP异常，则通过libprocess的exited(event.pid)走到这里
  {
    system("echo yang-exited >> /yyz2.log");
    if (aborted.load()) {
      VLOG(1) << "Ignoring exited event because the driver is aborted!";
      return;
    }

    // If the framework has checkpointing enabled and the executor has
    // successfully registered with the slave, the slave can reconnect with
    // this executor when it comes back up and performs recovery!

	//如果framwork scheduler的checkpoint打开，则不会走下面的shutdown流程
    if (checkpoint && connected) { 
      connected = false;

      LOG(INFO) << "Agent exited, but framework has checkpointing enabled. "
                << "Waiting " << recoveryTimeout << " to reconnect with agent "
                << slaveId;

	  system("echo recoveryTimeout-yang-exited-checkpoint >> /yyz2.log");
      delay(recoveryTimeout, self(), &Self::_recoveryTimeout, connection); //延迟重连

      return;
    }

    LOG(INFO) << "Agent exited ... shutting down";

    connected = false;

    if (!local) {
      // Start the Shutdown Process.
      system("echo ShutdownProcess-yang-exited >> /yyz2.log");
      spawn(new ShutdownProcess(shutdownGracePeriod), true);
    }

    Stopwatch stopwatch;
    if (FLAGS_v >= 1) {
      stopwatch.start();
    }

	
    // TODO(benh): Pass an argument to shutdown to tell it this is abnormal?
    executor->shutdown(driver);
	system("echo ShutdownProcess-yang-shutdown end>> /yyz2.log");

    LOG(INFO) << "Executor::shutdown took " << stopwatch.elapsed();

    aborted.store(true); // To make sure not to accept any new messages.

    // This is a pretty bad state ... no slave is left. Rather
    // than exit lets kill our process group (which includes
    // ourself) hoping to clean up any processes this executor
    // launched itself.
    // TODO(benh): Maybe do a SIGTERM and then later do a SIGKILL?
    if (local) {
      terminate(this);
    }
	
	system("echo ShutdownProcess-yang-terminate end>> /yyz2.log");
  }

  void sendStatusUpdate(const TaskStatus& status)
  {
    StatusUpdateMessage message;
    StatusUpdate* update = message.mutable_update();
    update->mutable_framework_id()->MergeFrom(frameworkId);
    update->mutable_executor_id()->MergeFrom(executorId);
    update->mutable_slave_id()->MergeFrom(slaveId);
    update->mutable_status()->MergeFrom(status);
    update->set_timestamp(Clock::now().secs());
    update->mutable_status()->set_timestamp(update->timestamp());
    message.set_pid(self());

    // We overwrite the UUID for this status update, however with
    // the HTTP API, the executor will have to generate a UUID
    // (which needs to be validated to be RFC-4122 compliant).
    UUID uuid = UUID::random();
    update->set_uuid(uuid.toBytes());
    update->mutable_status()->set_uuid(uuid.toBytes());

    // We overwrite the SlaveID for this status update, however with
    // the HTTP API, this can be overwritten by the slave instead.
    update->mutable_status()->mutable_slave_id()->CopyFrom(slaveId);

    VLOG(1) << "Executor sending status update " << *update;

    // Capture the status update.
    updates[uuid] = *update;

    send(slave, message);
  }

  void sendFrameworkMessage(const string& data)
  {
    ExecutorToFrameworkMessage message;
    message.mutable_slave_id()->MergeFrom(slaveId);
    message.mutable_framework_id()->MergeFrom(frameworkId);
    message.mutable_executor_id()->MergeFrom(executorId);
    message.set_data(data);
    send(slave, message);
  }

private:
  friend class mesos::MesosExecutorDriver;

  UPID slave;
  MesosExecutorDriver* driver;
  Executor* executor;
  SlaveID slaveId;
  FrameworkID frameworkId;
  ExecutorID executorId;
  bool connected; // Registered with the slave.
  UUID connection; // UUID to identify the connection instance.
  bool local;
  std::atomic_bool aborted;
  std::recursive_mutex* mutex;
  Latch* latch;
  const string directory;
  bool checkpoint; //如果framwork的scheduler设置了checkpoint，则这里会为1
  Duration recoveryTimeout;
  Duration shutdownGracePeriod;

  LinkedHashMap<UUID, StatusUpdate> updates; // Unacknowledged updates.

  // We store tasks that have not been acknowledged
  // (via status updates) by the slave. This ensures that, during
  // recovery, the slave relaunches only those tasks that have
  // never reached this executor.
  LinkedHashMap<TaskID, TaskInfo> tasks; // Unacknowledged tasks.
};

} // namespace internal {
} // namespace mesos {


//SchedulerProcess线程对应MesosSchedulerDriver处理  ExecutorProcess线程对应MesosExecutorDriver处理
//参考http://dongxicheng.org/apache-mesos/apache-mesos-task-assignment/

// Implementation of C++ API.  Executor的运行主要依赖于MesosExecutorDriver作为封装，和mesos-slave进行通信。
MesosExecutorDriver::MesosExecutorDriver(mesos::Executor* _executor)  
//executor程序(例如test-executor)会用该类做封装链接mesos-slave
  : executor(_executor),
    process(nullptr),
    latch(nullptr),
    status(DRIVER_NOT_STARTED)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  // Load any logging flags from the environment.
  logging::Flags flags;

  Try<flags::Warnings> load = flags.load("MESOS_");

  if (load.isError()) {
    status = DRIVER_ABORTED;
    executor->error(this, load.error());
    return;
  }

  // Initialize libprocess.
  process::initialize();

  // Initialize Latch.
  latch = new Latch();

  // Initialize logging.
  if (flags.initialize_driver_logging) {
    logging::initialize("mesos", flags);
  } else {
    VLOG(1) << "Disabling initialization of GLOG logging";
  }

  // Log any flag warnings (after logging is initialized).
  foreach (const flags::Warning& warning, load->warnings) {
    LOG(WARNING) << warning.message;
  }

  spawn(new VersionProcess(), true);
}

MesosExecutorDriver::~MesosExecutorDriver()
{
  // Just like with the MesosSchedulerDriver it's possible to get a
  // deadlock here. Otherwise we terminate the ExecutorProcess and
  // wait for it before deleting.
  terminate(process);
  wait(process);
  delete process;

  delete latch;
}

Status MesosExecutorDriver::start()
{
  synchronized (mutex) {
    if (status != DRIVER_NOT_STARTED) {
         return status;
    }

    // Set stream buffering mode to flush on newlines so that we
    // capture logs from user processes even when output is redirected
    // to a file. On POSIX, the buffer size is determined by the system
    // when the `buf` parameter is null. On Windows we have to specify
    // the size, so we use 1024 bytes, a number that is arbitrary, but
    // large enough to not affect performance.
    const size_t bufferSize =
#ifdef __WINDOWS__
      1024;
#else // __WINDOWS__
      0;
#endif // __WINDOWS__
    setvbuf(stdout, nullptr, _IOLBF, bufferSize);
    setvbuf(stderr, nullptr, _IOLBF, bufferSize);

    bool local;

    UPID slave;
    SlaveID slaveId;
    FrameworkID frameworkId;
    ExecutorID executorId;
    string workDirectory;
    bool checkpoint;

    Option<string> value;
    std::istringstream iss;

    // Check if this is local (for example, for testing).
    local = os::getenv("MESOS_LOCAL").isSome();

    // Get slave PID from environment.
    value = os::getenv("MESOS_SLAVE_PID");
    if (value.isNone()) {
      EXIT(EXIT_FAILURE)
        << "Expecting 'MESOS_SLAVE_PID' to be set in the environment";
    }

    slave = UPID(value.get());
    //CHECK(slave) << "Cannot parse MESOS_SLAVE_PID '" << value.get() << "'";

    // Get slave ID from environment.
    value = os::getenv("MESOS_SLAVE_ID");
    if (value.isNone()) {
      EXIT(EXIT_FAILURE)
        << "Expecting 'MESOS_SLAVE_ID' to be set in the environment";
    }
    slaveId.set_value(value.get());

    // Get framework ID from environment.
    value = os::getenv("MESOS_FRAMEWORK_ID");
    if (value.isNone()) {
      EXIT(EXIT_FAILURE)
        << "Expecting 'MESOS_FRAMEWORK_ID' to be set in the environment";
    }
    frameworkId.set_value(value.get());

    // Get executor ID from environment.
    value = os::getenv("MESOS_EXECUTOR_ID");
    if (value.isNone()) {
      EXIT(EXIT_FAILURE)
        << "Expecting 'MESOS_EXECUTOR_ID' to be set in the environment";
    }
    executorId.set_value(value.get());

    // Get working directory from environment.
    value = os::getenv("MESOS_DIRECTORY");
    if (value.isNone()) {
      EXIT(EXIT_FAILURE)
        << "Expecting 'MESOS_DIRECTORY' to be set in the environment";
    }
    workDirectory = value.get();

    // Get executor shutdown grace period from the environment.
    //
    // NOTE: We do not require this variable to be set
    // (in contrast to the others above) for backwards
    // compatibility: agents < 0.28.0 do not set it.
    Duration shutdownGracePeriod = DEFAULT_EXECUTOR_SHUTDOWN_GRACE_PERIOD;
    value = os::getenv("MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD");
    if (value.isSome()) {
      Try<Duration> parse = Duration::parse(value.get());
      if (parse.isError()) {
        EXIT(EXIT_FAILURE)
          << "Failed to parse value '" << value.get() << "' of "
          << "'MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD': " << parse.error();
      }

      shutdownGracePeriod = parse.get();
    }

    // Get checkpointing status from environment.
    //参考https://mesos-cn.gitbooks.io/mesos-cn/content/document/runing-Mesos/Slave-Recovery.html
    value = os::getenv("MESOS_CHECKPOINT");
    checkpoint = value.isSome() && value.get() == "1";

    Duration recoveryTimeout = RECOVERY_TIMEOUT;

    // Get the recovery timeout if checkpointing is enabled.
    if (checkpoint) {
      value = os::getenv("MESOS_RECOVERY_TIMEOUT");

      if (value.isSome()) {
        Try<Duration> parse = Duration::parse(value.get());

        if (parse.isError()) {
          EXIT(EXIT_FAILURE)
            << "Failed to parse value '" << value.get() << "'"
            << " of 'MESOS_RECOVERY_TIMEOUT': " << parse.error();
        }

        recoveryTimeout = parse.get();
      }
    }

    CHECK(process == nullptr);

    process = new ExecutorProcess(
        slave,
        this,
        executor,
        slaveId,
        frameworkId,
        executorId,
        local,
        workDirectory,
        checkpoint,
        recoveryTimeout,
        shutdownGracePeriod,
        &mutex,
        latch);

    spawn(process);

    return status = DRIVER_RUNNING;
  }
}


Status MesosExecutorDriver::stop()
{
  synchronized (mutex) {
    if (status != DRIVER_RUNNING && status != DRIVER_ABORTED) {
      return status;
    }

    CHECK(process != nullptr);

    dispatch(process, &ExecutorProcess::stop);

    bool aborted = status == DRIVER_ABORTED;

    status = DRIVER_STOPPED;

    return aborted ? DRIVER_ABORTED : status;
  }
}


Status MesosExecutorDriver::abort()
{
  synchronized (mutex) {
    if (status != DRIVER_RUNNING) {
      return status;
    }

    CHECK(process != nullptr);

    // We set the atomic aborted to true here to prevent any further
    // messages from being processed in the ExecutorProcess. However,
    // if abort() is called from another thread as the ExecutorProcess,
    // there may be at most one additional message processed.
    process->aborted.store(true);

    // Dispatching here ensures that we still process the outstanding
    // requests *from* the executor, since those do proceed when
    // aborted is true.
    dispatch(process, &ExecutorProcess::abort);

    return status = DRIVER_ABORTED;
  }
}


Status MesosExecutorDriver::join()
{
  // Exit early if the driver is not running.
  synchronized (mutex) {
    if (status != DRIVER_RUNNING) {
      return status;
    }
  }

  // If the driver was running, the latch will be triggered regardless
  // of the current `status`. Wait for this to happen to signify
  // termination.
  CHECK_NOTNULL(latch)->await();

  // Now return the current `status` of the driver.
  synchronized (mutex) {
    CHECK(status == DRIVER_ABORTED || status == DRIVER_STOPPED);

    return status;
  }
}


Status MesosExecutorDriver::run()
{
  Status status = start();
  return status != DRIVER_RUNNING ? status : join();
}

Status MesosExecutorDriver::sendStatusUpdate(const TaskStatus& taskStatus)
{
  synchronized (mutex) {
    if (status != DRIVER_RUNNING) {
      return status;
    }

    CHECK(process != nullptr);

    dispatch(process, &ExecutorProcess::sendStatusUpdate, taskStatus);

    return status;
  }
}


Status MesosExecutorDriver::sendFrameworkMessage(const string& data)
{
  synchronized (mutex) {
    if (status != DRIVER_RUNNING) {
      return status;
    }

    CHECK(process != nullptr);

    dispatch(process, &ExecutorProcess::sendFrameworkMessage, data);

    return status;
  }
}
