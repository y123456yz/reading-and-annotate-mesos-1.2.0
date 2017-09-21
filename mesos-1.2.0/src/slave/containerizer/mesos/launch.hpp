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

#ifndef __MESOS_CONTAINERIZER_LAUNCH_HPP__
#define __MESOS_CONTAINERIZER_LAUNCH_HPP__

#include <string>

#include <stout/json.hpp>
#include <stout/option.hpp>
#include <stout/subcommand.hpp>

#include <mesos/mesos.hpp>


namespace mesos {
namespace internal {
namespace slave {

//mesos-containerizer的main函数在src/slave/containerizer/mesos/main.cpp
class MesosContainerizerLaunch : public Subcommand  //运行见MesosContainerizerLaunch::execute
{
public:
  static const std::string NAME;
    /*
  I0803 21:14:49.190254 36044 containerizer.cpp:1555] Launching 'mesos-containerizer' with flags 
  '--help="false" --launch_info="{"command":{"shell":true,"value":"\/home\/yangyazhou\/mesos-1.2.0
  \/src\/.libs\/test-executor"},"environment":{"variables":[{"name":"LIBPROCESS_PORT","value":"0"},
  {"name":"MESOS_AGENT_ENDPOINT","value":"172.23.133.32:5051"},{"name":"MESOS_CHECKPOINT","value":"0"},
  {"name":"MESOS_DIRECTORY","value":".\/work-mesos\/slaves\/9c35b61d-8d30-442e-984f-5a3dc9d4d499-S0\
  /frameworks\/9c35b61d-8d30-442e-984f-5a3dc9d4d499-0000\/executors\/default\/runs\/c2525747-a3fb-490d-af
  ef-357dfcd7c186"},{"name":"MESOS_EXECUTOR_ID","value":"default"},{"name":"MESOS_EXECUTOR_SHUTDOWN_GR
  ACE_PERIOD","value":"5secs"},{"name":"MESOS_FRAMEWORK_ID","value":"9c35b61d-8d30-442e-984f-5a3dc9d4d
  499-0000"},{"name":"MESOS_HTTP_COMMAND_EXECUTOR","value":"0"},{"name":"MESOS_SLAVE_ID","value":"9c35
  b61d-8d30-442e-984f-5a3dc9d4d499-S0"},{"name":"MESOS_SLAVE_PID","value":"slave(1)@172.23.133.32:5051"},
  {"name":"MESOS_SANDBOX","value":".\/work-mesos\/slaves\/9c35b61d-8d30-442e-984f-5a3dc9d4d499-S0\
  /frameworks\/9c35b61d-8d30-442e-984f-5a3dc9d4d499-0000\/executors\/default\/runs\/c2525747-a3fb-490
  d-afef-357dfcd7c186"}]},"err":{"path":".\/work-mesos\/slaves\/9c35b61d-8d30-442e-984f-5a3dc9d4d499-S
  0\/frameworks\/9c35b61d-8d30-442e-984f-5a3dc9d4d499-0000\/executors\/default\/runs\/c2525747-a3fb-4
  90d-afef-357dfcd7c186\/stderr","type":"PATH"},"out":{"path":".\/work-mesos\/slaves\/9c35b61d-8d30-4
  42e-984f-5a3dc9d4d499-S0\/frameworks\/9c35b61d-8d30-442e-984f-5a3dc9d4d499-0000\/executors\/default
  \/runs\/c2525747-a3fb-490d-afef-357dfcd7c186\/stdout","type":"PATH"},"user":"root","working_directo
  ry":".\/work-mesos\/slaves\/9c35b61d-8d30-442e-984f-5a3dc9d4d499-S0\/frameworks\/9c35b61d-8d30-442e
  -984f-5a3dc9d4d499-0000\/executors\/default\/runs\/c2525747-a3fb-490d-afef-357dfcd7c186"}" --pipe_read
  ="8" --pipe_write="9" --runtime_directory="/var/run/mesos/containers/c2525747-a3fb-490d-afef-357dfcd7c186" 
  --unshare_namespace_mnt="false"'

  */
  struct Flags : public virtual flags::FlagsBase   //executor在int MesosContainerizerLaunch::execute()中调用执行
  {
    Flags();

	//mesos-containerizer launch和mesos-agent进程通过pipe通信，见MesosContainerizerProcess::_launch
	//lauch子进程会直接继承该info,也就可以直接使用
	//LinuxLauncherProcess::fork中运行mesos-containerizer launch进程
    Option<JSON::Object> launch_info; //MesosContainerizerLaunch::execute执行    
#ifdef __WINDOWS__
    Option<HANDLE> pipe_read;
    Option<HANDLE> pipe_write;
#else
     //mesos-containerizer launch和mesos-agent进程通过pipe()通信，见MesosContainerizerProcess::_launch
    Option<int> pipe_read;
    Option<int> pipe_write;
    Option<std::string> runtime_directory;
#endif // __WINDOWS__
#ifdef __linux__
    Option<pid_t> namespace_mnt_target;
    bool unshare_namespace_mnt;
#endif // __linux__
  };

  MesosContainerizerLaunch() : Subcommand(NAME) {}

  Flags flags;

protected:
  virtual int execute();
  virtual flags::FlagsBase* getFlags() { return &flags; }
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __MESOS_CONTAINERIZER_LAUNCH_HPP__
