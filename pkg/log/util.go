// Copyright 2016 DeepFabric, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"flag"
)

var (
	crashLog = flag.String("crash", "./crash.log", "The crash log file.")
	logFile  = flag.String("log-file", "", "The external log file. Default log to console.")
	logLevel = flag.String("log-level", "info", "The log level, default is info")
)

// Cfg is the log cfg
type Cfg struct {
	LogLevel string
	LogFile  string
}

// InitLog init log
func InitLog() {
	if !flag.Parsed() {
		flag.Parse()
	}

	//CrashLog(*crashLog)
	SetRotateByHour()
	SetHighlighting(false)
	SetLevelByString(*logLevel)
	if "" != *logFile {
		SetOutputByName(*logFile)
	}

	if !DebugEnabled() {
		SetFlags(Ldate | Ltime)
	}
}
