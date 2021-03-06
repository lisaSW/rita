package resources

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/activecm/rita/config"
	"github.com/activecm/rita/util"
	"github.com/rifflock/lfshook"
)

// initLogger creates the logger for logging to stdout and file
func initLogger(logConfig *config.LogStaticCfg) *log.Logger {
	var logs = &log.Logger{}

	logs.Formatter = new(log.TextFormatter)

	logs.Out = ioutil.Discard
	logs.Hooks = make(log.LevelHooks)

	switch logConfig.LogLevel {
	case 3:
		logs.Level = log.DebugLevel
		break
	case 2:
		logs.Level = log.InfoLevel
		break
	case 1:
		logs.Level = log.WarnLevel
		break
	case 0:
		logs.Level = log.ErrorLevel
	}
	if logConfig.LogToFile {
		addFileLogger(logs, logConfig.RitaLogPath)
	}
	return logs
}

func addFileLogger(logger *log.Logger, logPath string) {
	time := time.Now().Format(util.TimeFormat)
	logPath = path.Join(logPath, time)
	_, err := os.Stat(logPath)
	if err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(logPath, 0755)
		if err != nil {
			fmt.Println("[!] Could not initialize file logger. Check RitaLogDir.")
			return
		}
	}

	logger.Hooks.Add(lfshook.NewHook(lfshook.PathMap{
		log.DebugLevel: path.Join(logPath, "debug.log"),
		log.InfoLevel:  path.Join(logPath, "info.log"),
		log.WarnLevel:  path.Join(logPath, "warn.log"),
		log.ErrorLevel: path.Join(logPath, "error.log"),
		log.FatalLevel: path.Join(logPath, "fatal.log"),
		log.PanicLevel: path.Join(logPath, "panic.log"),
	}, nil))
}
