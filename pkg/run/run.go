package run

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	apiv3 "github.com/projectcalico/libcalico-go/lib/apis/v3"
	logsettings "github.com/projectcalico/libcalico-go/lib/logsettings"

	"github.com/kelseyhightower/confd/pkg/backends/calico"
	"github.com/kelseyhightower/confd/pkg/config"
	"github.com/kelseyhightower/confd/pkg/resource/template"
	log "github.com/sirupsen/logrus"
)

func DebuggingConfigurationHandler(logLevel apiv3.LogLevel, client interface{}) {
	log.Infof("Detected a default DebuggingConfiguration change. Processing it.", logLevel)

	// Currently log severity in DebuggingConfiguration can only be set to Info or Debug.
	// Setting it to Info or not setting at all means fall back to other mechanism.
	// Setting it to Debug has priority over anything else.
	if logLevel == apiv3.LogLevelDebug {
		log.Info("Set log severity to Debug")
		if err := os.Setenv(calico.DEBUGGING_CONFIGURATION_LOG_LEVEL, "DEBUG"); err != nil {
			log.Info("failed to set environment variable ", err)
		}
		log.SetLevel(log.DebugLevel)
		calico.TriggerLogLevelSetting(client)
	} else {
		os.Unsetenv(calico.DEBUGGING_CONFIGURATION_LOG_LEVEL)
		log.Info("Set log severity based on UpdateLogLevel")
		calico.TriggerLogLevelSetting(client)
	}
}

func Run(config *config.Config) {
	log.Info("Starting calico-confd")
	storeClient, err := calico.NewCalicoClient(config)
	if err != nil {
		log.Fatal(err.Error())
	}

	templateConfig := template.Config{
		ConfDir:       config.ConfDir,
		ConfigDir:     filepath.Join(config.ConfDir, "conf.d"),
		KeepStageFile: config.KeepStageFile,
		Noop:          config.Noop,
		Prefix:        config.Prefix,
		SyncOnly:      config.SyncOnly,
		TemplateDir:   filepath.Join(config.ConfDir, "templates"),
		StoreClient:   storeClient,
	}
	if config.Onetime {
		if err := template.Process(templateConfig); err != nil {
			log.Fatal(err.Error())
		}
		os.Exit(0)
	}

	stopChan := make(chan bool)
	doneChan := make(chan bool)
	errChan := make(chan error, 10)

	processor := template.WatchProcessor(templateConfig, stopChan, doneChan, errChan)
	go processor.Process()

	client, err := calico.NewClient(config)
	if err != nil {
		log.Fatal(err.Error())
	}
	ctx := context.Background()
	nodeName := os.Getenv("NODENAME")
	logsettings.RegisterForLogSettings(ctx, client, apiv3.ComponentCalicoNode, nodeName,
		DebuggingConfigurationHandler, storeClient)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case err := <-errChan:
			log.Error(err.Error())
		case s := <-signalChan:
			log.Info(fmt.Sprintf("Captured %v. Exiting...", s))
			close(doneChan)
		case <-doneChan:
			os.Exit(0)
		}
	}
}
