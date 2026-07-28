package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/labring/messagequeue/controller"
	v1alpha1 "github.com/labring/messagequeue/controller/api/v1alpha1"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	ctrlmetricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var scheme = runtime.NewScheme()

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
}

func main() {
	var metricsAddr string
	var probeAddr string
	var leaderElect bool
	var logLevel string
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&leaderElect, "leader-elect", true, "Enable leader election for the controller manager.")
	flag.StringVar(&logLevel, "log-level", "info", "Controller log level: debug, info, warn, error, dpanic, panic, or fatal.")
	flag.Parse()

	level := zapcore.InfoLevel
	if err := level.UnmarshalText([]byte(logLevel)); err != nil {
		fmt.Fprintf(os.Stderr, "invalid --log-level %q: %v\n", logLevel, err)
		os.Exit(2)
	}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zap.Options{Development: false}), zap.Level(level)))
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                ctrlmetricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "messagequeue-controller",
	})
	if err != nil {
		os.Exit(1)
	}
	if err := (&controller.MessageQueueReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		os.Exit(1)
	}
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		os.Exit(1)
	}
}
