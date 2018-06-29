package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"runtime"
)

var (
	version = "0.0.0"
	githash = "HEAD"
	date    = "1970-01-01T00:00:00Z UTC"

	app = &cobra.Command{
		Use:   "LookatchAgent",
		Short: "LookatchAgent short...",
		Long:  "Long version...",
	}

	agentCmd = &cobra.Command{
		Use:   "run",
		Short: "run agent",
		Long:  `run an instance of an agent.`,
		Run: func(cmd *cobra.Command, args []string) {
			runAgent(cmd, args)
		},
	}

	statusCmd = &cobra.Command{
		Use:   "status",
		Short: "get status",
		Long:  `get metrics and status for the running instance. `,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("todo")
		},
	}

	versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Show version",
		Run: func(cmd *cobra.Command, arguments []string) {
			fmt.Printf("lookatch-agent version %s %s\n", version, githash)
			fmt.Printf("lookatch-agent build date %s\n", date)
			fmt.Printf("go version %s %s/%s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH)
		},
	}
)

func init() {
	app.AddCommand(agentCmd, statusCmd, versionCmd)
	app.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $PWD/config.json)")
	app.PersistentFlags().StringVarP(&cfgPath, "configPath", "p", "", "config file path (default is $PWD)")
}
