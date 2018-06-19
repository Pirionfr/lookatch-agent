package main

import (
	"fmt"
	"github.com/spf13/cobra"
)

var (
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
)

func init() {
	app.AddCommand(agentCmd, statusCmd)
	app.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $PWD/config.json)")
	app.PersistentFlags().StringVarP(&cfgPath, "configPath", "p", "", "config file path (default is $PWD)")
}
