package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var byeCmd = &cobra.Command{
	Use:   "bye",
	Short: "says bye",
	Long:  `This subcommand says bye`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("bye called")
	},
}

func init() {
	rootCmd.AddCommand(byeCmd)
}
