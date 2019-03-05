package main

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/midbel/cli"
)

const (
	TimeFormat    = "2006-01-02"
	Day           = time.Hour * 24
	DefaultPeriod = 7
)

var (
	UNIX = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	GPS  = time.Date(1980, 1, 6, 0, 0, 0, 0, time.UTC)
)

const helpText = `{{.Name}} scan the Hadock archive and produces report about
its status such as:

  * total and uniq files
  * total corrupted files
  * status of missing files

Usage:

  {{.Name}} command [arguments]

The commands are:

{{range .Commands}}{{if .Runnable}}{{printf "  %-12s %s" .String .Short}}{{if .Alias}} (alias: {{ join .Alias ", "}}){{end}}{{end}}
{{end}}

Use {{.Name}} [command] -h for more information about its usage.
`

var commands = []*cli.Command{
	checkUPICommand,
	checkSourceCommand,
	digestCommand,
	srvCommand,
	walkCommand,
	pushCommand,
	inspectCommand,
}

func init() {
	log.SetFlags(0)
	log.SetOutput(os.Stdout)

	cli.Version = "0.6.0"
	cli.BuildTime = "2019-02-27 08:20:00"
}

func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Fatalf("unexpected error: %s", err)
		}
	}()
	usage := func() {
		data := struct {
			Name     string
			Commands []*cli.Command
		}{
			Name:     filepath.Base(os.Args[0]),
			Commands: commands,
		}
		fs := map[string]interface{}{
			"join": strings.Join,
		}
		t := template.Must(template.New("help").Funcs(fs).Parse(helpText))
		t.Execute(os.Stderr, data)

		os.Exit(2)
	}
	if err := cli.Run(commands, usage, nil); err != nil {
		log.Fatalln(err)
	}
}
