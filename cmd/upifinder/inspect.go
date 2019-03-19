package main

import (
	"fmt"
	"os"
	"sort"
	"text/template"
	"time"

	"github.com/midbel/cli"
)

var inspectCommand = &cli.Command{
	Usage: "inspect [-d] [-s] [-e] [-u] [-f] <archive,...>",
	Run:   runInspect,
}

const detail = `
{{.UPI | pretty}} ({{.Starts | strftime}} - {{.Ends | strftime}})

- Size  : {{.Size | size}}
- Total : {{.Total}}
- First : {{.First}}
- Last  : {{.Last}}
{{- with $rs := .Ranges}}
- Ranges: {{ len $rs}}
{{- range $ix, $v := $rs}}
-- {{$ix | increment}}: {{$v.First}} -> {{$v.Last}} (total: {{$v.Total | add}})
{{- end}}
{{- end}}
{{- with $gaps := .MissingRanges}}
{{- if $gaps}}
- Gaps : {{ len $gaps}}
{{- range $ix, $v := $gaps}}
-- {{$ix | increment}}: {{$v.First}} -> {{$v.Last}} (missing: {{$v.Total | minus}})
{{- end}}
{{- end}}
{{- end}}
`

func runInspect(cmd *cli.Command, args []string) error {
	var start, end When
	cmd.Flag.Var(&start, "s", "start")
	cmd.Flag.Var(&end, "e", "end")
	upi := cmd.Flag.String("u", "", "upi")
	period := cmd.Flag.Int("d", 0, "period")
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}

	if cmd.Flag.NArg() == 0 {
		cmd.Help()
	}
	fs := template.FuncMap{
		"increment": func(i int) int {
			return i + 1
		},
		"strftime": func(t time.Time) string {
			return t.Format("2006-01-02 15:04:05")
		},
		"add": func(i uint32) uint32 {
			return i + 1
		},
		"minus": func(i uint32) uint32 {
			return i - 1
		},
		"size":   prettySize,
		"pretty": Transform,
	}
	t, err := template.New("debug").Funcs(fs).Parse(detail)
	if err != nil {
		return err
	}

	paths, err := listPaths(cmd.Flag.Args(), *period, start.Time, end.Time)
	if err != nil {
		return err
	}

	rs := countFiles(walkFiles(paths, *upi, 8))
	if len(rs) == 0 {
		return nil
	}
	var vs []string
	for n := range rs {
		vs = append(vs, n)
	}
	sort.Strings(vs)

	for i, n := range vs {
		c := rs[n]
		first, last := c.Range()
		v := struct {
			*Coze
			First uint32
			Last  uint32
		}{Coze: c, First: first, Last: last}
		if err := t.ExecuteTemplate(os.Stdout, "debug", v); err != nil {
			return err
		}
		fmt.Println()
		if i < len(vs)-1 {
			fmt.Println("===")
		}
	}
	return nil
}
