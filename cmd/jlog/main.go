package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"syscall"
	_ "time/tzdata"

	"github.com/jessevdk/go-flags"
	"github.com/jrockway/json-logs/cmd/internal/jlog"
	"github.com/jrockway/json-logs/pkg/parse"
	"github.com/mattn/go-colorable"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
	builtBy = "unknown"
)

func printVersion(w io.Writer) {
	fmt.Fprintf(w, "jlog - Search and pretty-print your JSON logs.\nMore info: https://github.com/jrockway/json-logs\n")
	fmt.Fprintf(w, "Version %s (%s) built on %s by %s\n", version, commit, date, builtBy)
	if buildinfo, ok := debug.ReadBuildInfo(); ok {
		fmt.Fprintf(w, "    go: %v\n", buildinfo.GoVersion)
		for _, x := range buildinfo.Settings {
			fmt.Fprintf(w, "    %v: %v\n", x.Key, x.Value)
		}
	}
}

func main() {
	var gen jlog.General
	var in jlog.Input
	var out jlog.Output
	fp := flags.NewParser(nil, flags.HelpFlag|flags.PassDoubleDash)
	if _, err := fp.AddGroup("Input Schema", "", &in); err != nil {
		panic(err)
	}
	if _, err := fp.AddGroup("Output Format", "foo", &out); err != nil {
		panic(err)
	}
	if _, err := fp.AddGroup("General", "bar", &gen); err != nil {
		panic(err)
	}

	extraArgs, err := fp.Parse()
	if err != nil {
		if ferr, ok := err.(*flags.Error); ok && ferr.Type == flags.ErrHelp {
			printVersion(os.Stderr)
			fmt.Fprintf(os.Stderr, ferr.Message)
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "flag parsing: %v\n", err)
		os.Exit(3)
	}
	if len(extraArgs) > 0 {
		fmt.Fprintf(os.Stderr, "unexpected command-line arguments after flag parsing: %v\n", extraArgs)
		os.Exit(1)
	}
	if gen.Version {
		printVersion(os.Stdout)
		os.Exit(0)
	}

	ins, err := jlog.NewInputSchema(in)
	if err != nil {
		fmt.Fprintf(os.Stderr, "problem creating input schema: %v\n", err)
		os.Exit(1)
	}

	outs, err := jlog.NewOutputFormatter(out, gen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "problem creating output formatter: %v\n", err)
		os.Exit(1)
	}

	fsch, err := jlog.NewFilterScheme(gen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "problem creating filters: %v\n", err)
		os.Exit(1)
	}

	var f *os.File
	if gen.Profile != "" {
		var err error
		f, err = os.Create(gen.Profile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not create CPU profile: %v\n", err)
			os.Exit(1)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "could not start CPU profile: %v\n", err)
			os.Exit(1)
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGPIPE)
	var nSignals int32
	go func() {
		c := <-sigCh
		atomic.AddInt32(&nSignals, 1)
		fmt.Fprintf(os.Stderr, "signal: %v\n", c.String())
		os.Stdin.Close()
		signal.Stop(sigCh)
	}()

	summary, err := parse.ReadLog(os.Stdin, colorable.NewColorableStdout(), ins, outs, fsch)
	if err != nil {
		if signals := atomic.LoadInt32(&nSignals); signals < 1 || !strings.Contains(err.Error(), "file already closed") {
			outs.EmitError(err.Error())
		}
	}
	jlog.PrintOutputSummary(out, summary, os.Stderr)

	if f != nil {
		pprof.StopCPUProfile()
		if err := f.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write CPU profile: %v\n", err)
		}
	}
	if errors.Is(err, syscall.EPIPE) {
		os.Exit(2)
	} else if err != nil {
		os.Exit(1)
	}
}
