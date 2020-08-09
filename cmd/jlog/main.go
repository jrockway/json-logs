package main

import (
	"fmt"
	"os"

	"github.com/jessevdk/go-flags"
	"github.com/jrockway/json-logs/pkg/parse"
)

type inputFormat struct {
	TimestampKey string `long:"timekey" short:"t" default:"ts" description:"JSON key that holds the log timestamp"`
	MessageKey   string `long:"msgkey" short:"m" default:"msg" description:"JSON key that holds the log message"`
}

func main() {
	var inf inputFormat
	fp := flags.NewParser(nil, flags.HelpFlag|flags.PassDoubleDash)
	if _, err := fp.AddGroup("Input Schema", "", &inf); err != nil {
		panic(err)
	}
	if _, err := fp.Parse(); err != nil {
		if ferr, ok := err.(*flags.Error); ok && ferr.Type == flags.ErrHelp {
			fmt.Fprintf(os.Stderr, ferr.Message)
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "flag parsing: %v\n", err)
		os.Exit(3)
	}
	ins := &parse.InputSchema{
		MessageKey:   inf.MessageKey,
		TimeKey:      inf.TimestampKey,
		TimeFormat:   parse.DefaultTimeParserFn,
		StrictObject: true,
	}
	outs := &parse.OutputSchema{
		LevelKey: "level",
	}
	if err := parse.ReadLog(os.Stdin, os.Stdout, ins, outs); err != nil {
		outs.EmitError(err.Error())
	}
}
