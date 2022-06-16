# json-logs

This repository contains a tool, `jlog`, to pretty-print JSON logs, like those from zap or logrus.

Here's what it looks like in action (on some logs from an
[opinionated-server server](https://github.com/jrockway/opinionated-server)).

![Screenshot](https://user-images.githubusercontent.com/2367/147866806-aa5c68c3-f5ba-4f58-884d-4372986868b9.PNG)

The main goal is to never hide any information in the underlying logs, even if a particular line
doesn't parse. Unparsable lines or lines with missing information will be echoed verbatim, and at
the very end, a count is produced summarizing lines that parsed, lines that didn't parse, and lines
that were filtered out with a user expression (`-e`). That way, you know you're seeing everything,
even when an annoying library writes plain text to your JSON logs. These guarantees are tested with
fuzz tests and live integration tests against common logging libraries.

## Installation

Grab a binary from the releases area, `chmod a+x` it, and move it to somewhere in your $PATH. Or you
can `go install github.com/jrockway/json-logs/cmd/jlog@latest`.

## Use

Pipe it some json-formatted logs:

    $ jlog < log
    $ kubectl logs some-pod | jlog
    # etc.

The format is automatically guessed, and timestamps will appear in your local time zone.

Here's the `--help` message:

    jlog [OPTIONS]

    Input Schema:
      -l, --lax              If true, suppress any validation errors including non-JSON log lines and missing timestamps, levels, and message.  We extract as many of those as we can, but if something is missing,
                             the errors will be silently discarded. [$JLOG_LAX]
          --levelkey=        JSON key that holds the log level. [$JLOG_LEVEL_KEY]
          --nolevelkey       If set, don't look for a log level, and don't display levels. [$JLOG_NO_LEVEL_KEY]
          --timekey=         JSON key that holds the log timestamp. [$JLOG_TIMESTAMP_KEY]
          --notimekey        If set, don't look for a time, and don't display times. [$JLOG_NO_TIMESTAMP_KEY]
          --messagekey=      JSON key that holds the log message. [$JLOG_MESSAGE_KEY]
          --notimekey        If set, don't look for a time, and don't display times. [$JLOG_NO_TIMESTAMP_KEY]
          --delete=          JSON keys to be deleted before JQ processing and output; repeatable. [$JLOG_DELETE_KEYS]
          --upgrade=         JSON key (of type object) whose fields should be merged with any other fields; good for loggers that always put structed data in a separate key; repeatable.
                             --upgrade b would transform as follows: {a:'a', b:{'c':'c'}} -> {a:'a', c:'c'} [$JLOG_UPGRADE_KEYS]

    Output Format:
          --no-elide         Disable eliding repeated fields.  By default, fields that have the same value as the line above them have their values replaced with '↑'. [$JLOG_NO_ELIDE_DUPLICATES]
      -r, --relative         Print timestamps as a duration since the program started instead of absolute timestamps. [$JLOG_RELATIVE_TIMESTAMPS]
      -t, --time-format=     A go time.Format string describing how to format timestamps, or one of 'rfc3339(milli|micro|nano)', 'unix', 'stamp(milli|micro|nano)', or 'kitchen'. (default: stamp)
                             [$JLOG_TIME_FORMAT]
      -s, --only-subseconds  Display only the fractional part of times that are in the same second as the last log line.  Only works with the (milli|micro|nano) formats above.  (This can be revisited, but it's
                             complicated.) [$JLOG_ONLY_SUBSECONDS]
          --no-summary       Suppress printing the summary at the end. [$JLOG_NO_SUMMARY]
      -p, --priority=        A list of fields to show first; repeatable. [$JLOG_PRIORITY_FIELDS]
      -H, --highlight=       A list of fields to visually distinguish; repeatable. (default: err, error, warn, warning) [$JLOG_HIGHLIGHT_FIELDS]

    General:
      -e=                    A jq program to run on each record in the processed input; use this to ignore certain lines, add fields, etc.  Hint: 'select(condition)' will remove lines that don't match 'condition'.
      -M, --no-color         Disable the use of color. [$JLOG_FORCE_MONOCHROME]
      -C, --no-monochrome    Force the use of color. Note: the short flag will change in a future release.
      --profile=             If set, collect a CPU profile and write it to this file.
      -v, --version          Print version information and exit.

    Help Options:
      -h, --help             Show this help message

All options can be set as environment variables; if there's something you use every time you invoke
it, just set it up in your shell's init file.

### Input

`--levelkey`, `--timekey`, and `--messagekey` will allow jlog to handle log formats it's not yet
taught to recognize. If your JSON log uses `foo` as the level, `bar` as the time, and `baz` as the
message, like: `{"foo":"info", "bar":"2022-01-01T00:00:00.123", "baz":"information!"}`, then
`jlog --levelkey=foo --timekey=bar --messagekey=baz` will allow jlog to properly format those logs.

There is some logic to guess the log format based on the first line. If this yields incorrect
results, file a bug, but setting any of `--levelkey`, `--timekey`, or `--messagekey` will completely
disable auto-guessing.

Some loggers put all structured data into one key; you can merge that key's values into the main set
of fields with `--upgrade <key>`. This makes eliding of repeated fields work for that log format.
Logs that look like they were produced by a known library that does this are automatically upgraded.

Some loggers output schema format information with each log message. You can delete keys like this
with `--delete <key>`. Logs that look that look like they were produced by a known library that does
this automatically have that key deleted. (You can do this with `del(.key)` in a JQ program, as
well.)

## Output

There are many options to control the output format. You can output timestamps in your favorite
format with `-t XXX`, where XXX is one of the options listed above or any
[go time format string](https://pkg.go.dev/time#pkg-constants).

If you want to distinguish events that happened in the same second as the previous line, use `-s`.
It will turn output like:

    2022-01-01T00:00:00.000 DEBUG this is a debug message
    2022-01-01T00:00:00.123 DEBUG this is another debug message
    2022-01-01T00:00:01.000 DEBUG this is the last debug message

Into:

    2022-01-01T00:00:00.000 DEBUG this is a debug message
                       .123 DEBUG this is another debug message
    2022-01-01T00:00:01.000 DEBUG this is the last debug message

This can sometimes make spammy logs a little easier on the eyes.

You can pass `-r` to see the time difference between when the program started and the log line. This
is good if you don't want to do any mental math.

You can adjust the output timezone with the `TZ` environment variable. `TZ=America/Los_Angeles jlog`
will print times in Pacific, for example.

`-p` Will ensure that if a named field is present, it will appear immediately after the message.

`-H` Will highlight the named field in a different color. `-H error` is nice for locating errors at
a glance.

## Filtering

You can pass a [jq](https://stedolan.github.io/jq/) program to process the input. Something like
`jlog -e 'select($LVL>$INFO)'` will only show logs with a level greater than info. Something like
`jlog -e 'select($MSG | test("foo"))'` will only show messages that contain "foo" (even if a field
contains foo). You can of course access any field in the parsed JSON log line and make selection
decisions on that, or delete fields, or add new fields.

The JQ program is run after schema detection and validation.

## Highlighting

The built-in jq function `highlight` will caused matched messages to display in inverse-video
`jlog -e 'highlight(.foo == 42)'` would highlight any message where the `foo` key equals 42.
`jlog -e 'highlight($MSG|test("abc"))'` would highlight any message that contains `"abc"`.
