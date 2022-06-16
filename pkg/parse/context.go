package parse

import (
	"bytes"
)

type context struct {
	Before, After int

	lines      []string
	printAfter int
	line       int
	lastPrint  int
}

func (c *context) Print(buf *bytes.Buffer, msg string, selected bool) {
	c.line++
	if selected {
		c.printAfter = c.After
		if c.lastPrint != 0 && (c.After != 0 || c.Before != 0) && c.line-len(c.lines)-c.lastPrint > 1 {
			buf.WriteString("---\n")
		}
		for _, l := range c.lines {
			buf.WriteString(l)
		}
		buf.WriteString(msg)
		c.lastPrint = c.line
		c.lines = nil
		return
	}

	if c.printAfter > 0 {
		buf.WriteString(msg)
		c.lastPrint = c.line
		c.printAfter--
		return
	}

	if c.Before > 0 {
		c.lines = append(c.lines, msg)
		if len(c.lines) > c.Before {
			c.lines = c.lines[1:]
		}
	}
}
