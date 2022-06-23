package parse

type context struct {
	Before, After int

	lines      []line
	printAfter int
	line       int
	lastPrint  int
}

// Print returns the lines that should be displayed right now, based on the line that is being
// added, its filtering status, and the context configuration.
func (c *context) Print(msg *line, selected bool) []*line {
	c.line++
	if selected {
		var result []*line
		c.printAfter = c.After

		if true &&
			// suppress separator if it's the first line of output
			c.lastPrint != 0 &&
			// suppress separator if we are no-op context
			(c.After != 0 || c.Before != 0) &&
			// suppress separator if end of after is contiguous with the start of before
			c.line-len(c.lines)-c.lastPrint > 1 {
			result = append(result, &line{isSeparator: true})
		}
		for _, l := range c.lines {
			line := l
			result = append(result, &line)
		}
		result = append(result, msg)
		c.lastPrint = c.line
		c.lines = nil // TODO: allocate full capacity here
		return result
	}

	if c.printAfter > 0 {
		c.lastPrint = c.line
		c.printAfter--
		return []*line{msg}
	}

	if c.Before > 0 {
		// TODO: allocate full capacity here
		c.lines = append(c.lines, *msg) // shallow copy
		if len(c.lines) > c.Before {
			c.lines = c.lines[1:]
		}
	}
	return nil
}
