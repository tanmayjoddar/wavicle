package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
)

const version = "1.0.0"

func main() {
	addr := flag.String("p", "6379", "Server address (port, or host:port)")
	showHelp := flag.Bool("help", false, "Show help")
	showVersion := flag.Bool("version", false, "Show version")
	flag.Parse()

	if *showHelp {
		printHelp()
		os.Exit(0)
	}
	if *showVersion {
		fmt.Printf("wavicle-cli %s\n", version)
		os.Exit(0)
	}

	// Resolve address: if flag contains ":", use as-is; otherwise prepend localhost:
	serverAddr := *addr
	if !strings.Contains(serverAddr, ":") {
		serverAddr = "localhost:" + serverAddr
	}

	args := flag.Args()

	// REPL mode: no command arguments
	if len(args) == 0 {
		runREPL(serverAddr)
		return
	}

	// Single command mode
	err := runCommand(serverAddr, strings.Join(args, " "))
	if err != nil {
		fmt.Fprintf(os.Stderr, "(error) %v\n", err)
		os.Exit(1)
	}
}

func runCommand(addr, line string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("cannot connect to Wavicle at %s — %v", addr, err)
	}
	defer conn.Close()

	_, err = fmt.Fprintf(conn, "%s\r\n", line)
	if err != nil {
		return fmt.Errorf("send failed — %v", err)
	}

	return printResponse(conn, os.Stdout)
}

func runREPL(addr string) {
	fmt.Fprintf(os.Stderr, "wavicle-cli %s\n", version)
	fmt.Fprintf(os.Stderr, "Type \"exit\" or \"quit\" to quit, \"help\" for commands.\n")
	fmt.Fprintf(os.Stderr, "Connecting to %s...\n", addr)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "(error) cannot connect to Wavicle at %s — %v\n", addr, err)
		fmt.Fprintf(os.Stderr, "Start the server with:  wavicle\n")
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Fprintf(os.Stderr, "Connected.\n")

	// Handle Ctrl+C gracefully
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\nBye.")
		conn.Close()
		os.Exit(0)
	}()

	scanner := bufio.NewScanner(os.Stdin)
	br := bufio.NewReader(conn)

	fmt.Fprintf(os.Stderr, "\n")

	for {
		fmt.Fprintf(os.Stderr, "wavicle> ")

		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Built-in REPL commands
		switch strings.ToLower(line) {
		case "exit", "quit":
			fmt.Fprintln(os.Stderr, "Bye.")
			return
		case "help":
			printREPLHelp()
			continue
		}

		// Send command to server
		_, err := fmt.Fprintf(conn, "%s\r\n", line)
		if err != nil {
			fmt.Fprintf(os.Stderr, "(error) %v\n", err)
			return
		}

		// Read and display response
		err = printResponse(br, os.Stdout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "(error) %v\n", err)
			return
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "(error) %v\n", err)
		os.Exit(1)
	}
}

func printResponse(r io.Reader, w io.Writer) error {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}

	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			continue
		}

		switch line[0] {
		case '+': // Simple string
			fmt.Fprintln(w, line[1:])

		case '-': // Error
			fmt.Fprintf(w, "(error) %s\n", line[1:])

		case ':': // Integer
			fmt.Fprintf(w, "(integer) %s\n", line[1:])

		case '$': // Bulk string
			if err := printBulkString(br, w, line); err != nil {
				return err
			}
			return nil

		case '*': // Array
			if err := printArray(br, w, line); err != nil {
				return err
			}
			return nil

		case '%': // Map (RESP3)
			if err := printMap(br, w, line); err != nil {
				return err
			}
			return nil

		case '~': // Set (RESP3)
			if err := printArray(br, w, line); err != nil {
				return err
			}
			return nil

		case '>': // Push (RESP3, for pub/sub)
			if err := printArray(br, w, line); err != nil {
				return err
			}
			return nil

		case '!': // Blob error (RESP3)
			if err := printBulkString(br, w, line); err != nil {
				return err
			}
			return nil

		case '=': // Verbatim string (RESP3)
			if err := printBulkString(br, w, line); err != nil {
				return err
			}
			return nil

		case '_': // Null
			fmt.Fprintln(w, "(nil)")

		case '#': // Boolean
			if line[1:] == "t" {
				fmt.Fprintln(w, "(true)")
			} else {
				fmt.Fprintln(w, "(false)")
			}

		case ',': // Double
			fmt.Fprintln(w, line[1:])

		default:
			fmt.Fprintln(w, line)
		}

		// Single response (non-bulk, non-array, non-map)
		if line[0] != '$' && line[0] != '*' && line[0] != '%' && line[0] != '~' && line[0] != '>' && line[0] != '!' && line[0] != '=' {
			return nil
		}
	}
}

func printBulkString(br *bufio.Reader, w io.Writer, line string) error {
	length, err := strconv.Atoi(line[1:])
	if err != nil {
		return fmt.Errorf("invalid length: %s", line[1:])
	}
	if length == -1 {
		fmt.Fprintln(w, "(nil)")
		return nil
	}
	data := make([]byte, length+2)
	if _, err := io.ReadFull(br, data); err != nil {
		return err
	}
	fmt.Fprintln(w, string(data[:length]))
	return nil
}

func printArray(br *bufio.Reader, w io.Writer, line string) error {
	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return fmt.Errorf("invalid count: %s", line[1:])
	}
	if count == -1 {
		fmt.Fprintln(w, "(nil)")
		return nil
	}
	if count == 0 {
		fmt.Fprintln(w, "(empty)")
		return nil
	}

	for i := 0; i < count; i++ {
		elemLine, err := br.ReadString('\n')
		if err != nil {
			return err
		}
		elemLine = strings.TrimRight(elemLine, "\r\n")
		if elemLine == "" {
			continue
		}

		label := fmt.Sprintf("%d)", i+1)

		switch elemLine[0] {
		case '+':
			fmt.Fprintf(w, "%s %s\n", label, elemLine[1:])
		case ':':
			fmt.Fprintf(w, "%s (integer) %s\n", label, elemLine[1:])
		case '$':
			elemLen, _ := strconv.Atoi(elemLine[1:])
			if elemLen == -1 {
				fmt.Fprintf(w, "%s (nil)\n", label)
				continue
			}
			if elemLen == 0 {
				fmt.Fprintf(w, "%s \"\"\n", label)
				continue
			}
			data := make([]byte, elemLen+2)
			io.ReadFull(br, data)
			fmt.Fprintf(w, "%s %s\n", label, string(data[:elemLen]))
		case '-':
			fmt.Fprintf(w, "%s (error) %s\n", label, elemLine[1:])
		case '*':
			fmt.Fprintf(w, "%s (array)\n", label)
			// Skip nested array contents (we dont recurse for simplicity)
			nestedCount, _ := strconv.Atoi(elemLine[1:])
			for j := 0; j < nestedCount; j++ {
				nestedLine, _ := br.ReadString('\n')
				nestedLine = strings.TrimRight(nestedLine, "\r\n")
				if len(nestedLine) > 0 && nestedLine[0] == '$' {
					nl, _ := strconv.Atoi(nestedLine[1:])
					if nl > 0 {
						skip := make([]byte, nl+2)
						io.ReadFull(br, skip)
					}
				}
			}
		default:
			// Try to read as inline or fallback
			fmt.Fprintf(w, "%s %s\n", label, elemLine)
		}
	}
	return nil
}

func printMap(br *bufio.Reader, w io.Writer, line string) error {
	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return fmt.Errorf("invalid map count: %s", line[1:])
	}
	if count == -1 {
		fmt.Fprintln(w, "(nil)")
		return nil
	}
	if count == 0 {
		fmt.Fprintln(w, "(empty map)")
		return nil
	}

	for i := 0; i < count; i++ {
		// Key
		keyLine, err := br.ReadString('\n')
		if err != nil {
			return err
		}
		keyLine = strings.TrimRight(keyLine, "\r\n")

		key := readRESPValue(br, keyLine)
		if key == "" {
			key = "(nil)"
		}

		// Value
		valLine, err := br.ReadString('\n')
		if err != nil {
			return err
		}
		valLine = strings.TrimRight(valLine, "\r\n")

		val := readRESPValue(br, valLine)
		if val == "" {
			val = "(nil)"
		}

		fmt.Fprintf(w, "%d) %s => %s\n", i+1, key, val)
	}
	return nil
}

func readRESPValue(br *bufio.Reader, line string) string {
	if len(line) == 0 {
		return ""
	}
	switch line[0] {
	case '+':
		return line[1:]
	case ':':
		return line[1:]
	case '$':
		length, err := strconv.Atoi(line[1:])
		if err != nil || length == -1 {
			return "(nil)"
		}
		if length == 0 {
			return "\"\""
		}
		data := make([]byte, length+2)
		io.ReadFull(br, data)
		return string(data[:length])
	case '-':
		return "(error) " + line[1:]
	default:
		return line
	}
}

func printHelp() {
	fmt.Print(`Usage: wavicle-cli [options] [command [args...]]

Options:
  -p <addr>    Server address (default: 6379, or e.g. 192.168.1.5:6379)
  --help       Show this help
  --version    Show version

Commands:
  SET key value         Store a value
  GET key               Retrieve a value
  DEL key [keys...]     Delete one or more keys
  EXISTS key [keys...]  Check if keys exist
  DBSIZE                Count of live keys
  PING                  Health check

If no command is given, wavicle-cli starts in interactive REPL mode.

Examples:
  wavicle-cli SET user:name "Alice"
  wavicle-cli GET user:name
  wavicle-cli -p 6380 PING
  wavicle-cli -p 10.0.0.5:6379 DBSIZE
  wavicle-cli                    (opens interactive shell)
`)
}

func printREPLHelp() {
	fmt.Fprint(os.Stderr, `
Commands:
  SET key value         Store a value
  GET key               Retrieve a value
  DEL key [keys...]     Delete one or more keys
  EXISTS key [keys...]  Check if keys exist
  DBSIZE                Count of live keys
  PING                  Health check

REPL:
  help                  Show this help
  exit, quit            Exit the REPL

`)
}
