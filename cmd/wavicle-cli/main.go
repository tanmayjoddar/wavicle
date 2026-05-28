package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	port := flag.String("p", "6379", "Wavicle server port")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: wavicle-cli [-p port] <command> [args...]\n")
		fmt.Fprintf(os.Stderr, "\nCommands:\n")
		fmt.Fprintf(os.Stderr, "  SET key value\n")
		fmt.Fprintf(os.Stderr, "  GET key\n")
		fmt.Fprintf(os.Stderr, "  DEL key [keys...]\n")
		fmt.Fprintf(os.Stderr, "  EXISTS key [keys...]\n")
		fmt.Fprintf(os.Stderr, "  DBSIZE\n")
		fmt.Fprintf(os.Stderr, "  PING\n")
		os.Exit(1)
	}

	conn, err := net.Dial("tcp", "localhost:"+*port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: cannot connect to Wavicle at localhost:%s — %v\n", *port, err)
		os.Exit(1)
	}
	defer conn.Close()

	// Send as inline RESP3 command (space-separated, \r\n terminated)
	_, err = fmt.Fprintf(conn, "%s\r\n", strings.Join(args, " "))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to send command — %v\n", err)
		os.Exit(1)
	}

	// Read and format the response
	err = printResponse(conn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to read response — %v\n", err)
		os.Exit(1)
	}
}

func printResponse(r io.Reader) error {
	br := bufio.NewReader(r)

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
			fmt.Println(line[1:])

		case '-': // Error
			fmt.Fprintln(os.Stderr, "(error)", line[1:])

		case ':': // Integer
			fmt.Printf("(integer) %s\n", line[1:])

		case '$': // Bulk string
			length, err := strconv.Atoi(line[1:])
			if err != nil {
				return fmt.Errorf("invalid bulk string length: %s", line[1:])
			}
			if length == -1 {
				fmt.Println("(nil)")
				continue
			}
			if length == 0 {
				fmt.Println(`""`)
				continue
			}
			data := make([]byte, length+2) // +2 for \r\n
			if _, err := io.ReadFull(br, data); err != nil {
				return err
			}
			fmt.Println(string(data[:length]))

		case '*': // Array
			count, err := strconv.Atoi(line[1:])
			if err != nil {
				return fmt.Errorf("invalid array length: %s", line[1:])
			}
			if count == -1 {
				fmt.Println("(nil)")
				continue
			}
			if count == 0 {
				fmt.Println("(empty array)")
				continue
			}
			// Array elements follow — recurse for each
			for i := 0; i < count; i++ {
				elemLine, err := br.ReadString('\n')
				if err != nil {
					return err
				}
				elemLine = strings.TrimRight(elemLine, "\r\n")
				fmt.Printf("%d) ", i+1)
				if len(elemLine) > 0 && elemLine[0] == '$' {
					elemLen, _ := strconv.Atoi(elemLine[1:])
					if elemLen == -1 {
						fmt.Println("(nil)")
						continue
					}
					data := make([]byte, elemLen+2)
					io.ReadFull(br, data)
					fmt.Println(string(data[:elemLen]))
				} else {
					fmt.Println(elemLine)
				}
			}
			return nil

		default:
			fmt.Println(line)
		}

		// Single response — stop after first complete message
		// (for simple strings, integers, errors; bulk strings and arrays handle themselves)
		if line[0] != '$' && line[0] != '*' {
			return nil
		}
		if line[0] == '*' {
			return nil // array handler already printed everything
		}
		// For bulk strings, we already read the data, so we're done
		return nil
	}
}
