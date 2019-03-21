/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package log

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/alipay/sofa-mosn/pkg/buffer"
	"github.com/alipay/sofa-mosn/pkg/types"
	"github.com/hashicorp/go-syslog"
	"runtime"
)

var (
	// localOffset is offset in seconds east of UTC
	_, localOffset = time.Now().Zone()
	// error
	ErrReopenUnsupported = errors.New("reopen unsupported")

	remoteSyslogPrefixes = map[string]string{
		"syslog+tcp://": "tcp",
		"syslog+udp://": "udp",
		"syslog://":     "udp",
	}
)

// Logger is a basic sync logger implement, contains unexported fields
// The Logger Function contains:
// Print(buffer types.IoBuffer, discard bool) error
// Printf(format string, args ...interface{})
// Println(args ...interface{})
// Fatalf(format string, args ...interface{})
// Fatal(args ...interface{})
// Fatalln(args ...interface{})
// Close() error
// Reopen() error
// Toggle(disable bool)
type Logger struct {
	// output is the log's output path
	// if output is empty(""), it is equals to stderr
	output string
	// writer writes the log, created by output
	writer io.Writer
	// roller rotates the log, if the output is a file path
	roller *Roller
	// disable presents the logger state. if disable is true, the logger will write nothing
	// the default value is false
	disable bool
	// implementation elements
	create          time.Time
	reopenChan      chan struct{}
	closeChan       chan struct{}
	writeBufferChan chan types.IoBuffer
}

// loggers keeps all Logger we created
// key is output, same output reference the same Logger
var loggers map[string]*Logger

func init() {
	loggers = make(map[string]*Logger)
}

func GetOrCreateLogger(output string) (*Logger, error) {
	if lg, ok := loggers[output]; ok {
		return lg, nil
	}
	lg := &Logger{
		output:          output,
		roller:          defaultRoller, // TODO: support by configuration
		writeBufferChan: make(chan types.IoBuffer, 1000),
		reopenChan:      make(chan struct{}),
		closeChan:       make(chan struct{}),
		// writer and create will be setted in start()
	}
	err := lg.start()
	if err == nil { // only keeps start success logger
		loggers[output] = lg
	}
	return lg, err
}

func (l *Logger) start() error {
	switch l.output {
	case "", "stderr", "/dev/stderr":
		l.writer = os.Stderr
	case "stdout", "/dev/stdout":
		l.writer = os.Stdout
	case "syslog":
		writer, err := gsyslog.NewLogger(gsyslog.LOG_ERR, "LOCAL0", "mosn")
		if err != nil {
			return err
		}
		l.writer = writer
	default:
		if address := parseSyslogAddress(l.output); address != nil {
			writer, err := gsyslog.DialLogger(address.network, address.address, gsyslog.LOG_ERR, "LOCAL0", "mosn")
			if err != nil {
				return err
			}
			l.writer = writer
		} else { // write to file
			if err := os.MkdirAll(filepath.Dir(l.output), 0755); err != nil {
				return err
			}
			file, err := os.OpenFile(l.output, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
			if err != nil {
				return err
			}
			if l.roller != nil {
				file.Close()
				l.roller.Filename = l.output
				l.writer = l.roller.GetLogWriter()
			} else {
				stat, err := file.Stat()
				if err != nil {
					return err
				}
				l.create = stat.ModTime()
				l.writer = file
			}
		}
	}
	go l.handler()
	return nil
}

func (l *Logger) handler() {
	defer func() {
		if p := recover(); p != nil {
			debug.PrintStack()
			go l.handler()
		}
	}()
	var buf types.IoBuffer
	for {
		select {
		case <-l.reopenChan:
			// reopen is used for roller
			err := l.reopen()
			if err == nil {
				return
			}
			DefaultLogger.Infof("%s reopen failed : %v", l.output, err)
		case <-l.closeChan:
			// flush all buffers before close
			// make sure all logs are outputed
			// a closed logger can not write anymore
			for {
				select {
				case buf = <-l.writeBufferChan:
					buf.WriteTo(l)
					buffer.PutIoBuffer(buf)
				default:
					l.stop()
					return
				}
			}
		case buf = <-l.writeBufferChan:
			for i := 0; i < 50; i++ {
				select {
				case b := <-l.writeBufferChan:
					buf.Write(b.Bytes())
					buffer.PutIoBuffer(b)
				default:
					break
				}
			}
			buf.WriteTo(l)
			buffer.PutIoBuffer(buf)
		}

		runtime.Gosched()
	}
}

func (l *Logger) stop() error {
	if l.writer == os.Stdout || l.writer == os.Stderr {
		return nil
	}

	if closer, ok := l.writer.(io.WriteCloser); ok {
		err := closer.Close()
		return err
	}

	return nil
}

func (l *Logger) reopen() error {
	if l.writer == os.Stdout || l.writer == os.Stderr {
		return ErrReopenUnsupported
	}
	if closer, ok := l.writer.(io.WriteCloser); ok {
		err := closer.Close()
		if err != nil {
			return err
		}
		return l.start()
	}
	return ErrReopenUnsupported
}

// Print writes the final buffere to the buffer chan
// if discard is true and the buffer is full, returns an error
func (l *Logger) Print(buf types.IoBuffer, discard bool) error {
	if l.disable {
		// free the buf
		buffer.PutIoBuffer(buf)
		return nil
	}
	select {
	case l.writeBufferChan <- buf:
	default:
		// todo: configurable
		if discard {
			return types.ErrChanFull
		} else {
			l.writeBufferChan <- buf
		}
	}
	return nil
}

func (l *Logger) Println(args ...interface{}) {
	if l.disable {
		return
	}
	s := fmt.Sprintln(args...)
	buf := buffer.GetIoBuffer(len(s))
	buf.WriteString(s)
	if len(s) == 0 || s[len(s)-1] != '\n' {
		buf.WriteString("\n")
	}
	l.Print(buf, true)
}

func (l *Logger) Printf(format string, args ...interface{}) {
	if l.disable {
		return
	}
	s := fmt.Sprintf(format, args...)
	buf := buffer.GetIoBuffer(len(s))
	buf.WriteString(s)
	if len(s) == 0 || s[len(s)-1] != '\n' {
		buf.WriteString("\n")
	}
	l.Print(buf, true)
}

// Fatal cannot be disabled
func (l *Logger) Fatalf(format string, args ...interface{}) {
	s := fmt.Sprintf(format, args...)
	buf := buffer.GetIoBuffer(len(s))
	buf.WriteString(s)
	buf.WriteTo(l.writer)
	os.Exit(1)
}

func (l *Logger) Fatal(args ...interface{}) {
	s := fmt.Sprint(args...)
	buf := buffer.GetIoBuffer(len(s))
	buf.WriteString(logTime() + " " + FatalPre)
	buf.WriteString(s)
	if len(s) == 0 || s[len(s)-1] != '\n' {
		buf.WriteString("\n")
	}
	buf.WriteTo(l.writer)
	os.Exit(1)
}

func (l *Logger) Fatalln(args ...interface{}) {
	s := fmt.Sprintln(args...)
	buf := buffer.GetIoBuffer(len(s))
	buf.WriteString(logTime() + " " + FatalPre)
	buf.WriteString(s)
	if len(s) == 0 || s[len(s)-1] != '\n' {
		buf.WriteString("\n")
	}
	buf.WriteTo(l.writer)
	os.Exit(1)
}

func (l *Logger) Write(p []byte) (n int, err error) {
	// default roller by daily
	if l.roller == nil {
		if !l.create.IsZero() {
			now := time.Now()
			if (l.create.Unix()+int64(localOffset))/defaultRollerTime != (now.Unix()+int64(localOffset))/defaultRollerTime {
				if err = os.Rename(l.output, l.output+"."+l.create.Format("2006-01-02")); err != nil {
					return 0, err
				}
				l.create = now
				go l.Reopen()
			}
		}
	}
	return l.writer.Write(p)
}

func (l *Logger) Close() error {
	l.closeChan <- struct{}{}
	return nil
}

func (l *Logger) Reopen() error {
	l.reopenChan <- struct{}{}
	return nil
}

func (l *Logger) Toggle(disable bool) {
	l.disable = disable
}

// syslogAddress
type syslogAddress struct {
	network string
	address string
}

func parseSyslogAddress(location string) *syslogAddress {
	for prefix, network := range remoteSyslogPrefixes {
		if strings.HasPrefix(location, prefix) {
			return &syslogAddress{
				network: network,
				address: strings.TrimPrefix(location, prefix),
			}
		}
	}

	return nil
}
