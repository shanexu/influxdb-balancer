package relay

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	retryInitial    = 500 * time.Millisecond
	retryMultiplier = 2
)

// Buffers and retries operations, if the buffer is full operations are dropped.
// Only tries one operation at a time, the next operation is not attempted
// until success or timeout of the previous operation.
// There is no delay between attempts of different operations.
type retryBuffer struct {
	initialInterval time.Duration
	multiplier      time.Duration
	maxInterval     time.Duration

	maxBuffered int
	maxBatch    int

	list  *bufferList
	batch *batch

	p    Poster
	name string

	dataDir string
}

func NewRetryBuffer(dataDir string, name string, size, batch int, max time.Duration, p Poster) *retryBuffer {
	r := &retryBuffer{
		initialInterval: retryInitial,
		multiplier:      retryMultiplier,
		maxInterval:     max,
		maxBuffered:     size,
		maxBatch:        batch,
		list:            newBufferList(size, batch),
		batch:           nil,
		p:               p,
		name:            name,
		dataDir:         dataDir,
	}
	go r.run()
	go r.reloadDumps()
	go r.onShutdown()
	return r
}

func (r *retryBuffer) GetBufferSize() int {
	return r.list.size
}

func (r *retryBuffer) Post(buf []byte, query string, auth string) (*ResponseData, error) {
	buf1 := make([]byte, len(buf))
	copy(buf1, buf)
	_, err := r.list.add(buf1, query, auth)
	if err != nil {
		return nil, err
	}

	return &ResponseData{
		StatusCode: 204,
	}, nil
}

func (r *retryBuffer) onShutdown() {
	OnShutdownWg.Add(1)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	<-sigChan
	r.list.cond.L.Lock()
	defer r.list.cond.L.Unlock()
	defer OnShutdownWg.Done()
	if (r.batch != nil && r.batch.resp == nil) || (r.list.head != nil) {
		parent := filepath.Join(r.dataDir, r.name+"_dumps")
		if err := os.MkdirAll(parent, os.ModeDir|os.ModePerm); err != nil {
			log.Errorw("make dumps dir failed", "error", err)
			return
		}
		dumpFile, err := os.Create(filepath.Join(parent, "dump_"+strconv.FormatInt(time.Now().UnixNano(), 10)))
		if err != nil {
			log.Errorw("create dump file failed", "error", err)
		}
		if r.batch != nil && r.batch.resp == nil {
			for _, buf := range r.batch.bufs {
				dumpRequest(dumpFile, buf, r.batch.query, r.batch.auth)
			}

		}
		for cur := &(r.list.head); *cur != nil; cur = &(*cur).next {
			for _, buf := range (*cur).bufs {
				dumpRequest(dumpFile, buf, (*cur).query, (*cur).auth)
			}
		}
		dumpFile.Close()
	}
}

func (r *retryBuffer) reloadDumps() {
	parent := filepath.Join(r.dataDir, r.name+"_dumps")
	os.MkdirAll(parent, os.ModeDir|os.ModePerm)
	dumpFiles, err := filepath.Glob(filepath.Join(parent, "dump_*"))
	if err != nil {
		log.Error("cannot scan dump files", err)
		return
	}
	for _, filename := range dumpFiles {
		if data, err := ioutil.ReadFile(filename); err != nil {
			log.Error("read dump file ", filename, " failed", err)
		} else {
			buffer := bytes.NewBuffer(data)
			for {
				if buf, query, auth, err := loadRequest(buffer); err != nil {
					break
				} else {
					for {
						if _, err := r.Post(buf, query, auth); err == nil {
							break
						}
						time.Sleep(10 * time.Millisecond)
					}
				}
			}
			os.Remove(filename)
		}
	}
}

func (r *retryBuffer) run() {
	buf := bytes.NewBuffer(make([]byte, 0, r.maxBatch))
	for {
		buf.Reset()
		batch := r.list.pop()
		r.batch = batch

		for _, b := range batch.bufs {
			buf.Write(b)
			if len(b) > 0 && b[len(b)-1] != '\n' {
				buf.WriteByte('\n')
			}
		}

		interval := r.initialInterval
		for {
			resp, err := r.p.Post(buf.Bytes(), batch.query, batch.auth)
			if err == nil && resp.StatusCode/100 != 5 {
				if resp.StatusCode != 204 {
					log.Warnw("batch failed", "error", string(resp.Body))
				}
				batch.resp = resp
				break
			}

			if err == nil && resp.StatusCode == 500 && strings.Contains(string(resp.Body), "retention policy not found") {
				log.Warnw("batch failed", "error", string(resp.Body))
				batch.resp = resp
				break
			}

			if interval != r.maxInterval {
				interval *= r.multiplier
				if interval > r.maxInterval {
					interval = r.maxInterval
				}
			}

			time.Sleep(interval)
		}
	}
}

type batch struct {
	query string
	auth  string
	bufs  [][]byte
	size  int
	full  bool

	resp *ResponseData

	next *batch
}

func newBatch(buf []byte, query string, auth string) *batch {
	b := new(batch)
	b.bufs = [][]byte{buf}
	b.size = len(buf)
	b.query = query
	b.auth = auth
	return b
}

type bufferList struct {
	cond     *sync.Cond
	head     *batch
	size     int
	maxSize  int
	maxBatch int
}

func newBufferList(maxSize, maxBatch int) *bufferList {
	return &bufferList{
		cond:     sync.NewCond(new(sync.Mutex)),
		maxSize:  maxSize,
		maxBatch: maxBatch,
	}
}

// pop will remove and return the first element of the list, blocking if necessary
func (l *bufferList) pop() *batch {
	l.cond.L.Lock()

	for l.size == 0 {
		l.cond.Wait()
	}

	b := l.head
	l.head = l.head.next
	l.size -= b.size

	l.cond.L.Unlock()

	return b
}

func (l *bufferList) add(buf []byte, query string, auth string) (*batch, error) {
	l.cond.L.Lock()

	if l.size+len(buf) > l.maxSize {
		l.cond.L.Unlock()
		return nil, ErrBufferFull
	}

	l.size += len(buf)
	l.cond.Signal()

	var cur **batch

	// non-nil batches that either don't match the query string, don't match the auth
	// credentials, or would be too large when adding the current set of points
	// (auth must be checked to prevent potential problems in multi-user scenarios)
	for cur = &l.head; *cur != nil; cur = &(*cur).next {
		if (*cur).query != query || (*cur).auth != auth || (*cur).full {
			continue
		}

		if (*cur).size+len(buf) > l.maxBatch {
			// prevent future writes from preceding this write
			(*cur).full = true
			continue
		}

		break
	}

	if *cur == nil {
		// new tail element
		*cur = newBatch(buf, query, auth)
	} else {
		// append to current batch
		b := *cur
		b.size += len(buf)
		b.bufs = append(b.bufs, buf)
	}

	defer l.cond.L.Unlock()
	return *cur, nil
}

func loadRequest(r *bytes.Buffer) ([]byte, string, string, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, "", "", err
	}
	buf := make([]byte, length)
	if _, err := r.Read(buf); err != nil {
		return nil, "", "", err
	}
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, "", "", err
	}
	queryBuf := make([]byte, length)
	if _, err := r.Read(queryBuf); err != nil {
		return nil, "", "", err
	}
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, "", "", err
	}
	authBuf := make([]byte, length)
	if _, err := r.Read(authBuf); err != nil {
		return nil, "", "", err
	}
	return buf, string(queryBuf), string(authBuf), nil
}

func dumpRequest(w io.Writer, buf []byte, query, auth string) {
	binary.Write(w, binary.BigEndian, uint32(len(buf)))
	w.Write(buf)
	binary.Write(w, binary.BigEndian, uint32(len(query)))
	w.Write([]byte(query))
	binary.Write(w, binary.BigEndian, uint32(len(auth)))
	w.Write([]byte(auth))
}
