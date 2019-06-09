package relay

import (
	"bytes"
	"encoding/binary"
	"github.com/tecbot/gorocksdb"
	"os"
	"os/signal"
	fp "path/filepath"
	"sync"
	"syscall"
	"time"
)

// Buffers and retries operations, if the buffer is full operations are dropped.
// Only tries one operation at a time, the next operation is not attempted
// until success or timeout of the previous operation.
// There is no delay between attempts of different operations.
type rocksDBBuffer struct {
	name string

	maxBuffered uint64

	list *rocksDBBufferList

	p Poster
}

func NewRocksDBBuffer(dataDir string, name string, size uint64, p Poster) *rocksDBBuffer {
	r := &rocksDBBuffer{
		name:        name,
		maxBuffered: size,
		list:        newRocksDBBufferList(dataDir, name, size),
		p:           p,
	}
	go r.run()
	return r
}

func (r *rocksDBBuffer) Post(buf []byte, query string, auth string) (*ResponseData, error) {
	buf1 := make([]byte, len(buf))
	copy(buf1, buf)
	err := r.list.add(buf1, query, auth)
	if err != nil {
		return nil, err
	}

	return &ResponseData{
		StatusCode: 204,
	}, nil
}

func (r *rocksDBBuffer) run() {
	var buf bytes.Buffer
	for {
		buf.Reset()
		batch, err := r.list.pop()
		if err != nil {
			continue
		}

		for _, b := range batch.bufs {
			buf.Write(b)
		}

		for {
			resp, err := r.p.Post(buf.Bytes(), batch.query, batch.auth)
			if err == nil {
				batch.resp = resp
				break
			}

			time.Sleep(retryInitial)
		}
	}
}

type rocksDBBufferList struct {
	cond         *sync.Cond
	head         *batch
	size         uint64
	maxSize      uint64
	maxBatch     int
	db           *gorocksdb.DB
	offset       uint64
	commitOffset uint64
}

func (l *rocksDBBufferList) getMetadata(name string) uint64 {
	bs, _ := l.db.GetBytes(readOpts, []byte(name))
	if len(bs) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(bs)
}

func (l *rocksDBBufferList) setMetadata(name string, value uint64) {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, value)
	if err := l.db.Put(writeOpts, []byte(name), bs); err != nil {
		log.Warnf("set metadata failed %s", err)
	}
}

func newRocksDBBufferList(dataDir string, name string, maxSize uint64) *rocksDBBufferList {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(67108864))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	if err := os.MkdirAll(dataDir, os.ModeDir|os.ModePerm); err != nil {
		log.Fatalf("mkdir data dir failed: %s", err)
	}
	db, err := gorocksdb.OpenDb(opts, fp.Join(dataDir, name))
	if err != nil {
		log.Fatalf("open rocksdb failed %s", err)
	}

	go func() {
		OnShutdownWg.Add(1)
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
		<-sigChan
		log.Infof("before close db %s", name)
		db.Close()
		log.Infof("after close db %s", name)
		OnShutdownWg.Done()
	}()

	l := &rocksDBBufferList{
		cond:         sync.NewCond(new(sync.Mutex)),
		maxSize:      maxSize,
		db:           db,
		offset:       0,
		commitOffset: 0,
		size:         0,
	}
	l.offset = l.getMetadata("offset")
	l.commitOffset = l.getMetadata("commitOffset")
	l.size = l.getMetadata("size")
	log.Infof("db: %s, offset: %d, commitOffset: %d, size: %d", name, l.offset, l.commitOffset, l.size)
	return l
}

// pop will remove and return the first element of the list, blocking if necessary
func (l *rocksDBBufferList) pop() (*batch, error) {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	for l.size == 0 {
		l.cond.Wait()
	}
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, l.commitOffset)
	value, err := l.db.GetBytes(readOpts, key)
	if err != nil {
		return nil, err
	} else {
		defer func() {
			if err := l.db.Delete(writeOpts, key); err != nil {
				log.Warnw("delete key failed", "key", key)
			}
			l.commitOffset += 1
			l.size -= uint64(len(value))
			l.setMetadata("commitOffset", l.commitOffset)
			l.setMetadata("size", l.size)
		}()
		r := bytes.NewBuffer(value)
		if buf, query, auth, err := loadRequest(r); err == nil {
			return newBatch(buf, query, auth), nil
		} else {
			return nil, err
		}
	}
}

func (l *rocksDBBufferList) add(buf []byte, query string, auth string) error {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	if l.size > l.maxSize {
		return ErrBufferFull
	}

	var valueBuf bytes.Buffer
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, l.offset)
	dumpRequest(&valueBuf, buf, query, auth)
	value := valueBuf.Bytes()

	err := l.db.Put(writeOpts, key, value)
	if err == nil {
		l.offset += 1
		l.size += uint64(len(value))
		l.setMetadata("offset", l.offset)
		l.setMetadata("size", l.size)
		l.cond.Signal()
	}
	return err
}
