package flume

import (
	"errors"
	"fmt"
	"github.com/chentao/heka-flume/flume"
	"github.com/chentao/thrift/lib/go/thrift"
	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
	"sync"
	"sync/atomic"
	"time"
)

type FBatch struct {
	queueCursor string
	count       int64
	batch       []*flume.ThriftFlumeEvent
}

type MsgPack struct {
	bytes       []byte
	queueCursor string
}

type FlumeOutput struct {
	config           *FlumeOutputConfig
	pConfig          *PipelineConfig
	or               OutputRunner
	sentMessageCount int64
	dropMessageCount int64
	outputBlock      *RetryHelper
	reportLock       sync.Mutex
	thriftAppender   *ThriftAppender

	outBatch    []*flume.ThriftFlumeEvent
	count       int64
	queueCursor string

	recvChan    chan MsgPack
	backChan    chan []*flume.ThriftFlumeEvent
	batchChan   chan FBatch
	stopChan    chan bool
	flushTicker *time.Ticker
}

type FlumeOutputConfig struct {
	Address        string
	BatchSize      int64  `toml:"batch_size"`
	ConnectTimeout uint64 `toml:"connect_timeout"`
	UseBuffering   bool   `toml:"use_buffering"`
	FlushInterval  uint32 `toml:"flush_interval"`
}

func (o *FlumeOutput) ConfigStruct() interface{} {
	return &FlumeOutputConfig{
		BatchSize:      1000,
		ConnectTimeout: 10000,
		UseBuffering:   true,
		FlushInterval:  1000,
	}
}

func (o *FlumeOutput) Init(config interface{}) (err error) {
	o.config = config.(*FlumeOutputConfig)
	if len(o.config.Address) == 0 {
		return fmt.Errorf("address must be specified")
	}

	o.batchChan = make(chan FBatch)
	o.backChan = make(chan []*flume.ThriftFlumeEvent, 2)
	o.recvChan = make(chan MsgPack, 100)

	o.thriftAppender, err = NewThriftAppender(o.config.Address, o.config.ConnectTimeout)
	if err != nil {
		return err
	}
	return
}

func (o *FlumeOutput) Prepare(or OutputRunner, h PluginHelper) (err error) {
	if or.Encoder() == nil {
		return errors.New("Encoder must be specified.")
	}

	o.or = or
	o.pConfig = h.PipelineConfig()
	o.stopChan = or.StopChan()

	o.outputBlock, err = NewRetryHelper(RetryOptions{
		MaxDelay:   "5s",
		MaxRetries: -1,
	})
	if err != nil {
		return fmt.Errorf("can't create retry helper: %s", err.Error())
	}

	o.outBatch = make([]*flume.ThriftFlumeEvent, 0, 10000)
	go o.committer()

	if o.config.FlushInterval > 0 {
		d, err := time.ParseDuration(fmt.Sprintf("%dms", o.config.FlushInterval))
		if err != nil {
			return fmt.Errorf("can't create flush ticker: %s", err.Error())
		}
		o.flushTicker = time.NewTicker(d)
	}
	go o.batchSender()
	return nil
}

func (o *FlumeOutput) ProcessMessage(pack *PipelinePack) (err error) {
	outBytes, err := o.or.Encode(pack)
	if err != nil {
		return fmt.Errorf("can't encode: %s", err)
	}

	if outBytes != nil {
		o.recvChan <- MsgPack{bytes: outBytes, queueCursor: pack.QueueCursor}
	}

	return nil
}

func (o *FlumeOutput) batchSender() {
	ok := true
	for ok {
		select {
		case <-o.stopChan:
			ok = false
			continue
		case pack := <-o.recvChan:
			event := &flume.ThriftFlumeEvent{
				Body: pack.bytes,
			}
			o.outBatch = append(o.outBatch, event)
			o.queueCursor = pack.queueCursor
			o.count++
			if len(o.outBatch) > 0 && o.count >= o.config.BatchSize {
				o.sendBatch()
			}
		case <-o.flushTicker.C:
			if len(o.outBatch) > 0 {
				o.sendBatch()
			}
		}
	}
}

func (o *FlumeOutput) sendBatch() {
	b := FBatch{
		queueCursor: o.queueCursor,
		count:       o.count,
		batch:       o.outBatch,
	}
	o.count = 0
	select {
	case <-o.stopChan:
		return
	case o.batchChan <- b:
	}
	select {
	case <-o.stopChan:
	case o.outBatch = <-o.backChan:
	}
}

func (o *FlumeOutput) committer() {
	o.backChan <- make([]*flume.ThriftFlumeEvent, 0, 10000)

	var b FBatch
	ok := true
	for ok {
		select {
		case <-o.stopChan:
			ok = false
			continue
		case b, ok = <-o.batchChan:
			if !ok {
				continue
			}
		}
		if err := o.sendRecord(b.batch); err != nil {
			atomic.AddInt64(&o.dropMessageCount, b.count)
			o.or.LogError(err)
		} else {
			atomic.AddInt64(&o.sentMessageCount, b.count)
		}
		o.or.UpdateCursor(b.queueCursor)
		b.batch = b.batch[:0]
		o.backChan <- b.batch
	}
}

func (o *FlumeOutput) sendRecord(batch []*flume.ThriftFlumeEvent) error {
	err := o.thriftAppender.AppendBatch(batch)
	if err == nil {
		return nil
	}

	defer o.outputBlock.Reset()
	for {
		select {
		case <-o.stopChan:
			return err
		default:
		}
		e := o.outputBlock.Wait()
		if e != nil {
			break
		}
		err := o.thriftAppender.AppendBatch(batch)
		if err == nil {
			break
		}
		o.or.LogError(fmt.Errorf("can't AppendBatch: %s", err))
	}
	return err
}

func (o *FlumeOutput) CleanUp() {
	if o.flushTicker != nil {
		o.flushTicker.Stop()
	}
	o.thriftAppender.Disconnect()
}

func (o *FlumeOutput) ReportMsg(msg *message.Message) error {
	o.reportLock.Lock()
	defer o.reportLock.Unlock()

	message.NewInt64Field(msg, "SentMessageCount",
		atomic.LoadInt64(&o.sentMessageCount), "count")
	message.NewInt64Field(msg, "DropMessageCount",
		atomic.LoadInt64(&o.dropMessageCount), "count")
	return nil
}

func init() {
	RegisterPlugin("FlumeOutput", func() interface{} {
		return new(FlumeOutput)
	})
}

type ThriftAppender struct {
	addr    string
	timeout uint64
	client  *flume.ThriftSourceProtocolClient
}

func NewThriftAppender(addr string, timeout uint64) (*ThriftAppender, error) {
	return &ThriftAppender{
		addr:    addr,
		timeout: timeout,
		client:  nil,
	}, nil
}

func (t *ThriftAppender) Connect() error {
	if t.client != nil && t.client.Transport != nil {
		if t.client.Transport.IsOpen() {
			return nil
		}
	}

	tSock, err := thrift.NewTSocketTimeout(t.addr, time.Duration(t.timeout)*time.Millisecond)
	if err != nil {
		return err
	}
	trans := thrift.NewTFramedTransport(tSock)
	protoFactory := thrift.NewTCompactProtocolFactory()
	c := flume.NewThriftSourceProtocolClientFactory(trans, protoFactory)
	t.client = c

	return t.client.Transport.Open()
}

func (t *ThriftAppender) Disconnect() error {
	if t.client.Transport != nil {
		err := t.client.Transport.Close()
		if err != nil {
			return err
		}
		t.client.Transport = nil
	}
	return nil
}

func (t *ThriftAppender) AppendBatch(batch []*flume.ThriftFlumeEvent) (err error) {
	var status flume.Status
	if err = t.Connect(); err == nil {
		status, err = t.client.AppendBatch(batch)
		if status == flume.Status_OK && err == nil {
			return nil
		}
		if err != nil {
			t.Disconnect()
		}
	}

	return fmt.Errorf("AppendBatch fail, status:%s, err:%v", status.String(), err)
}

func (t *ThriftAppender) Append(event *flume.ThriftFlumeEvent) (err error) {
	var status flume.Status
	if err = t.Connect(); err == nil {
		status, err = t.client.Append(event)
		if status == flume.Status_OK && err == nil {
			return nil
		}
		if err != nil {
			t.Disconnect()
		}
	}

	return fmt.Errorf("Append fail, status:%s, err:%v", status.String(), err)
}
