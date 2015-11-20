package flume

import (
	"errors"
	"fmt"
	"github.com/chentao/heka-flume/flume"
	"github.com/chentao/thrift/lib/go/thrift"
	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
	"regexp"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type FlumeOutput struct {
	config              *FlumeOutputConfig
	name                string
	or                  OutputRunner
	pConfig             *PipelineConfig
	bufferedOut         *BufferedOutput
	processMessageCount int64
	dropMessageCount    int64
	backMessageCount    int64
	rh                  *RetryHelper
	reportLock          sync.Mutex
	boErrorChan         chan error
	boExitChan          chan error
	backChan            chan []byte
	thriftAppender      *ThriftAppender
	thriftAppenderBack  *ThriftAppender
}

type FlumeOutputConfig struct {
	Address            string
	BatchSize          int    `toml:"batch_size"`
	ConnectTimeout     uint64 `toml:"connect_timeout"`
	QueueMaxBufferSize uint64 `toml:"queue_max_buffer_size"`
	QueueFullAction    string `toml:"queue_full_action"`
}

func (o *FlumeOutput) ConfigStruct() interface{} {
	return &FlumeOutputConfig{
		BatchSize:          1000,
		ConnectTimeout:     10000,
		QueueMaxBufferSize: 0,
		QueueFullAction:    "block",
	}
}

func (o *FlumeOutput) SetName(name string) {
	o.name = name
}

func (o *FlumeOutput) Init(config interface{}) (err error) {
	o.config = config.(*FlumeOutputConfig)
	if len(o.config.Address) == 0 {
		return fmt.Errorf("address must be specified")
	}

	switch o.config.QueueFullAction {
	case "shutdown", "drop", "block":
	default:
		return fmt.Errorf("`queue_full_action` must be 'shutdown', 'drop', or 'block'")
	}

	o.thriftAppender, err = NewThriftAppender(o.config.Address, o.config.ConnectTimeout)
	if err != nil {
		return err
	}
	o.thriftAppenderBack, err = NewThriftAppender(o.config.Address, o.config.ConnectTimeout)
	if err != nil {
		return err
	}
	return
}

func (o *FlumeOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	var (
		ok       = true
		pack     *PipelinePack
		inChan   = or.InChan()
		outBytes []byte
		stopChan = make(chan bool, 1)
		e        error
		count    int
		outBatch []*flume.ThriftFlumeEvent
	)

	o.boErrorChan = make(chan error, 5)
	o.boExitChan = make(chan error)

	if or.Encoder() == nil {
		return errors.New("Encoder must be specified.")
	}

	re := regexp.MustCompile("\\W")
	name := re.ReplaceAllString(or.Name(), "_")

	o.rh, err = NewRetryHelper(RetryOptions{
		MaxDelay:   "5s",
		MaxRetries: -1,
	})
	if err != nil {
		return fmt.Errorf("can't create retry helper: %s", err.Error())
	}

	o.pConfig = h.PipelineConfig()
	o.or = or
	o.bufferedOut, err = NewBufferedOutput("output_queue", name, or, h, o.config.QueueMaxBufferSize)
	if err != nil {
		if err == QueueIsFull {
			or.LogMessage("Queue capacity is already reached.")
		} else {
			return
		}
	}
	o.bufferedOut.Start(o, o.boErrorChan, o.boExitChan, stopChan)

	err = o.thriftAppender.Connect()
	if err != nil {
		or.LogError(err)
		return
	}
	err = o.thriftAppenderBack.Connect()
	if err != nil {
		or.LogError(err)
		return
	}
	defer func() {
		o.thriftAppender.Disconnect()
		o.thriftAppenderBack.Disconnect()
	}()

	o.backChan = make(chan []byte, 100)
	go o.backing()

	for ok {
		select {
		case e := <-o.boErrorChan:
			or.LogError(e)
		case pack, ok = <-inChan:
			if !ok {
				if len(outBatch) > 0 {
					o.sendBatch(outBatch)
				}
				stopChan <- true
				// Make sure buffer isn't blocked on sending to outputError
				select {
				case e := <-o.boErrorChan:
					or.LogError(e)
				default:
				}
				<-o.boExitChan
				close(o.backChan)
				break
			}

			outBytes, e = or.Encode(pack)
			pack.Recycle()
			if e != nil {
				or.LogError(e)
				atomic.AddInt64(&o.dropMessageCount, 1)
				continue
			}

			if outBytes != nil {
				event := &flume.ThriftFlumeEvent{
					Body: outBytes,
				}
				outBatch = append(outBatch, event)
				if count++; count >= o.config.BatchSize {
					if len(outBatch) > 0 {
						o.sendBatch(outBatch)
					}
					count = 0
					outBatch = outBatch[:0]
				}
			}
		case err = <-o.boExitChan:
			ok = false
		}
	}
	return
}

func (o *FlumeOutput) sendBatch(batch []*flume.ThriftFlumeEvent) {
	if len(batch) == 1 {
		o.backChan <- batch[0].Body
		return
	}

	err := o.thriftAppender.AppendBatch(batch)
	if err == nil {
		atomic.AddInt64(&o.processMessageCount, int64(len(batch)))
		return
	}
	o.or.LogError(fmt.Errorf("Backing %d events: %v", len(batch), err))
	for _, event := range batch {
		o.backChan <- event.Body
	}
}

func (o *FlumeOutput) SendRecord(buffer []byte) (err error) {
	event := &flume.ThriftFlumeEvent{
		Body: buffer,
	}
	err = o.thriftAppenderBack.Append(event)
	if err == nil {
		atomic.AddInt64(&o.processMessageCount, 1)
		return nil
	}
	return fmt.Errorf("SendRecord fail: %v", err)
}

func (o *FlumeOutput) backing() {
	for true {
		select {
		case backEvent, ok := <-o.backChan:
			if !ok {
				o.or.LogMessage("aaa")
				return
			}
			err := o.bufferedOut.QueueBytes(backEvent)
			if err == nil {
				atomic.AddInt64(&o.backMessageCount, 1)
			} else if err == QueueIsFull {
				o.or.LogError(err)
				if !o.queueFull(backEvent, 1) {
					return
				}
			} else if err != nil {
				o.or.LogError(err)
				atomic.AddInt64(&o.dropMessageCount, 1)
			}
		}
	}
}

func (o *FlumeOutput) queueFull(buffer []byte, count int64) bool {
	switch o.config.QueueFullAction {
	case "block":
		for o.rh.Wait() == nil {
			if o.pConfig.Globals.IsShuttingDown() {
				return false
			}
			blockErr := o.bufferedOut.QueueBytes(buffer)
			if blockErr == nil {
				atomic.AddInt64(&o.processMessageCount, count)
				o.rh.Reset()
				break
			}
			select {
			case e := <-o.boErrorChan:
				o.or.LogError(e)
			default:
			}
			runtime.Gosched()
		}
	case "shutdown":
		o.pConfig.Globals.ShutDown()
		return false
	case "drop":
		atomic.AddInt64(&o.dropMessageCount, count)
	}
	return true
}

func init() {
	RegisterPlugin("FlumeOutput", func() interface{} {
		return new(FlumeOutput)
	})
}

func (o *FlumeOutput) ReportMsg(msg *message.Message) error {
	o.reportLock.Lock()
	defer o.reportLock.Unlock()

	message.NewInt64Field(msg, "ProcessMessageCount",
		atomic.LoadInt64(&o.processMessageCount), "count")
	message.NewInt64Field(msg, "DropMessageCount",
		atomic.LoadInt64(&o.dropMessageCount), "count")
	message.NewInt64Field(msg, "BackMessageCount",
		atomic.LoadInt64(&o.backMessageCount), "count")
	o.bufferedOut.ReportMsg(msg)

	return nil
}

type ThriftAppender struct {
	client *flume.ThriftSourceProtocolClient
}

func NewThriftAppender(addr string, timeout uint64) (*ThriftAppender, error) {
	tSock, err := thrift.NewTSocketTimeout(addr, time.Duration(timeout)*time.Millisecond)
	if err != nil {
		return nil, err
	}
	trans := thrift.NewTFramedTransport(tSock)
	protoFactory := thrift.NewTCompactProtocolFactory()
	c := flume.NewThriftSourceProtocolClientFactory(trans, protoFactory)

	return &ThriftAppender{
		client: c,
	}, nil
}

func (t *ThriftAppender) Connect() error {
	if t.client.Transport.IsOpen() {
		return nil
	}

	return t.client.Transport.Open()
}

func (t *ThriftAppender) Disconnect() error {
	if t.client.Transport != nil {
		err := t.client.Transport.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *ThriftAppender) AppendBatch(batch []*flume.ThriftFlumeEvent) error {
	status, err := t.client.AppendBatch(batch)
	if status == flume.Status_OK && err == nil {
		return nil
	}
	t.Disconnect()
	if err := t.Connect(); err != nil {
		return fmt.Errorf("AppendBatch fail at reconnect: %v, ", err)
	}
	status, err = t.client.AppendBatch(batch)
	if status == flume.Status_OK && err == nil {
		return nil
	}
	return fmt.Errorf("AppendBatch fail after retry, status:%s, err:%v", status.String(), err)
}

func (t *ThriftAppender) Append(event *flume.ThriftFlumeEvent) error {
	status, err := t.client.Append(event)
	if status == flume.Status_OK && err == nil {
		return nil
	}
	t.Disconnect()
	if err := t.Connect(); err != nil {
		return fmt.Errorf("Append fail at reconnect: %v, ", err)
	}
	status, err = t.client.Append(event)
	if status == flume.Status_OK && err == nil {
		return nil
	}
	return fmt.Errorf("Append fail after retry, status:%s, err:%v", status.String(), err)
}
