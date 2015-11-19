package flume

import (
	"./flume"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	. "github.com/mozilla-services/heka/pipeline"
	"regexp"
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
	tClient             *flume.ThriftSourceProtocolClient
	backChan            chan []byte
}

type FlumeOutputConfig struct {
	Address            string
	BatchSize          int    `toml:"batch_size"`
	ConnectTimeout     uint64 `toml:"connect_timeout"`
	QueueMaxBufferSize uint64 `toml:"queue_max_buffer_size"`
	QueueFullAction    string `toml:"queue_full_action"`
	//RequestTimeout     uint64 `toml:"request_timeout"`
	//UseBufferingBack   bool   `toml:"use_buffering_back"`
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

	switch o.config.QueueFullAction {
	case "shutdown", "drop", "block":
	default:
		return fmt.Errorf("`queue_full_action` must be 'shutdown', 'drop', or 'block'")
	}
	return
}

func (o *FlumeOutput) connect(addr string, timeout uint64) (err error) {
	var tSock *thrift.TSocket
	tSock, err = thrift.NewTSocketTimeout(addr, timeout*time.Millisecond)
	if err != nil {
		return
	}
	protoFactory := thrift.NewTBinaryProtocalFactoryDefault()
	o.tClient = flume.NewThriftSourceProtocolClientFactory(o.tSock, protoFactory)
	if err = o.tClient.Transport.Open(); err != nil {
		return
	}
	return nil
}

func (o *FlumeOutput) disconnect() (err error) {
	if o.tClient.Transport != nil {
		err = o.tClient.Transport.Close()
		if err != nil {
			return err
		}
		o.tClient.Transport = nil
	}
}

func (o *FlumeOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	var (
		ok       = true
		pack     *PipelinePack
		inChan   = or.InChan()
		outBytes []byte
		stopChan = make(chan bool, 1)
		e        error
		outBatch []*thrift.ThriftFlumeEvent
		count    int64
	)

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
		o.bufferedOut.Start(o, o.boErrorChan, o.boExitChan, stopChan)
	}

	err = o.connect(o.config.Address, o.config.ConnectTimeout)
	if err != nil {
		or.LogError(err)
		return
	}
	defer func() {
		err = o.disconnect()
		if err != nil {
			or.LogError(err)
			return
		}
	}()

	o.backChan = make(chan []byte, 100)
	go o.backing(stopChan)

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
				event := &thrift.ThriftFlumeEvent{
					body: outBytes,
				}
				outBatch = append(outBatch, event)
				if count++; count > o.config.BatchSize {
					if len(outBatch) > 0 {
						o.sendBatch(outBatch)
					}
					count = 0
				}
			}
		case err = <-o.boExitChan:
			ok = false
		}
	}
	return
}

func (o *FlumeOutput) sendBatch(batch []*flume.ThriftFlumeEvent) {
	status, err := o.tClient.AppendBatch(batch)
	if status != flume.Status_OK {
		o.or.LogError(fmt.Sprintf("flume.AppendBatch fail, start backing: %v", err))
		for event := range batch {
			o.backChan <- event.body
		}
	}
	atomic.AddInt64(&o.processMessageCount, len(batch))
}

func (o *FlumeOutput) SendRecord(buffer []byte) (err error) {
	event := &thrift.ThriftFlumeEvent{
		body: buffer,
	}
	status, err := o.tClient.Append(event)
	if status == flume.Status_OK {
		atomic.AddInt64(&o.processMessageCount, 1)
		return nil
	}
	return
}

func (o *FlumeOutput) backing(stopChan chan bool) {
	for true {
		select {
		case <-stopChan:
			return
		case backEvent, ok := <-o.backChan:
			if !ok {
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
				atomic.AddInt64(&o.dropMessageCount, count)
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
