package flume

import (
	"./flume"
	"fmt"
	. "github.com/mozilla-services/heka/pipeline"
)

type FlumeOutput struct {
	config              *FlumeOutputConfig
	name                string
	or                  OutputRunner
	pConfig             *PipelineConfig
	bufferedOut         *BufferedOutput
	processMessageCount int64
	dropMessageCount    int64
	rh                  *RetryHelper
	reportLock          sync.Mutex
	boErrorChan         chan error
	boExitChan          chan error
}

type FlumeOutputConfig struct {
	Address   string
	BatchSize int `toml:"batch_size"`
	//ConnectTimeout          uint64 `toml:"connect_timeout"`
	//RequestTimeout          uint64 `toml:"request_timeout"`
	UseBufferingBack   bool   `toml:"use_buffering_back"`
	QueueMaxBufferSize uint64 `toml:"queue_max_buffer_size"`
	QueueFullAction    string `toml:"queue_full_action"`
}

func (o *FlumeOutput) ConfigStruct() interface{} {
	return &FlumeOutputConfig{}
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

func (o *FlumeOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	var (
		ok       = true
		pack     *PipelinePack
		inChan   = or.InChan()
		outBytes []byte
		stopChan = make(chan bool, 1)
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
			or.LogMessage("Queue capacity is already reached/")
		} else {
			return
		}
		o.bufferedOut.Start(0, o.boErrorChan, o.boExitChan, stopChan)
	}

	for ok {
		select {
		case e := <-o.boErrorChan:
			or.LogError(e)
		case pack, ok = <-inChan:
			if !ok {

			}
		}
	}
	return

}
