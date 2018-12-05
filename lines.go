package linesd

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/korovkin/limiter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const SERVICE_NAME = "linesd"
const VERSION_NUMBER = "0.0.3"
const ID_LINESD = "LINESD"

type ConfigStream struct {
	Name         string `json:"name"`
	TaiCmd       string `json:"tail_cmd"`
	TailFilename string `json:"tail_filename"`
	IsWriter     bool   `json:"is_writer"`
	IsStdin      bool   `json:"is_stdin"`

	batch *LinesBatch
}

type Config struct {
	AWSRegion           string                   `json:"aws_region"`
	AWSBucket           string                   `json:"aws_bucket"`
	AWSKeyPrefix        string                   `json:"aws_key_prefix"`
	AWSElasticSearchURL string                   `json:"aws_elastic_search"`
	Env                 string                   `json:"env"`
	ConcLimit           int                      `json:"conc_limit"`
	IsShowBatches       bool                     `json:"is_show_batches"`
	IsShowLines         bool                     `json:"is_show_lines"`
	BatchSizeInLines    int                      `json:"batch_size_in_lines"`
	BatchSizeInSeconds  int                      `json:"batch_size_in_seconds"`
	Progress            int                      `json:"progress"`
	Address             string                   `json:"address"`
	Streams             map[string]*ConfigStream `json:"streams"`

	awsKeyPrefixEnv string
}

type Stats struct {
	Counters *prometheus.CounterVec `json:"-"`
	Gauges   *prometheus.GaugeVec   `json:"-"`
}

type LinesBatch struct {
	Name           string    `json:"name"`
	BatchId        string    `json:"batch_id"`
	TimestampStart int64     `json:"ts_start"`
	TimestampEnd   int64     `json:"ts_end"`
	Lines          []*string `json:"lines"`
}

type Server struct {
	Stats               Stats
	Config              Config
	conc                *limiter.ConcurrencyLimiter
	count               uint32
	hostname            string
	machineId           uint16
	writerStreamChannel chan []byte

	linesCounter int64
}

func (s *Server) GenerateUniqueId(idType string) (string, string, time.Time) {
	var i = (atomic.AddUint32(&s.count, 1)) % 0xFFFF
	now := time.Now()
	id := fmt.Sprintf("%04d%02d%02d_%02d%02d%02d_%010Xm%04Xi%04X_%s",
		now.Year(),
		now.Month(),
		now.Day(),

		now.Hour(),
		now.Minute(),
		now.Second(),

		now.Nanosecond(),
		s.machineId,
		i,
		idType)
	return id, id[0:8], now
}

func (t *Server) Initialize(config *Config) {
	var err error

	t.Config = *config

	if t.Config.Env == "" {
		t.Config.Env = "dev"
	}

	if t.Config.BatchSizeInLines == 0 && t.Config.BatchSizeInSeconds == 0 {
		fmt.Println("FATAL: must set batch_size_in_lines or batch_size_in_seconds")
		return
	}

	if t.Config.ConcLimit <= 0 {
		t.Config.ConcLimit = 8
	}

	t.writerStreamChannel = make(chan []byte, 5000)
	t.Config.awsKeyPrefixEnv = t.Config.AWSKeyPrefix + "/" + t.Config.Env
	fmt.Println("LINESD: config:", ToJsonString(t.Config))
	t.conc = limiter.NewConcurrencyLimiter(t.Config.ConcLimit)
	t.hostname, _ = os.Hostname()
	t.machineId, err = Lower16BitPrivateIP()
	CheckFatal(err)
}

func (t *Server) UploadBatch(batch *LinesBatch) {
	if batch == nil {
		return
	}

	// S3:
	if t.Config.AWSBucket != "" {
		s3Bucket := t.Config.AWSBucket
		s3Key := fmt.Sprintf(
			"%s/%s/%s/%s/%s.json",
			t.Config.awsKeyPrefixEnv,
			t.hostname,
			batch.Name,
			batch.BatchId[0:8],
			batch.BatchId,
		)

		_, _, err := S3PutBlob(
			&t.Config.AWSRegion,
			s3Bucket,
			ToJsonBytes(batch),
			s3Key,
			CONTENT_TYPE_JSON,
		)
		CheckNotFatal(err)

		if t.Config.IsShowBatches {
			fmt.Println("BATCH:", ToJsonString(batch))
		}

		if err == nil {
			fmt.Println("LINESD: LINELINESD: S3:", batch.Name, len(batch.Lines), fmt.Sprintf("s3://%s/%s", s3Bucket, s3Key))
			t.Stats.Counters.WithLabelValues("log_lines_out", "").Add(float64(len(batch.Lines)))
			t.Stats.Counters.WithLabelValues("log_batches_out", "").Inc()
		} else {
			t.Stats.Counters.WithLabelValues("log_lines_out_err", CleanupStringASCII(err.Error(), true)).Add(float64(len(batch.Lines)))
			t.Stats.Counters.WithLabelValues("log_batches_out_err", CleanupStringASCII(err.Error(), true)).Inc()
		}
	}

	// ES:
	if t.Config.AWSElasticSearchURL != "" {
		items := map[string]interface{}{}
		for lc, line := range batch.Lines {
			itemId := fmt.Sprintf("%s-%06d", batch.BatchId, lc)

			a := math.Min(float64(batch.TimestampStart), float64(batch.TimestampEnd))
			b := math.Max(float64(batch.TimestampStart), float64(batch.TimestampEnd))
			dt := math.Abs(b-a) * float64(lc) / float64(len(batch.Lines))
			timestamp := a + dt

			item := map[string]interface{}{
				"lc":        lc,
				"timestamp": int64(timestamp),
				"hostname":  t.hostname,
				"stream":    batch.Name,
				"line":      *line,
			}
			items[itemId] = item
		}
		err := ElasticSearchPut(
			t.Config.AWSElasticSearchURL,
			fmt.Sprintf("tlines-%s-%s", t.hostname, batch.Name),
			t.Config.Env,
			"line",
			items)
		CheckNotFatal(err)

		if err == nil {
			fmt.Println("LINESD: ES:", batch.Name, len(batch.Lines), batch.BatchId)
			t.Stats.Counters.WithLabelValues("log_batches_es_ok", "").Inc()
		} else {
			t.Stats.Counters.WithLabelValues("log_batches_es_err", CleanupStringASCII(err.Error(), true)).Inc()
		}
	}
}

func (t *Server) ProcessLine(streamAddress *string, stream *ConfigStream, line *string) {
	now := time.Now()

	if line != nil && t.Config.IsShowLines {
		fmt.Println(
			"LINESD: LINE:",
			t.linesCounter,
			stream.Name,
			"|",
			*line)
	}

	t.Stats.Counters.WithLabelValues("log_lines_in", "").Inc()

	batch := stream.batch
	if stream.batch == nil {
		batch = &LinesBatch{}
		id, _, now := t.GenerateUniqueId(ID_LINESD)
		batch.Name = stream.Name
		batch.BatchId = id
		batch.TimestampStart = now.Unix()
		batch.TimestampEnd = now.Unix()
		stream.batch = batch
		t.Stats.Counters.WithLabelValues("log_batches_in", "").Inc()
	}

	if batch.Lines == nil {
		batch.Lines = []*string{}
	}

	if line != nil {
		batch.Lines = append(batch.Lines, line)
		batch.TimestampEnd = now.Unix()
	}
	isBatchDone := false

	if t.Config.BatchSizeInLines > 0 && !isBatchDone && len(batch.Lines) > t.Config.BatchSizeInLines {
		isBatchDone = true
	} else if t.Config.BatchSizeInSeconds > 0 &&
		len(batch.Lines) > 0 &&
		math.Abs(float64(batch.TimestampEnd)-float64(batch.TimestampStart)) >= float64(t.Config.BatchSizeInSeconds) {
		isBatchDone = true
	}

	if line != nil {
		t.linesCounter += 1
		if (t.linesCounter % int64(t.Config.Progress)) == 0 {
			log.Println("=> LINESD:",
				"lines:", humanize.Comma(int64(t.linesCounter)),
			)
		}
	}

	if isBatchDone {
		stream.batch = nil
		t.conc.Execute(func() {
			t.UploadBatch(batch)
		})
	}

}

func (t *Server) ReadStream(streamAddress *string, stream *ConfigStream) {
	var err error
	var command *exec.Cmd
	var inputStream io.ReadCloser = nil

	if stream.IsStdin {
		inputStream = os.Stdin
	} else if stream.TailFilename != "" {
		if stream.TaiCmd == "" {
			stream.TaiCmd = "/usr/bin/tail"
		}
		command = exec.Command(
			stream.TaiCmd,
			"-F",
			stream.TailFilename,
		)
		command.Env = []string{
			"LINESD=1",
		}
		fmt.Println("=> running tail:", command.Path, command.Args)
		inputStream, err = command.StdoutPipe()
		CheckFatal(err)

		// run the tailer:
		err = command.Start()
		CheckFatal(err)
	} else if stream.IsWriter {
	} else {
		panic(errors.New("unknown stream type: " + stream.Name))
	}

	linesQueue := make(chan *string, 1000)
	doneQueue := make(chan bool, 1)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	signal.Notify(signals, os.Kill)

	// processor:
	if err == nil {
		go func() {
			isKeepWorking := true
			defer t.ProcessLine(streamAddress, stream, nil)
			defer func() {
				fmt.Println(stream.Name, "LINESD: TAILER: DONE")
				doneQueue <- true
			}()
			for isKeepWorking == true {
				select {
				case s := <-signals:
					fmt.Println(stream.Name, "LINESD: SIGNAL:", s.String())
					if inputStream != nil {
						inputStream.Close()
					}
					if stream.IsWriter {
						close(t.writerStreamChannel)
					}
					isKeepWorking = false
					break
				case line, isMore := <-linesQueue:
					t.ProcessLine(streamAddress, stream, line)
					if !isMore {
						fmt.Println(stream.Name, "LINESD: EOF.", isMore)
						isKeepWorking = false
						break
					}
				case <-time.After(time.Second * 10):
					t.ProcessLine(streamAddress, stream, nil)
				}
			}
		}()
	}

	// stdin / file reader:
	if err == nil && inputStream != nil {
		reader := bufio.NewReader(inputStream)
		for true {
			line, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println(stream.Name, "LINESD: EOF:", err.Error())
				close(linesQueue)
				break
			}

			line = strings.TrimSpace(line)
			if len(line) == 0 {
				fmt.Println(stream.Name, "LINESD: EMPTY LINE")
				continue
			}
			// if t.Config.IsShowLines {
			// 	fmt.Println(stream.Name, "LINESD: LINE:", line)
			// }
			linesQueue <- &line
		}
		fmt.Println(stream.Name, "LINESD: READER: DONE")
	}

	// writer reader:
	if err == nil && stream.IsWriter {
		for isKeepWorking := true; isKeepWorking; {
			select {
			case b, more := <-t.writerStreamChannel:
				if len(b) == 0 || b == nil || more == false {
					fmt.Println(stream.Name, "LINESD: WRITER: DONE")
					isKeepWorking = false
					close(linesQueue)
					break
				}
				line := string(b)
				linesQueue <- &line
				break
			}
		}
		fmt.Println(stream.Name, "LINESD: WRITER: DONE")
	}

	if err == nil {
		select {
		case <-doneQueue:
			fmt.Println(stream.Name, "LINESD: DONE: ACK")
		}
	}
}

func (t *Server) Write(p []byte) (n int, err error) {
	err = nil
	n = 0

	if p != nil && len(p) > 0 {
		fmt.Println("LINESD: WRITER: WRITE", len(p))
		t.writerStreamChannel <- p
	}

	return len(p), err
}

func (t *Server) RunForever() {
	var err error

	flag.Parse()
	log.SetFlags(log.Ltime | log.Lshortfile | log.Lmicroseconds | log.Ldate)

	// stats:
	labels := []string{"name", "arg"}
	t.Stats.Counters = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: SERVICE_NAME + "_counters",
			Help: "counters"},
		labels,
	)
	err = prometheus.Register(t.Stats.Counters)
	CheckFatal(err)

	t.Stats.Gauges = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: SERVICE_NAME + "_gauges",
			Help: "gauges"},
		labels,
	)
	err = prometheus.Register(t.Stats.Gauges)
	CheckFatal(err)

	VersionExport := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: SERVICE_NAME + "_version",
			Help: "version number"},
		[]string{"version", "hash"})
	err = prometheus.Register(VersionExport)
	CheckFatal(err)

	VersionExport.WithLabelValues(VERSION_NUMBER, "").Inc()

	handlePanic := func(c http.ResponseWriter, req *http.Request) {
		r := recover()
		path := Prefix(req.URL.Path, 32)
		path = strings.Replace(path, "/", "_", -1)
		path = CleanupStringASCII(path, true)

		if r != nil {
			errorString := ""
			switch t := r.(type) {
			case string:
				CheckNotFatal(errors.New(t))
				errorString = t
			case error:
				CheckNotFatal(t)
				errorString = t.Error()
			default:
				errorString = "unknown error"
			}
			fmt.Println("ERROR:", errorString)
			http.Error(c, "error: "+errorString, http.StatusBadRequest)
			t.Stats.Counters.WithLabelValues("request_error"+path, errorString).Inc()
		} else {
			t.Stats.Counters.WithLabelValues("request_ok"+path, "").Inc()
		}
	}

	authenticate := func(c http.ResponseWriter, req *http.Request) bool {
		return true
	}

	http.HandleFunc("/ping",
		func(c http.ResponseWriter, req *http.Request) {
			c.Write([]byte("ok"))
		})

	http.HandleFunc("/version",
		func(c http.ResponseWriter, req *http.Request) {
			defer handlePanic(c, req)
			io.WriteString(c, VERSION_NUMBER+"\n")
		})

	http.HandleFunc("/",
		func(c http.ResponseWriter, req *http.Request) {
			defer handlePanic(c, req)
			io.WriteString(c, VERSION_NUMBER+"\n")
		})

	metricsHandler := promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{})
	http.HandleFunc("/metrics", func(c http.ResponseWriter, req *http.Request) {
		if authenticate(c, req) {
			metricsHandler.ServeHTTP(c, req)
		}
	})

	go func() {
		fmt.Println("=> LINESD: METRICS ADDRESS:", t.Config.Address)
		err = http.ListenAndServe(t.Config.Address, nil)
		CheckFatal(err)
		fmt.Println("=> LINESD: METRICS ADDRESS:", t.Config.Address, "DONE")
	}()

	defer func() {
		// always drain the batches
		for streamAddress, stream := range t.Config.Streams {
			if stream.batch == nil {
				continue
			}

			fmt.Println("=> LINESD: FINALIZE:", streamAddress)
			t.UploadBatch(stream.batch)
			fmt.Println("=> LINESD: FINALIZE:", streamAddress, "DONE.")
		}

		t.conc.Wait()
	}()

	var wg sync.WaitGroup
	for streamAddress, stream := range t.Config.Streams {
		wg.Add(1)
		sAddress := streamAddress
		sStream := stream
		go func() {
			defer wg.Done()
			t.ReadStream(&sAddress, sStream)
		}()
	}
	wg.Wait()
}
