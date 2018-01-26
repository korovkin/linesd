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
	"os/signal"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/korovkin/limiter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const SERVICE_NAME = "linesd"
const VERSION_NUMBER = "0.0.1"
const ID_LINESD = "LINESD"

var (
	config = flag.String(
		"config",
		"config.json",
		"config file")

	version = flag.Bool(
		"version",
		false,
		"show long version string")

	address = flag.String(
		"address",
		":9400",
		"HTTP server address")

	env = flag.String(
		"env",
		"dev",
		"prod / dev")

	conc_limit = flag.Int(
		"conc_limit",
		8,
		"concurrency limit for processing the lines")

	is_show_lines = flag.Bool(
		"is_show_lines",
		false,
		"log lines to stdout",
	)

	is_show_batches = flag.Bool(
		"is_show_batches",
		false,
		"print the uploaded batches")

	batch_size_seconds = flag.Int(
		"batch_size_seconds",
		180,
		"batch size in seconds",
	)

	batch_size_lines = flag.Int(
		"batch_size_lines",
		5000,
		"batch size in lines",
	)

	progress = flag.Int(
		"progress",
		100,
		"progress debug prints",
	)
)

type ConfigStream struct {
	Name string `json:"name"`
}

type Config struct {
	AWSRegion           string                  `json:"aws_region"`
	AWSBucket           string                  `json:"aws_bucket"`
	AWSKeyPrefix        string                  `json:"aws_key_prefix"`
	AWSElasticSearchURL string                  `json:"aws_elastic_search"`
	Streams             map[string]ConfigStream `json:"streams"`

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
	Stats     Stats
	Config    Config
	conc      *limiter.ConcurrencyLimiter
	count     uint32
	hostname  string
	machineId uint16
	Data      map[string]*LinesBatch

	linesCounter int64
}

func (s *Server) GenerateUniqueId(idType string) (string, string, time.Time) {
	var i = (atomic.AddUint32(&s.count, 1)) % 0xFFFF
	now := time.Now()
	id := fmt.Sprintf("%04d%02d%02d_%02d%02d_%010X_m%04X_i%04X_%s",
		now.Year(),
		now.Month(),
		now.Day(),
		now.Hour(),
		now.Minute(),
		now.Nanosecond(),
		s.machineId,
		i,
		idType)
	return id, id[0:8], now
}

func (t *Server) Initialize() {
	var err error

	err = ReadJsonFile(*config, &t.Config)
	CheckFatal(err)
	t.Config.awsKeyPrefixEnv = t.Config.AWSKeyPrefix + "/" + *env

	t.conc = limiter.NewConcurrencyLimiter(*conc_limit)

	log.Println("LINESD: config:", ToJsonString(t.Config))

	t.Data = map[string]*LinesBatch{}

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

		if *is_show_batches {
			log.Println("BATCH:", ToJsonString(batch))
		}

		if err == nil {
			log.Println("LINESD: S3 UPLOADED:", fmt.Sprintf("s3://%s/%s", s3Bucket, s3Key))
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
			*env,
			"line",
			items)
		CheckNotFatal(err)

		if err == nil {
			log.Println("LINESD: ES UPLOADED:", batch.BatchId)
			t.Stats.Counters.WithLabelValues("log_batches_es_ok", "").Inc()
		} else {
			t.Stats.Counters.WithLabelValues("log_batches_es_err", CleanupStringASCII(err.Error(), true)).Inc()
		}
	}
}

func (t *Server) ProcessLine(streamAddress *string, stream *ConfigStream, line *string) {
	now := time.Now()

	if line != nil && *is_show_lines {
		fmt.Println("LINE:",
			t.linesCounter,
			"|",
			line)
	}

	t.Stats.Counters.WithLabelValues("log_lines_in", "").Inc()

	batch := t.Data[*streamAddress]
	if batch == nil {
		batch = &LinesBatch{}
		id, _, now := t.GenerateUniqueId(ID_LINESD)
		batch.Name = stream.Name
		batch.BatchId = id
		batch.TimestampStart = now.Unix()
		batch.TimestampEnd = now.Unix()
		t.Data[*streamAddress] = batch
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

	if *batch_size_lines > 0 && !isBatchDone &&
		len(batch.Lines) > *batch_size_lines {

		isBatchDone = true
	} else if *batch_size_seconds > 0 &&
		len(batch.Lines) > 0 &&
		math.Abs(float64(batch.TimestampEnd)-float64(batch.TimestampStart)) >= float64(*batch_size_seconds) {

		isBatchDone = true
	}

	if line != nil {
		t.linesCounter += 1
		if (t.linesCounter % int64(*progress)) == 0 {
			log.Println("=> LINESD:",
				"lines:", humanize.Comma(int64(t.linesCounter)),
			)
		}
	}

	if isBatchDone {
		t.Data[*streamAddress] = nil
		t.conc.Execute(func() {
			t.UploadBatch(batch)
		})
	}

}

func (t *Server) ReadStdin() {
	var err error
	linesQueue := make(chan *string, 1000)
	doneQueue := make(chan bool, 1)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	signal.Notify(signals, os.Kill)
	streamAddress := "stdin"
	stream, ok := t.Config.Streams[streamAddress]

	// processor:
	if err == nil {
		go func() {
			isKeepWorking := true
			if !ok {
				log.Fatalln("no stdin stream")
			}
			for isKeepWorking {
				select {
				case <-doneQueue:
					isKeepWorking = false
					break // the loop
				case <-signals:
					log.Println("EOF. SIGNAL.")
					isKeepWorking = false
					break

				case line := <-linesQueue:
					t.ProcessLine(&streamAddress, &stream, line)
				case <-time.After(time.Second * 10):
					t.ProcessLine(&streamAddress, &stream, nil)
				}
			}
		}()
	}

	// reader:
	if err == nil {
		reader := bufio.NewReader(os.Stdin)
		for true {
			line, err := reader.ReadString('\n')
			if err == io.EOF {
				log.Println("LINESD: EOF.")
				close(linesQueue)
				break
			}
			CheckFatal(err)
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				continue
				break
			}
			linesQueue <- &line
		}
	}
}

func (t *Server) RunForever() {
	var err error

	flag.Parse()
	log.SetFlags(log.Ltime | log.Lshortfile | log.Lmicroseconds | log.Ldate)

	if *version {
		fmt.Println(VERSION_NUMBER)
		return
	}
	log.Println("proc version:", VERSION_NUMBER)

	if *env != "dev" && *env != "prod" {
		log.Fatalln("FATAL: invalid env:", *env)
		return
	}

	if *batch_size_lines == 0 && *batch_size_seconds == 0 {
		log.Fatalln("must pick at least one of batch_size_seconds / batch_size_lines")
	}

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
			log.Println("ERROR:", errorString)
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
		log.Println("=> LINESD: METRICS ADDRESS:", *address)
		err = http.ListenAndServe(*address, nil)
		CheckFatal(err)
	}()

	defer func() {
		// always drain the batches
		for streamAddress, batch := range t.Data {
			log.Println("=> LINESD: FINALIZE:", streamAddress)
			t.UploadBatch(batch)
			log.Println("=> LINESD: FINALIZE:", streamAddress, "DONE.")
		}

		t.conc.Wait()
	}()

	// read stdin:
	t.ReadStdin()

}
