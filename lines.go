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
	"strings"
	"sync/atomic"
	"time"

	"github.com/korovkin/limiter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	_ "github.com/aws/aws-sdk-go/aws"
	_ "github.com/aws/aws-sdk-go/aws/session"
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
		":9310",
		"HTTP server address")

	env = flag.String(
		"env",
		"dev",
		"prod / dev")

	conc_limit = flag.Int(
		"conc_limit",
		8,
		"concurrency limit for processing the lines")
)

type ConfigStream struct {
	Name string `json:"name"`
}

type Config struct {
	AWSRegion    string                  `json:"aws_region"`
	AWSBucket    string                  `json:"aws_bucket"`
	AWSKeyPrefix string                  `json:"aws_key_prefix"`
	Streams      map[string]ConfigStream `json:"streams"`

	awsKeyPrefixEnv string
}

type Stats struct {
	Counters *prometheus.CounterVec `json:"-"`
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
}

func (s *Server) GenerateUniqueId(idType string) (string, time.Time) {
	var i = (atomic.AddUint32(&s.count, 1)) % 0xFFFF
	now := time.Now()
	return fmt.Sprintf("%04d%02d%02d_%02d%02d_%010X_m%04X_i%04X_%s",
		now.Year(),
		now.Month(),
		now.Day(),
		now.Hour(),
		now.Minute(),
		now.Nanosecond(),
		s.machineId,
		i,
		idType), now
}

func (t *Server) Initialize() {
	var err error

	err = ReadJsonFile(*config, &t.Config)
	CheckFatal(err)
	t.Config.awsKeyPrefixEnv = t.Config.AWSKeyPrefix + "/" + *env

	t.conc = limiter.NewConcurrencyLimiter(*conc_limit)

	log.Println("config:", ToJsonString(t.Config))

	t.Data = map[string]*LinesBatch{}

	t.hostname, _ = os.Hostname()
	t.machineId, err = Lower16BitPrivateIP()
	CheckFatal(err)
}

func (t *Server) UploadBatch(batch *LinesBatch) {
	s3Bucket := t.Config.AWSBucket
	s3Key := fmt.Sprintf(
		"%s/%s/%s.json",
		t.Config.awsKeyPrefixEnv,
		batch.Name,
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

	if err == nil {
		log.Println("=> Uploaded Batch:", fmt.Sprintf("s3://%s/%s", s3Bucket, s3Key))
	}
}

func (t *Server) ReadStdin() {
	var err error

	streamAddress := "stdin"
	stream, ok := t.Config.Streams[streamAddress]
	if !ok {
		log.Fatalln("no stdin stream")
	}

	reader := bufio.NewReader(os.Stdin)
	linesCounter := 0

	for err == nil {
		line, err := reader.ReadString('\n')
		now := time.Now()

		if err == io.EOF {
			log.Println("EOF.")
			break
		}

		CheckFatal(err)

		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		// fmt.Println("LINE:",
		// 	linesCounter,
		// 	"|",
		// 	line)

		batch := t.Data[streamAddress]
		if batch == nil {
			batch = &LinesBatch{}
			id, now := t.GenerateUniqueId(ID_LINESD)
			batch.Name = stream.Name
			batch.BatchId = id
			batch.TimestampStart = now.Unix()
			batch.TimestampEnd = now.Unix()
			t.Data[streamAddress] = batch
		}

		if batch.Lines == nil {
			batch.Lines = []*string{}
		}

		batch.Lines = append(batch.Lines, &line)
		batch.TimestampEnd = now.Unix()
		isBatchDone := false

		if !isBatchDone && len(batch.Lines) > 3 {
			isBatchDone = true
		} else if math.Abs(float64(batch.TimestampEnd)-float64(batch.TimestampStart)) >= 60.0 {
			isBatchDone = true
		}

		if isBatchDone {
			t.Data[streamAddress] = nil
			t.conc.Execute(func() {
				t.UploadBatch(batch)
			})
		}

		linesCounter += 1

		if err != nil {
			break
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

	server := &Server{}
	server.Initialize()

	// stats:
	labels := []string{"name", "arg"}
	server.Stats.Counters = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: SERVICE_NAME + "_counters",
			Help: "counters"},
		labels,
	)
	err = prometheus.Register(server.Stats.Counters)
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
			server.Stats.Counters.WithLabelValues("request_error"+path, errorString).Inc()
		} else {
			server.Stats.Counters.WithLabelValues("request_ok"+path, "").Inc()
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
		err = http.ListenAndServe(*address, nil)
		CheckFatal(err)
	}()

	t.ReadStdin()

	for streamAddress, batch := range t.Data {
		log.Println("=> Finalize:", streamAddress)
		t.UploadBatch(batch)
	}

	t.conc.Wait()

	log.Println("listen on:", *address)

}
