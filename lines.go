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
	"path/filepath"
	"runtime"
	"sort"
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
const VERSION_NUMBER = "0.0.5"
const ID_LINESD = "LINESD"
const LOG_FILES_MAX_NUM = 10
const LOG_FILES_EXT = ".log"
const LOG_FILES_CURRENT_LOG = "current" + LOG_FILES_EXT

type ConfigStream struct {
	Name         string `json:"name"`
	TaiCmd       string `json:"tail_cmd"`
	TailFilename string `json:"tail_filename"`
	IsStdin      bool   `json:"is_stdin"`

	// private:
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

	// HTTP timeout:
	TimeoutSeconds int `json:"timeout_seconds"`
	// log to local files as well:
	LogFilesFolder string `json:"log_files_folder"`
	// max local file size in bytes:
	LogFilesFileSizeBytes int `json:"log_files_size_bytes"`
	// S3 destination prefix:
	AWSKeyPrefixEnv string
}

type Stats struct {
	Counters *prometheus.CounterVec `json:"-"`
	Gauges   *prometheus.GaugeVec   `json:"-"`
}

type LinesBatch struct {
	Name           string    `json:"name"`
	Hostname       string    `json:"hostname"`
	BatchId        string    `json:"batch_id"`
	TimestampStart int64     `json:"ts_start"`
	TimestampEnd   int64     `json:"ts_end"`
	Lines          []*string `json:"lines"`
}

type Server struct {
	Stats  Stats
	Config Config
	// conc limiter to upload files to S3/ES
	conc *limiter.ConcurrencyLimiter
	// unique / atomic id:
	uniqueId uint32
	// hostname:
	hostname string
	// machine id;
	machineId uint16
	// monotonic line counter:
	linesCounter int64
	// current opened file:
	currentFile          *os.File
	currentFilename      string
	currentFileLock      sync.Mutex
	currnetFileSizeBytes int
}

func (s *Server) GenerateUniqueId(idType string) (string, string, time.Time) {
	var i = (atomic.AddUint32(&s.uniqueId, 1)) % 0xFFFF
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
		fmt.Println("LINESD: FATAL: must set batch_size_in_lines or batch_size_in_seconds")
		return
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

	if t.Config.ConcLimit <= 0 {
		t.Config.ConcLimit = 8
	}

	if t.Config.TimeoutSeconds <= 0 {
		t.Config.TimeoutSeconds = 60
	}

	if t.Config.LogFilesFolder != "" {
		os.MkdirAll(t.Config.LogFilesFolder, 0777)
	}

	const MIN_LOG_FILE_SIZE = 1024

	if t.Config.LogFilesFileSizeBytes <= MIN_LOG_FILE_SIZE {
		t.Config.LogFilesFileSizeBytes = MIN_LOG_FILE_SIZE
	}

	t.Config.AWSKeyPrefixEnv = t.Config.AWSKeyPrefix + "/" + t.Config.Env
	t.AppendDebugLineToLogFile("LINESD: INITIALIZE: CONFIG: " + ToJsonString(t.Config))

	t.hostname, _ = os.Hostname()
	t.machineId, err = Lower16BitPrivateIP()
	t.conc = limiter.NewConcurrencyLimiter(t.Config.ConcLimit)

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
			t.Config.AWSKeyPrefixEnv,
			t.hostname,
			batch.Name,
			batch.BatchId[0:8],
			batch.BatchId,
		)

		_, _, err := S3PutBlob(
			&t.Config.AWSRegion,
			time.Duration(t.Config.TimeoutSeconds)*time.Second,
			s3Bucket,
			ToJsonBytes(batch),
			s3Key,
			CONTENT_TYPE_JSON,
		)
		CheckNotFatal(err)

		if t.Config.IsShowBatches {
			log.Println("LINESD: BATCH:", ToJsonString(batch))
		}

		if err == nil {
			log.Println("LINESD: S3:", batch.Name, len(batch.Lines), fmt.Sprintf("s3://%s/%s", s3Bucket, s3Key))
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
			time.Duration(t.Config.TimeoutSeconds)*time.Second,
			t.Config.AWSElasticSearchURL,
			fmt.Sprintf("tlines-%s-%s", t.hostname, batch.Name),
			t.Config.Env,
			"line",
			items)
		CheckNotFatal(err)

		if err == nil {
			log.Println("LINESD: ES:", batch.Name, len(batch.Lines), batch.BatchId)
			t.Stats.Counters.WithLabelValues("log_batches_es_ok", "").Inc()
		} else {
			t.Stats.Counters.WithLabelValues("log_batches_es_err", CleanupStringASCII(err.Error(), true)).Inc()
		}
	}
}

func (t *Server) ProcessLineToLocalFile(line *string) {
	if line == nil || len(*line) <= 0 {
		return
	}
	var e error
	var bytesAdded int

	// IMPORTANT: this function is used by log.Write, thus log.Print* can't be called from this function

	t.currentFileLock.Lock()
	defer t.currentFileLock.Unlock()

	// open a new file:
	if t.currentFile == nil {
		filename, _, _ := t.GenerateUniqueId("LOG")
		filename += LOG_FILES_EXT
		absFilename := fmt.Sprintf("%s/%s", t.Config.LogFilesFolder, filename)

		// convert to ABS filename:
		absFilename, e = filepath.Abs(absFilename)
		CheckNotFatal(e)

		fmt.Println("LINESD: NEW FILE:", absFilename)
		go func() {
			log.Println("LINESD: NEW FILE:", absFilename)
		}()

		t.currentFile, e = os.OpenFile(absFilename, os.O_WRONLY|os.O_CREATE, 0777)
		t.currnetFileSizeBytes = 0
		CheckNotFatal(e)
		if e == nil {
			t.currentFilename = absFilename
		}

		// update "<log_folder>/current" link
		absFilenameCurrent := fmt.Sprintf("%s/%s", t.Config.LogFilesFolder, LOG_FILES_CURRENT_LOG)
		os.Remove(absFilenameCurrent)
		os.Symlink(absFilename, absFilenameCurrent)

		t.Stats.Counters.WithLabelValues("log_lines_files_opened", "").Inc()
	}

	// append to the file:
	if t.currentFile != nil {
		bytesAdded, e = t.currentFile.Write([]byte(*line + "\n"))
		CheckNotFatal(e)

		if e == nil {
			t.currnetFileSizeBytes += bytesAdded
			t.currentFile.Sync()
		}
	}

	// check if the file needs to be closed:
	isCleanLogFiles := false
	if t.currentFile != nil && t.currnetFileSizeBytes >= t.Config.LogFilesFileSizeBytes {
		currentFilename := t.currentFilename
		t.currentFilename = ""
		t.currnetFileSizeBytes = 0
		t.currentFile.Close()
		t.currentFile = nil
		fmt.Println("LINESD: CLOSE FILE:", currentFilename)
		go func() {
			log.Println("LINESD: CLOSE FILE:", currentFilename)
		}()
		t.Stats.Counters.WithLabelValues("log_lines_files_closed", "").Inc()
		isCleanLogFiles = true
	}

	// walk the folder and cleanup old files
	if isCleanLogFiles {
		currentLogFiles := []string{}
		filepath.Walk(
			t.Config.LogFilesFolder,
			func(path string, info os.FileInfo, err error) error {
				CheckNotFatal(err)
				_, filename := filepath.Split(path)
				if info.IsDir() && path != t.Config.LogFilesFolder {
					return filepath.SkipDir
				}
				if filepath.Ext(filename) != LOG_FILES_EXT || filename == LOG_FILES_CURRENT_LOG {
					return nil
				}
				if info.IsDir() || !info.Mode().IsRegular() {
					return nil
				}
				currentLogFiles = append(currentLogFiles, path)
				return nil
			})

		// sort by filename (thus by time)
		sort.Sort(sort.StringSlice(currentLogFiles))

		for len(currentLogFiles) > LOG_FILES_MAX_NUM {
			oldestFilename := currentLogFiles[0]
			fmt.Println("LINESD: DELETE FILE:", len(currentLogFiles), oldestFilename)
			go func() {
				log.Println("LINESD: DELETE FILE:", oldestFilename)
			}()
			os.RemoveAll(oldestFilename)
			currentLogFiles = currentLogFiles[1:]
			t.Stats.Counters.WithLabelValues("log_lines_files_deleted", "").Inc()
		}
	}
}

func (t *Server) ProcessLine(streamAddress *string, stream *ConfigStream, line *string) {
	now := time.Now()

	if line != nil && t.Config.IsShowLines {
		fmt.Println("LINESD: LINE:",
			t.linesCounter,
			stream.Name,
			"|",
			*line)
	}

	t.Stats.Counters.WithLabelValues("log_lines_in", "").Inc()

	t.ProcessLineToLocalFile(line)

	batch := stream.batch
	if stream.batch == nil {
		batch = &LinesBatch{}
		id, _, now := t.GenerateUniqueId(ID_LINESD)
		batch.Name = stream.Name
		batch.Hostname = t.hostname
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
			log.Println("LINESD: PROGRESS:", humanize.Comma(int64(t.linesCounter)))
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
		fmt.Println("LINESD: running tail:", command.Path, command.Args)
		inputStream, err = command.StdoutPipe()
		CheckFatal(err)

		// run the tailer:
		err = command.Start()
		CheckFatal(err)
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

	if err == nil {
		select {
		case <-doneQueue:
			fmt.Println(stream.Name, "LINESD: DONE: ACK")
		}
	}
}

func (t *Server) AppendDebugLineToLogFile(line string) {
	now := time.Now()
	_, fileName, lineNumber, ok := runtime.Caller(1)
	if !ok {
		fileName = "unknown"
		lineNumber = 0
	}
	_, fileName = filepath.Split(fileName)
	lineWithPrefix := fmt.Sprintf("LINESD: %02d%02d%02d_%02d%02d%02d %.3f %-12s %5d :: %s",
		now.Year(),
		now.Month(),
		now.Day(),
		now.Hour(),
		now.Minute(),
		now.Second(),
		float64(now.UnixNano())/1000000000.0,
		fileName,
		lineNumber,
		line,
	)
	t.ProcessLineToLocalFile(&lineWithPrefix)
}

// This will be called from log.print* function as log.SetOutput was called on it
func (t *Server) Write(p []byte) (n int, err error) {
	err = nil
	n = 0
	if p != nil && len(p) > 0 {
		t.AppendDebugLineToLogFile(strings.TrimSpace(string(p)))
	}

	return len(p), err
}

func (t *Server) RunForever() {
	var err error

	flag.Parse()
	log.SetFlags(log.Ltime | log.Lshortfile | log.Lmicroseconds | log.Ldate)

	// make an attempt to output lines to the log files as well:
	log.SetOutput(io.MultiWriter(os.Stderr, t))

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
			fmt.Println("LINESD: ERROR:", errorString)
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
		fmt.Println("LINESD: METRICS ADDRESS:", t.Config.Address)
		err = http.ListenAndServe(t.Config.Address, nil)
		CheckFatal(err)
		fmt.Println("LINESD: METRICS ADDRESS:", t.Config.Address, "DONE")
	}()

	defer func() {
		// always drain the batches
		for streamAddress, stream := range t.Config.Streams {
			if stream.batch == nil {
				continue
			}

			fmt.Println("LINESD: FINALIZE:", streamAddress)
			t.UploadBatch(stream.batch)
			fmt.Println("LINESD: FINALIZE:", streamAddress, "DONE.")
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
