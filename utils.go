package linesd

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
)

func FromJsonBytes(buf []byte, o interface{}) error {
	err := json.Unmarshal(buf, &o)
	return err
}

func FromJsonString(buf string, o interface{}) error {
	err := json.Unmarshal([]byte(buf), &o)
	return err
}

func FromJson(buf []byte, o interface{}) error {
	err := json.Unmarshal(buf, &o)
	return err
}

func ToJsonBytes(v interface{}) []byte {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", " ")
	err := enc.Encode(v)
	if err == nil {
		return buf.Bytes()
	}
	return []byte("{}")
}

func ToJsonString(v interface{}) string {
	bytes := ToJsonBytes(v)
	return string(bytes)
}

func ToJsonBytesNoIndent(v interface{}) []byte {
	bytes, err := json.Marshal(v)
	CheckNotFatal(err)
	if err == nil {
		return bytes
	}
	return []byte("{}")
}

func ToJsonStringNoIndent(v interface{}) string {
	bytes := ToJsonBytesNoIndent(v)
	return string(bytes)
}

func CheckFatal(e error) error {
	if e != nil {
		debug.PrintStack()
		log.Println("CHECK: ERROR:", e)
		panic(e)
	}
	return e
}

func CheckNotFatal(e error) error {
	if e != nil {
		debug.PrintStack()
		log.Println("CHECK: ERROR:", e.Error())
	}
	return e
}

func ReadJsonFile(filename string, o interface{}) error {
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	err = json.Unmarshal(file, o)
	return err
}

func CompressBlob(in []byte) ([]byte, error) {
	var out bytes.Buffer
	w, err := gzip.NewWriterLevel(&out, gzip.BestCompression)
	CheckNotFatal(err)

	if err != nil {
		return out.Bytes(), err
	}
	defer w.Close()

	_, err = w.Write(in)
	CheckNotFatal(err)
	if err != nil {
		return out.Bytes(), err
	}

	w.Close()

	return out.Bytes(), err
}

const HTTP_OK_STATUS_MIN = 200
const HTTP_OK_STATUS_MAX = 300

func HttpGetJson(url string, o interface{}, username string, password string,
	timeout time.Duration) error {

	var err error = nil
	log.Println("HTTP_GET: JSON:", url)
	body := []byte{}

	if strings.HasPrefix(url, "http") {
		req, err := http.NewRequest("GET", url, nil)
		CheckNotFatal(err)
		if err != nil {
			return err
		}

		if username != "" || password != "" {
			req.SetBasicAuth(username, password)
		}

		client := http.Client{
			Timeout: timeout,
		}
		resp, err := client.Do(req)
		CheckNotFatal(err)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode < HTTP_OK_STATUS_MIN || resp.StatusCode >= HTTP_OK_STATUS_MAX {
			err = errors.New(fmt.Sprintf("ERROR: HTTP: CODE: %d", resp.StatusCode))
			CheckNotFatal(err)
			return err
		}

		body, err = ioutil.ReadAll(resp.Body)
		CheckNotFatal(err)
		if err != nil {
			return err
		}
	} else if strings.HasPrefix(url, "file://") {
		body, err = ioutil.ReadFile(url[len("file://"):])
		CheckNotFatal(err)
		if err != nil {
			return err
		}
	} else {
		err = errors.New("unknown URL: " + url)
		CheckNotFatal(err)
		return err
	}

	json.Unmarshal(body, o)
	CheckNotFatal(err)
	if err != nil {
		return err
	}

	return nil
}

func GetMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

func IsValidURL(urlString string) error {
	_, err := url.Parse(urlString)
	CheckFatal(err)
	return err
}

var FilterASCIIRE = regexp.MustCompile(`[^a-zA-Z_0-9\-]`)

func CleanupStringASCII(s string, isToLower bool) string {
	s = FilterASCIIRE.ReplaceAllString(s, "")
	if isToLower {
		s = strings.ToLower(s)
	}
	return s
}

const (
	CONTENT_TYPE      = "Content-Type"
	CONTENT_TYPE_AAC  = "audio/aac"
	CONTENT_TYPE_JPEG = "image/jpeg"
	CONTENT_TYPE_RAW  = "application/octet-stream"
	CONTENT_TYPE_BIN  = "application/octet-stream"
	CONTENT_TYPE_FLAC = "audio/flac"
	CONTENT_TYPE_XML  = "text/xml"
	CONTENT_TYPE_CSV  = "text/csv"
	CONTENT_TYPE_JSON = "application/json"
	CONTENT_TYPE_HTML = "text/html"
	CONTENT_TYPE_TXT  = "text/plain"
	CONTENT_TYPE_MP4  = "video/mp4"
	CONTENT_TYPE_PNG  = "image/png"
	CONTENT_TYPE_KML  = "application/vnd.google-earth.kml+xml"
	CONTENT_TYPE_GZ   = "application/x-gzip"
)

var DurationREValue = regexp.MustCompile(`(\d+).*`)
var DurationREMult = regexp.MustCompile(`\d+([sSmMhHdDwW])`)

func ParseDurationString(duration string) int {
	durationValue := 0
	durationMultiplier := 1

	duration = strings.TrimSpace(duration)

	res := DurationREValue.FindSubmatch([]byte(duration))
	if len(res) > 1 {
		durationValue, _ = strconv.Atoi(string(res[1]))
	}
	res = DurationREMult.FindSubmatch([]byte(duration))
	if len(res) > 1 {
		v := string(string(res[1]))
		switch string(v) {
		case "w":
			durationMultiplier = 7 * 24 * 60 * 60
		case "d":
			durationMultiplier = 24 * 60 * 60
		case "h":
			durationMultiplier = 60 * 60
		case "m":
			durationMultiplier = 60
		case "s":
			durationMultiplier = 1
		case "":
			durationMultiplier = 1
		}
	}
	return durationValue * durationMultiplier
}

const SEP = ":::"

func Implode(a, b string) string {
	return a + SEP + b
}

func Explode(x string) (a, b string) {
	l := strings.Split(x, SEP)
	if len(l) > 1 {
		return l[0], l[1]
	} else if len(l) > 0 {
		return l[0], ""
	}
	return "", ""
}

func ParseFloatOrDefault(val string, bits int, defaultVal float64) float64 {
	floatValue, e := strconv.ParseFloat(val, 64)
	if e != nil {
		return defaultVal
	}
	return floatValue
}

func ParseStringOrDefault(s *string, defaultVal string) string {
	if s == nil {
		return ""
	}
	return *s
}

func IsPrivateIPv4(ip net.IP) bool {
	return ip != nil &&
		(ip[0] == 10 || ip[0] == 172 && (ip[1] >= 16 && ip[1] < 32) || ip[0] == 192 && ip[1] == 168)
}

func PrivateIPv4() (net.IP, error) {
	as, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	for _, a := range as {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}

		ip := ipnet.IP.To4()
		if IsPrivateIPv4(ip) {
			return ip, nil
		}
	}
	return nil, errors.New("no private ip address")
}

func Lower16BitPrivateIP() (uint16, error) {
	ip, err := PrivateIPv4()
	if err != nil {
		return 0, err
	}

	return uint16(ip[2])<<8 + uint16(ip[3]), nil
}

func ToHexString(buf []byte) string {
	ret := ""
	for _, i := range buf {
		ret += fmt.Sprintf("%02x", i)
	}
	return ret
}

func Prefix(s string, l int) string {
	if l < len(s) {
		return s[0:l]
	}
	return s
}

func HumanFloat(value float64, units string) string {
	// one decimal point:
	value = float64(int64(value*10.0)) / 10.0
	return fmt.Sprintf("%s %s", humanize.Commaf(value), units)
}

func PercentBar(percent float64) string {
	if percent > 100.0 {
		percent = 100.0
	}
	if percent < 0 {
		percent = 0.0
	}
	numHashes := int(percent) / 10
	if numHashes < 0 {
		numHashes = 0
	}
	numSpaces := 10 - numHashes
	if numSpaces < 0 {
		numSpaces = 0
	}

	return fmt.Sprintf("**%.1f%s **  `[%s%s]`",
		percent,
		"%",
		strings.Repeat("#", numHashes),
		strings.Repeat(" ", numSpaces),
	)
}

var (
	ERR_INVALID_S3_URL = errors.New("S3 URL must be of form 's3://{bucket}/{key}'")
)

func ParseS3URL(urlStr string) (string, string, error) {
	if !strings.HasPrefix(urlStr, "s3://") {
		return "", "", ERR_INVALID_S3_URL
	}
	bucketAndKey := strings.SplitN(urlStr[5:], "/", 2)
	if len(bucketAndKey) != 2 {
		return "", "", ERR_INVALID_S3_URL
	}
	return bucketAndKey[0], bucketAndKey[1], nil
}
