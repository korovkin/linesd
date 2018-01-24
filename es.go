package linesd

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type ESWrapper struct {
	Doc       interface{} `json:"doc"`
	DocUpsert bool        `json:"doc_as_upsert"`
}

type ESResponse struct {
	Status int `json:"status"`
}

func ElasticSearchPut(endpoint string, id string, indexPrefix string, estype string, record interface{}) error {
	curTime := time.Now()
	index := indexPrefix + "-" + estype + "-" + curTime.Format("200601")

	url := endpoint + "/" + index + "/" + estype + "/" + id + "/_update"

	wrapper := ESWrapper{record, true}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(ToJsonBytes(wrapper)))
	CheckNotFatal(err)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	CheckNotFatal(err)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	CheckNotFatal(err)

	if err != nil {
		if bodyBytes != nil {
			log.Println("ERROR: ReadAll:", string(bodyBytes))
		}
		return err
	}

	var esResp ESResponse
	err = json.Unmarshal(bodyBytes, &esResp)
	CheckNotFatal(err)

	if esResp.Status != 0 {
		log.Println(string(bodyBytes))
	}
	return err
}
