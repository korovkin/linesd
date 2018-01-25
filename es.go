package linesd

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

func ElasticSearchPut(endpoint string, indexPrefix string, env string, itemType string, items map[string]interface{}) error {
	curTime := time.Now()
	index := indexPrefix + "-" + env + "-" + itemType + "-" + curTime.Format("200601")
	requestBody := bytes.NewBuffer([]byte{})

	for itemId, item := range items {
		// { "update" : {"_id" : "1", "_type" : "type1", "_index" : "index1", "retry_on_conflict" : 3} }
		update := map[string]map[string]interface{}{
			"update": {
				"_id":    itemId,
				"_type":  itemType,
				"_index": index,
				// "retry_on_conflict": 3,
			},
		}
		requestBody.WriteString(ToJsonStringNoIndent(update))
		requestBody.WriteString("\n")

		// { "doc" : {"field" : "value"} }
		doc := map[string]interface{}{
			"doc":           item,
			"doc_as_upsert": true,
		}
		requestBody.WriteString(ToJsonStringNoIndent(doc))
		requestBody.WriteString("\n")

		log.Println("ES:",
			ToJsonStringNoIndent(update),
			ToJsonStringNoIndent(doc))
	}

	req, err := http.NewRequest("POST", endpoint+"/_bulk", requestBody)
	CheckNotFatal(err)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", CONTENT_TYPE_JSON)

	client := &http.Client{}
	resp, err := client.Do(req)
	CheckNotFatal(err)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	responseBodyBytes, err := ioutil.ReadAll(resp.Body)
	CheckNotFatal(err)

	if responseBodyBytes != nil {
		log.Println("ES: _bulk ERROR:", string(responseBodyBytes))
		esResp := map[string]interface{}{}
		err = json.Unmarshal(responseBodyBytes, &esResp)
		CheckNotFatal(err)

		log.Println("ES: RESPONSE:", ToJsonString(esResp))
	}

	return err
}
