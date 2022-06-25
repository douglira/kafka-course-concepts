package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	opensearchapi "github.com/opensearch-project/opensearch-go/v2/opensearchapi"
)

type Storage interface {
	Upsert(ctx context.Context, payload string, id string) error
	BulkUpsert(ctx context.Context, payload string) error
}

type WikimediaStorage struct {
	opensearchClient *ElasticSearchClient
}

func NewWikimedia(client *ElasticSearchClient) *WikimediaStorage {
	return &WikimediaStorage{
		opensearchClient: client,
	}
}

func (w *WikimediaStorage) Upsert(ctx context.Context, payload string, id string) error {
	data := strings.NewReader(payload)

	req := opensearchapi.IndexRequest{
		Index: w.opensearchClient.index,
		Body:  data,
	}

	if id != "" {
		req.DocumentID = id
	}

	res, err := req.Do(ctx, w.opensearchClient.client)
	if err != nil {
		log.Println("[WikimediaStorage] Error at save", err, res)
		return err
	}
	if res.StatusCode != 201 && res.StatusCode != 200 {
		buf := new(strings.Builder)
		io.Copy(buf, res.Body)
		log.Println("[WikimediaStorage] Error at save", err, res, res.Body, res.StatusCode)
		return errors.New(buf.String())
	}
	log.Println("Save successfully", res)
	return nil
}

func getBulkAppendCount(payloadMap map[string]string) (string, error) {
	var count int32
	c, ok := payloadMap["count"]
	if ok {
		countInt, err := strconv.Atoi(c)
		if err != nil {
			return "", err
		}
		count = int32(countInt) + 1
	} else {
		count = 1
	}
	countString := strconv.Itoa(int(count))
	return countString, nil
}

func getBulkAppendPayload(payloadMap map[string]string, data string, id string, operation string) string {
	var doc string
	payload, ok := payloadMap["payload"]

	doc = fmt.Sprintf(`{"%s":{"_id":"%s"}}`, "create", id)

	if ok {
		doc = fmt.Sprint(payload, "\n", doc)
	}

	doc = fmt.Sprint(doc, "\n", data, "\n")

	return doc
}

func AppendBulkInsert(payloadMap map[string]string, data string, id string) (map[string]string, error) {
	result := map[string]string{}
	count, err := getBulkAppendCount(payloadMap)
	if err != nil {
		return nil, err
	}
	result["count"] = count

	payload := getBulkAppendPayload(payloadMap, data, id, "create")
	result["payload"] = payload
	return result, nil
}

func (w *WikimediaStorage) BulkUpsert(ctx context.Context, payload string) error {
	data := strings.NewReader(payload)

	req := opensearchapi.BulkRequest{
		Index: w.opensearchClient.index,
		Body:  data,
	}

	res, err := req.Do(ctx, w.opensearchClient.client)
	if err != nil {
		log.Println("[WikimediaStorage] Error at save", err, res)
		return err
	}
	if res.StatusCode != 201 && res.StatusCode != 200 {
		buf := new(strings.Builder)
		io.Copy(buf, res.Body)
		log.Println("[WikimediaStorage] Error at save", err, res, res.Body, res.StatusCode)
		return errors.New(buf.String())
	}
	log.Println("Save successfully", res)
	return nil
}
