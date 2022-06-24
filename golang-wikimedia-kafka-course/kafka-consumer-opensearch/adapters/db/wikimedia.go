package db

import (
	"context"
	"errors"
	"io"
	"log"
	"strings"

	opensearchapi "github.com/opensearch-project/opensearch-go/v2/opensearchapi"
)

type Storage interface {
	Upsert(payload string, id string) error
}

type WikimediaStorage struct {
	opensearchClient *ElasticSearchClient
	ctx              context.Context
}

func NewWikimedia(client *ElasticSearchClient, ctx context.Context) *WikimediaStorage {
	return &WikimediaStorage{
		opensearchClient: client,
		ctx:              ctx,
	}
}

func (w *WikimediaStorage) Upsert(payload string, id string) error {
	data := strings.NewReader(payload)

	req := opensearchapi.IndexRequest{
		Index: w.opensearchClient.index,
		Body:  data,
	}

	if id != "" {
		req.DocumentID = id
	}

	res, err := req.Do(w.ctx, w.opensearchClient.client)
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
