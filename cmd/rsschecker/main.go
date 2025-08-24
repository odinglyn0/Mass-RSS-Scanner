package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"log"
	"os"
	"bytes"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	awsRegion              = "eu-west-1"
	dynamoTableName        = "Orkavi-RSS"
	s3Bucket               = "authentik-300m"
	s3Key                  = "300m.txt"
	scanPageLimit          = 2000
	scannerConcurrency     = 1024
	fetcherConcurrency     = 32768
	requestTimeoutSeconds  = 10
	connectTimeoutSeconds  = 5
	keepAliveSeconds       = 30
	maxIdleConns           = 100000
	maxIdleConnsPerHost    = 4096
	idleConnTimeoutSeconds = 90
	totalFeeds = 337_000_000
)

const (
	attrURL = "url"
)

const (
	awsAccessKeyID     = "YOUR_ACCESS_KEY"
	awsSecretAccessKey = "YOUR_SECRET_ACCESS_KEY"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func buildHTTPClient() *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   connectTimeoutSeconds * time.Second,
			KeepAlive: keepAliveSeconds * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          maxIdleConns,
		MaxIdleConnsPerHost:   maxIdleConnsPerHost,
		IdleConnTimeout:       idleConnTimeoutSeconds * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}
	return &http.Client{Transport: transport, Timeout: requestTimeoutSeconds * time.Second}
}

func newAWSConfig(ctx context.Context) aws.Config {
	baseCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(awsRegion))
	must(err)
	if awsAccessKeyID == "" || awsAccessKeyID == "REPLACE_WITH_KEY_ID" || awsSecretAccessKey == "" || awsSecretAccessKey == "REPLACE_WITH_SECRET" {
		return baseCfg
	}
	baseCfg.Credentials = credentials.NewStaticCredentialsProvider(awsAccessKeyID, awsSecretAccessKey, "")
	return baseCfg
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetOutput(os.Stdout)
	log.SetFlags(0)

	var activeCount uint64
	var failedCount uint64

	ctx := context.Background()
	cfg := newAWSConfig(ctx)
	ddb := dynamodb.NewFromConfig(cfg)
	s3c := s3.NewFromConfig(cfg)

	urlCh := make(chan string, 1_000_000)
	reportTicker := time.NewTicker(1 * time.Second)
	defer reportTicker.Stop()

	httpClient := buildHTTPClient()
	var fetchWG sync.WaitGroup
	fetchWG.Add(fetcherConcurrency)
	for i := 0; i < fetcherConcurrency; i++ {
		go func() {
			defer fetchWG.Done()
			for u := range urlCh {
				if checkURL(httpClient, u) {
					activeTotal := atomic.AddUint64(&activeCount, 1)
					failedTotal := atomic.LoadUint64(&failedCount)
					total := activeTotal + failedTotal
					percent := (float64(total) / totalFeeds) * 100
					log.Printf("✓ ACTIVE [%d/%d %.4f%%] active=%d failed=%d %s", total, totalFeeds, percent, activeTotal, failedTotal, u)
				} else {
					failedTotal := atomic.AddUint64(&failedCount, 1)
					activeTotal := atomic.LoadUint64(&activeCount)
					total := activeTotal + failedTotal
					percent := (float64(total) / totalFeeds) * 100
					log.Printf("✗ FAILED [%d/%d %.4f%%] active=%d failed=%d %s", total, totalFeeds, percent, activeTotal, failedTotal, u)
				}
			}
		}()
	}

	stopReport := make(chan struct{})
	go func() {
		for {
			select {
			case <-reportTicker.C:
				ac := atomic.LoadUint64(&activeCount)
				fc := atomic.LoadUint64(&failedCount)
				body := []byte(fmt.Sprintf("active=%d failed=%d\n", ac, fc))
				_, _ = s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s3Bucket), Key: aws.String(s3Key), Body: bytes.NewReader(body), ContentType: aws.String("text/plain")})
			case <-stopReport:
				return
			}
		}
	}()

	var scanWG sync.WaitGroup
	for s := 0; s < scannerConcurrency; s++ {
		scanWG.Add(1)
		segment := int32(s)
		total := int32(scannerConcurrency)
		go func() {
			defer scanWG.Done()
			scanSegment(ctx, ddb, segment, total, urlCh)
		}()
	}

	scanWG.Wait()
	close(urlCh)
	fetchWG.Wait()
	close(stopReport)

	ac := atomic.LoadUint64(&activeCount)
	fc := atomic.LoadUint64(&failedCount)
	final := []byte(fmt.Sprintf("active=%d failed=%d\n", ac, fc))
	_, _ = s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s3Bucket), Key: aws.String(s3Key), Body: bytes.NewReader(final), ContentType: aws.String("text/plain")})
}

func scanSegment(ctx context.Context, db *dynamodb.Client, segment, total int32, urlCh chan<- string) {
	var lastKey map[string]types.AttributeValue
	for {
		items, lk, err := scanPage(ctx, db, lastKey, segment, total)
		if err != nil {
			log.Println("dynamo_error", err)
			backoffSleep()
			continue
		}
		for _, item := range items {
			if u := extractURL(item); u != "" {
				urlCh <- u
			}
		}
		if len(lk) == 0 {
			return
		}
		lastKey = lk
	}
}

func backoffSleep() {
	base := 50 * time.Millisecond
	j := time.Duration(rand.Intn(200)) * time.Millisecond
	time.Sleep(base + j)
}

func scanPage(ctx context.Context, db *dynamodb.Client, startKey map[string]types.AttributeValue, segment, total int32) ([]map[string]types.AttributeValue, map[string]types.AttributeValue, error) {
	input := &dynamodb.ScanInput{
		TableName:              aws.String(dynamoTableName),
		ProjectionExpression:   aws.String("#u"),
		ExpressionAttributeNames: map[string]string{"#u": attrURL},
		Limit:                  aws.Int32(scanPageLimit),
		ConsistentRead:         aws.Bool(false),
		Segment:                aws.Int32(segment),
		TotalSegments:          aws.Int32(total),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityNone,
	}
	if len(startKey) > 0 {
		input.ExclusiveStartKey = startKey
	}
	out, err := db.Scan(ctx, input)
	if err != nil {
		return nil, startKey, err
	}
	return out.Items, out.LastEvaluatedKey, nil
}

func extractURL(item map[string]types.AttributeValue) string {
	av, ok := item[attrURL]
	if !ok {
		return ""
	}
	if s, ok := av.(*types.AttributeValueMemberS); ok {
		return strings.TrimSpace(s.Value)
	}
	if m, ok := av.(*types.AttributeValueMemberM); ok {
		if inner, ok := m.Value["S"]; ok {
			if sv, ok := inner.(*types.AttributeValueMemberS); ok {
				return strings.TrimSpace(sv.Value)
			}
		}
	}
	return ""
}

func checkURL(client *http.Client, u string) bool {
	if u == "" {
		return false
	}
	req, err := http.NewRequest("HEAD", u, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", "rss-checker/1.0")
	resp, err := client.Do(req)
	if err != nil {
		return tryGet(client, u)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode <= 399 {
		return true
	}
	if resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotImplemented {
		return tryGet(client, u)
	}
	return false
}

func tryGet(client *http.Client, u string) bool {
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", "rss-checker/1.0")
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode <= 399
} 