package main

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	metricsLookback       = 30 * time.Minute
	metricsStep           = 30 * time.Second
	metricsRequestTimeout = 8 * time.Second
	maxMetricPoints       = 2048
)

// FixedMetricsProvider is a small adapter used by API contract tests.
type FixedMetricsProvider struct {
	Values map[string]MetricResponse
}

func (p FixedMetricsProvider) Metrics(_ context.Context, _ string, name, key string) (MetricResponse, error) {
	if !metricKeys[key] {
		return MetricResponse{}, ErrInvalid
	}
	response, ok := p.Values[key]
	if !ok {
		return degradedMetric(name, key, metricUnit(key), "metric data is not available yet"), nil
	}
	response.Name = name
	response.Key = key
	response.Degraded = false
	response.Message = ""
	return response, nil
}

// VictoriaMetricsProvider maps public metric keys to private, server-owned
// PromQL. Namespace and instance selectors are validated before interpolation.
type VictoriaMetricsProvider struct {
	BaseURL string
	Client  *http.Client
	Now     func() time.Time
}

func metricsProviderFromEnv() MetricsProvider {
	baseURL := strings.TrimSpace(os.Getenv("MESSAGEQUEUE_METRICS_URL"))
	if baseURL == "" {
		return UnavailableMetricsProvider{}
	}
	return &VictoriaMetricsProvider{
		BaseURL: baseURL,
		Client:  &http.Client{Timeout: metricsRequestTimeout},
		Now:     time.Now,
	}
}

func (p *VictoriaMetricsProvider) Metrics(ctx context.Context, namespace, name, key string) (MetricResponse, error) {
	unit := metricUnit(key)
	if !metricKeys[key] || !validDNSLabel(namespace) || !validDNSLabel(name) {
		return degradedMetric(name, key, unit, "metric request is not available"), nil
	}
	query, ok := fixedMetricQuery(key, namespace, name)
	if !ok {
		return degradedMetric(name, key, unit, "metric request is not available"), nil
	}
	endpoint, err := p.queryRangeURL(query)
	if err != nil {
		return degradedMetric(name, key, unit, "metrics provider is unavailable"), nil
	}

	requestContext, cancel := context.WithTimeout(ctx, metricsRequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, endpoint, nil)
	if err != nil {
		return degradedMetric(name, key, unit, "metrics provider is unavailable"), nil
	}
	request.Header.Set("Accept", "application/json")
	client := p.Client
	if client == nil {
		client = &http.Client{Timeout: metricsRequestTimeout}
	}
	response, err := client.Do(request)
	if err != nil {
		return degradedMetric(name, key, unit, "metrics provider is unavailable"), nil
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return degradedMetric(name, key, unit, "metrics provider is unavailable"), nil
	}

	var payload victoriaMetricsResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(&payload); err != nil {
		return degradedMetric(name, key, unit, "metrics provider returned invalid data"), nil
	}
	points, ok := metricPoints(payload)
	if !ok {
		return degradedMetric(name, key, unit, "metric data is not available yet"), nil
	}
	return MetricResponse{Name: name, Key: key, Unit: unit, Values: points}, nil
}

func (p *VictoriaMetricsProvider) queryRangeURL(query string) (string, error) {
	endpoint, err := url.Parse(strings.TrimSpace(p.BaseURL))
	if err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" {
		return "", ErrInvalid
	}
	path := strings.TrimRight(endpoint.Path, "/")
	switch {
	case strings.HasSuffix(path, "/api/v1/query_range"):
		endpoint.Path = path
	case strings.HasSuffix(path, "/api/v1"):
		endpoint.Path = path + "/query_range"
	default:
		endpoint.Path = path + "/api/v1/query_range"
	}
	now := time.Now
	if p.Now != nil {
		now = p.Now
	}
	end := now().UTC()
	values := endpoint.Query()
	values.Set("query", query)
	values.Set("start", strconv.FormatInt(end.Add(-metricsLookback).Unix(), 10))
	values.Set("end", strconv.FormatInt(end.Unix(), 10))
	values.Set("step", strconv.FormatInt(int64(metricsStep/time.Second), 10))
	endpoint.RawQuery = values.Encode()
	return endpoint.String(), nil
}

func fixedMetricQuery(key, namespace, name string) (string, bool) {
	namespaceSelector := `namespace="` + namespace + `"`
	clusterSelector := namespaceSelector + `,strimzi_io_cluster="` + name + `"`
	brokerContainerSelector := namespaceSelector + `,pod=~"^` + name + `-` + name + `-pool-[0-9]+$",container="kafka"`
	queries := map[string]string{
		"broker_count":     `count(count by (pod) (kafka_server_replicamanager_underreplicatedpartitions{` + clusterSelector + `}))`,
		"partition_health": `max(kafka_server_replicamanager_underreplicatedpartitions{` + clusterSelector + `})`,
		"throughput":       `sum(rate(kafka_server_brokertopicmetrics_messagesin_total{` + clusterSelector + `}[5m]))`,
		"consumer_lag":     `sum(kafka_consumergroup_lag{` + clusterSelector + `})`,
		"cpu":              `sum(rate(container_cpu_usage_seconds_total{` + brokerContainerSelector + `}[5m]))`,
		"memory":           `sum(container_memory_working_set_bytes{` + brokerContainerSelector + `}) / 1048576`,
		"storage":          `sum(kafka_log_log_size{` + clusterSelector + `}) / 1073741824`,
	}
	query, ok := queries[key]
	return query, ok
}

func metricUnit(key string) string {
	units := map[string]string{
		"broker_count":     "count",
		"partition_health": "count",
		"throughput":       "messages/s",
		"consumer_lag":     "messages",
		"cpu":              "cores",
		"memory":           "Mi",
		"storage":          "Gi",
	}
	return units[key]
}

func degradedMetric(name, key, unit, message string) MetricResponse {
	return MetricResponse{Name: name, Key: key, Unit: unit, Values: []MetricPoint{}, Degraded: true, Message: message}
}

type victoriaMetricsResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Values [][]json.RawMessage `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func metricPoints(payload victoriaMetricsResponse) ([]MetricPoint, bool) {
	if payload.Status != "success" || payload.Data.ResultType != "matrix" || len(payload.Data.Result) != 1 {
		return nil, false
	}
	values := payload.Data.Result[0].Values
	if len(values) == 0 || len(values) > maxMetricPoints {
		return nil, false
	}
	points := make([]MetricPoint, 0, len(values))
	for _, rawPoint := range values {
		if len(rawPoint) != 2 {
			return nil, false
		}
		var timestamp float64
		if err := json.Unmarshal(rawPoint[0], &timestamp); err != nil || math.IsNaN(timestamp) || math.IsInf(timestamp, 0) {
			return nil, false
		}
		var rawValue string
		if err := json.Unmarshal(rawPoint[1], &rawValue); err != nil {
			return nil, false
		}
		value, err := strconv.ParseFloat(rawValue, 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, false
		}
		seconds, fraction := math.Modf(timestamp)
		pointTime := time.Unix(int64(seconds), int64(fraction*float64(time.Second))).UTC()
		points = append(points, MetricPoint{Timestamp: pointTime.Format(time.RFC3339Nano), Value: value})
	}
	return points, true
}
