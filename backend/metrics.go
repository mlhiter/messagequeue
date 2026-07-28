package main

import "context"

// FixedMetricsProvider is a small adapter for an authenticated metrics
// gateway. The gateway implementation can map keys to private PromQL, while
// this API exposes only MetricResponse and never the query text.
type FixedMetricsProvider struct {
	Values map[string]MetricResponse
}

func (p FixedMetricsProvider) Metrics(_ context.Context, _ string, name, key string) (MetricResponse, error) {
	if !metricKeys[key] {
		return MetricResponse{}, ErrInvalid
	}
	response, ok := p.Values[key]
	if !ok {
		return MetricResponse{Name: name, Key: key, Values: []MetricPoint{}, Degraded: true, Message: "metric is not available yet"}, nil
	}
	response.Name = name
	response.Key = key
	response.Degraded = false
	response.Message = ""
	return response, nil
}
