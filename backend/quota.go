package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

const (
	defaultCreateEngine         = "kafka"
	defaultCreateKafkaVersion   = "3.9.0"
	defaultCreateBrokerReplicas = 1
	defaultCreateBrokerCPU      = "500m"
	defaultCreateBrokerMemory   = "1Gi"
	defaultCreateStorageSize    = "10Gi"
	defaultCreateDeletionPolicy = "Retain"
)

type QuotaProvider interface {
	Quota(context.Context, string) (QuotaResponse, error)
}

type QuotaResponse struct {
	Namespace string      `json:"namespace"`
	Items     []QuotaItem `json:"items"`
	Degraded  bool        `json:"degraded,omitempty"`
	Message   string      `json:"message,omitempty"`
}

type QuotaItem struct {
	Type      string  `json:"type"`
	Used      float64 `json:"used"`
	Limit     float64 `json:"limit"`
	Available float64 `json:"available"`
	Unit      string  `json:"unit"`
}

type ResourceEstimate struct {
	Brokers  int32   `json:"brokers"`
	CPU      float64 `json:"cpu"`
	Memory   float64 `json:"memory"`
	Storage  float64 `json:"storage"`
	CPUUnit  string  `json:"cpuUnit"`
	DataUnit string  `json:"dataUnit"`
}

func applyProductDefaults(spec MessageQueueSpec) MessageQueueSpec {
	if strings.TrimSpace(spec.Engine) == "" {
		spec.Engine = defaultCreateEngine
	}
	if strings.TrimSpace(spec.Kafka.Version) == "" && strings.TrimSpace(spec.Version) == "" {
		spec.Kafka.Version = defaultCreateKafkaVersion
	}
	if spec.Kafka.Replicas == 0 && spec.Replicas == 0 {
		spec.Kafka.Replicas = defaultCreateBrokerReplicas
	}
	if strings.TrimSpace(spec.Resources.CPU) == "" {
		spec.Resources.CPU = defaultCreateBrokerCPU
	}
	if strings.TrimSpace(spec.Resources.Memory) == "" {
		spec.Resources.Memory = defaultCreateBrokerMemory
	}
	if strings.TrimSpace(spec.Storage.Size) == "" {
		spec.Storage.Size = defaultCreateStorageSize
	}
	if strings.TrimSpace(spec.DeletionPolicy) == "" {
		spec.DeletionPolicy = defaultCreateDeletionPolicy
	}
	return spec
}

func resourceEstimate(spec MessageQueueSpec) (ResourceEstimate, error) {
	spec = applyProductDefaults(spec)
	replicas := spec.Kafka.Replicas
	if replicas == 0 {
		replicas = spec.Replicas
	}
	if replicas == 0 {
		replicas = defaultCreateBrokerReplicas
	}
	cpu, err := cpuMilli(strings.TrimSpace(spec.Resources.CPU))
	if err != nil {
		return ResourceEstimate{}, err
	}
	memory, err := quantityBytes(strings.TrimSpace(spec.Resources.Memory))
	if err != nil {
		return ResourceEstimate{}, err
	}
	storage, err := quantityBytes(strings.TrimSpace(spec.Storage.Size))
	if err != nil {
		return ResourceEstimate{}, err
	}
	return ResourceEstimate{
		Brokers:  replicas,
		CPU:      roundQuantity(float64(replicas)*cpu/1000, 3),
		Memory:   roundQuantity(float64(replicas)*float64(memory)/float64(gib), 3),
		Storage:  roundQuantity(float64(replicas)*float64(storage)/float64(gib), 3),
		CPUUnit:  "cores",
		DataUnit: "Gi",
	}, nil
}

func (s *Server) preflightCreate(ctx context.Context, namespace string, spec MessageQueueSpec) error {
	if s.Quota == nil {
		return nil
	}
	quota, err := s.Quota.Quota(ctx, namespace)
	if err != nil {
		return err
	}
	if quota.Degraded {
		return nil
	}
	estimate, err := resourceEstimate(spec)
	if err != nil {
		return err
	}
	for _, item := range quota.Items {
		switch item.Type {
		case "cpu":
			if quotaExceeded(item, estimate.CPU) {
				return quotaError("CPU", estimate.CPU, item.Available, item.Unit)
			}
		case "memory":
			if quotaExceeded(item, estimate.Memory) {
				return quotaError("memory", estimate.Memory, item.Available, item.Unit)
			}
		case "storage":
			if quotaExceeded(item, estimate.Storage) {
				return quotaError("storage", estimate.Storage, item.Available, item.Unit)
			}
		}
	}
	return nil
}

func quotaExceeded(item QuotaItem, requested float64) bool {
	return item.Limit > 0 && requested > item.Available
}

func quotaError(kind string, requested, available float64, unit string) error {
	return publicError{
		err:     ErrQuotaExceeded,
		status:  http.StatusForbidden,
		code:    "quota_exceeded",
		message: fmt.Sprintf("%s quota is not sufficient: requested %.3g %s, available %.3g %s", kind, requested, unit, available, unit),
	}
}

func (s *Server) quota(w http.ResponseWriter, r *http.Request) {
	if len(r.URL.Query()) != 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "quota does not accept query parameters")
		return
	}
	if s.Quota == nil {
		writeJSON(w, http.StatusOK, QuotaResponse{
			Namespace: identityFromContext(r.Context()).Namespace,
			Items:     []QuotaItem{},
			Degraded:  true,
			Message:   "quota provider is not configured",
		})
		return
	}
	response, err := s.Quota.Quota(r.Context(), identityFromContext(r.Context()).Namespace)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *KubernetesStore) Quota(ctx context.Context, namespace string) (QuotaResponse, error) {
	var quota struct {
		Status struct {
			Hard map[string]string `json:"hard"`
			Used map[string]string `json:"used"`
		} `json:"status"`
	}
	path := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/resourcequotas/" + url.PathEscape("quota-"+namespace)
	if err := s.requestJSON(ctx, http.MethodGet, path, nil, &quota); err != nil {
		if errors.Is(err, ErrNotFound) {
			return QuotaResponse{Namespace: namespace, Items: []QuotaItem{}, Degraded: true, Message: "workspace quota is not configured"}, nil
		}
		return QuotaResponse{}, err
	}
	items := []QuotaItem{
		quotaQuantityItem("cpu", "cores", quota.Status.Hard["limits.cpu"], quota.Status.Used["limits.cpu"], cpuQuantityToCores),
		quotaQuantityItem("memory", "Gi", quota.Status.Hard["limits.memory"], quota.Status.Used["limits.memory"], memoryQuantityToGi),
		quotaQuantityItem("storage", "Gi", quota.Status.Hard["requests.storage"], quota.Status.Used["requests.storage"], memoryQuantityToGi),
	}
	return QuotaResponse{Namespace: namespace, Items: items}, nil
}

func quotaQuantityItem(kind, unit, hard, used string, parser func(string) (float64, error)) QuotaItem {
	limit, _ := parser(hard)
	usedValue, _ := parser(used)
	available := limit - usedValue
	if available < 0 {
		available = 0
	}
	return QuotaItem{
		Type:      kind,
		Used:      roundQuantity(usedValue, 3),
		Limit:     roundQuantity(limit, 3),
		Available: roundQuantity(available, 3),
		Unit:      unit,
	}
}

func cpuQuantityToCores(value string) (float64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	milli, err := cpuMilli(value)
	if err != nil {
		return 0, err
	}
	return milli / 1000, nil
}

func memoryQuantityToGi(value string) (float64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	bytes, err := quantityBytes(value)
	if err != nil {
		return 0, err
	}
	return float64(bytes) / float64(gib), nil
}

func roundQuantity(value float64, precision int) float64 {
	if precision <= 0 {
		return value
	}
	scale := 1.0
	for range precision {
		scale *= 10
	}
	return float64(int64(value*scale+0.5)) / scale
}
