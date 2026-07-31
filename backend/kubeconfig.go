package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const maxKubeconfigHeaderBytes = 512 << 10

type kubernetesAccessContextKey struct{}

type KubernetesAccess struct {
	BaseURL string
	Token   string
	Client  *http.Client
}

func WithKubernetesAccess(ctx context.Context, access KubernetesAccess) context.Context {
	return context.WithValue(ctx, kubernetesAccessContextKey{}, access)
}

func kubernetesAccessFromContext(ctx context.Context) (KubernetesAccess, bool) {
	access, ok := ctx.Value(kubernetesAccessContextKey{}).(KubernetesAccess)
	return access, ok
}

type KubeconfigIdentityProvider struct {
	Fallback IdentityProvider
}

func (p KubeconfigIdentityProvider) Identity(ctx context.Context, r *http.Request) (Identity, error) {
	if rawAuthorization(r) == "" {
		if p.Fallback == nil {
			return Identity{}, ErrUnauthenticated
		}
		return p.Fallback.Identity(ctx, r)
	}
	_, identity, err := accessFromAuthorization(r.Header.Get("Authorization"))
	return identity, err
}

func (p KubeconfigIdentityProvider) KubernetesContext(ctx context.Context, r *http.Request) (context.Context, error) {
	if rawAuthorization(r) == "" {
		return ctx, nil
	}
	access, _, err := accessFromAuthorization(r.Header.Get("Authorization"))
	if err != nil {
		return ctx, err
	}
	return WithKubernetesAccess(ctx, access), nil
}

func rawAuthorization(r *http.Request) string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.Header.Get("Authorization"))
}

func accessFromAuthorization(value string) (KubernetesAccess, Identity, error) {
	raw := strings.TrimSpace(value)
	if raw == "" || strings.HasPrefix(strings.ToLower(raw), "bearer ") || len(raw) > maxKubeconfigHeaderBytes {
		return KubernetesAccess{}, Identity{}, ErrUnauthenticated
	}
	kubeconfig, err := url.PathUnescape(raw)
	if err != nil || strings.TrimSpace(kubeconfig) == "" {
		return KubernetesAccess{}, Identity{}, ErrUnauthenticated
	}
	return parseKubeconfig(kubeconfig)
}

type kubeconfigFile struct {
	CurrentContext string `yaml:"current-context"`
	Clusters       []struct {
		Name    string `yaml:"name"`
		Cluster struct {
			Server                   string `yaml:"server"`
			CertificateAuthorityData string `yaml:"certificate-authority-data"`
			InsecureSkipTLSVerify    bool   `yaml:"insecure-skip-tls-verify"`
		} `yaml:"cluster"`
	} `yaml:"clusters"`
	Contexts []struct {
		Name    string `yaml:"name"`
		Context struct {
			Cluster   string `yaml:"cluster"`
			User      string `yaml:"user"`
			Namespace string `yaml:"namespace"`
		} `yaml:"context"`
	} `yaml:"contexts"`
	Users []struct {
		Name string `yaml:"name"`
		User struct {
			Token                 string `yaml:"token"`
			ClientCertificateData string `yaml:"client-certificate-data"`
			ClientKeyData         string `yaml:"client-key-data"`
		} `yaml:"user"`
	} `yaml:"users"`
}

func parseKubeconfig(data string) (KubernetesAccess, Identity, error) {
	var cfg kubeconfigFile
	if err := yaml.Unmarshal([]byte(data), &cfg); err != nil {
		return KubernetesAccess{}, Identity{}, ErrUnauthenticated
	}
	contextEntry, ok := selectedContext(cfg)
	if !ok {
		return KubernetesAccess{}, Identity{}, ErrUnauthenticated
	}
	clusterEntry, ok := selectedCluster(cfg, contextEntry.Context.Cluster)
	if !ok {
		return KubernetesAccess{}, Identity{}, ErrUnauthenticated
	}
	userEntry, ok := selectedUser(cfg, contextEntry.Context.User)
	if !ok {
		return KubernetesAccess{}, Identity{}, ErrUnauthenticated
	}
	namespace := strings.TrimSpace(contextEntry.Context.Namespace)
	if namespace == "" {
		namespace = "ns-" + strings.TrimSpace(userEntry.Name)
	}
	identity, err := validateIdentity(Identity{UserID: userEntry.Name, Namespace: namespace})
	if err != nil {
		return KubernetesAccess{}, Identity{}, ErrUnauthenticated
	}
	access, err := kubernetesAccessFromParts(clusterEntry.Cluster.Server, clusterEntry.Cluster.CertificateAuthorityData, clusterEntry.Cluster.InsecureSkipTLSVerify, userEntry.User.Token, userEntry.User.ClientCertificateData, userEntry.User.ClientKeyData)
	if err != nil {
		return KubernetesAccess{}, Identity{}, ErrUnauthenticated
	}
	return access, identity, nil
}

func selectedContext(cfg kubeconfigFile) (struct {
	Name    string `yaml:"name"`
	Context struct {
		Cluster   string `yaml:"cluster"`
		User      string `yaml:"user"`
		Namespace string `yaml:"namespace"`
	} `yaml:"context"`
}, bool) {
	if strings.TrimSpace(cfg.CurrentContext) != "" {
		for _, entry := range cfg.Contexts {
			if entry.Name == cfg.CurrentContext {
				return entry, true
			}
		}
	}
	if len(cfg.Contexts) == 0 {
		return struct {
			Name    string `yaml:"name"`
			Context struct {
				Cluster   string `yaml:"cluster"`
				User      string `yaml:"user"`
				Namespace string `yaml:"namespace"`
			} `yaml:"context"`
		}{}, false
	}
	return cfg.Contexts[0], true
}

func selectedCluster(cfg kubeconfigFile, name string) (struct {
	Name    string `yaml:"name"`
	Cluster struct {
		Server                   string `yaml:"server"`
		CertificateAuthorityData string `yaml:"certificate-authority-data"`
		InsecureSkipTLSVerify    bool   `yaml:"insecure-skip-tls-verify"`
	} `yaml:"cluster"`
}, bool) {
	name = strings.TrimSpace(name)
	for _, entry := range cfg.Clusters {
		if name == "" || entry.Name == name {
			return entry, true
		}
	}
	return struct {
		Name    string `yaml:"name"`
		Cluster struct {
			Server                   string `yaml:"server"`
			CertificateAuthorityData string `yaml:"certificate-authority-data"`
			InsecureSkipTLSVerify    bool   `yaml:"insecure-skip-tls-verify"`
		} `yaml:"cluster"`
	}{}, false
}

func selectedUser(cfg kubeconfigFile, name string) (struct {
	Name string `yaml:"name"`
	User struct {
		Token                 string `yaml:"token"`
		ClientCertificateData string `yaml:"client-certificate-data"`
		ClientKeyData         string `yaml:"client-key-data"`
	} `yaml:"user"`
}, bool) {
	name = strings.TrimSpace(name)
	for _, entry := range cfg.Users {
		if name == "" || entry.Name == name {
			return entry, true
		}
	}
	return struct {
		Name string `yaml:"name"`
		User struct {
			Token                 string `yaml:"token"`
			ClientCertificateData string `yaml:"client-certificate-data"`
			ClientKeyData         string `yaml:"client-key-data"`
		} `yaml:"user"`
	}{}, false
}

func kubernetesAccessFromParts(server, caData string, insecureSkipTLSVerify bool, token, clientCertData, clientKeyData string) (KubernetesAccess, error) {
	server = strings.TrimSpace(server)
	parsed, err := url.Parse(server)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return KubernetesAccess{}, errors.New("invalid Kubernetes server")
	}
	token = strings.TrimSpace(token)
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if insecureSkipTLSVerify {
		tlsConfig.InsecureSkipVerify = true
	} else if strings.TrimSpace(caData) != "" {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(caData))
		if err != nil {
			return KubernetesAccess{}, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(decoded) {
			return KubernetesAccess{}, errors.New("invalid Kubernetes CA")
		}
		tlsConfig.RootCAs = pool
	}
	if clientCertData != "" || clientKeyData != "" {
		certPEM, err := base64.StdEncoding.DecodeString(strings.TrimSpace(clientCertData))
		if err != nil {
			return KubernetesAccess{}, err
		}
		keyPEM, err := base64.StdEncoding.DecodeString(strings.TrimSpace(clientKeyData))
		if err != nil {
			return KubernetesAccess{}, err
		}
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return KubernetesAccess{}, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	if token == "" && len(tlsConfig.Certificates) == 0 {
		return KubernetesAccess{}, errors.New("missing Kubernetes user credentials")
	}
	return KubernetesAccess{
		BaseURL: strings.TrimRight(server, "/"),
		Token:   token,
		Client: &http.Client{
			Transport: &http.Transport{TLSClientConfig: tlsConfig},
			Timeout:   30 * time.Second,
		},
	}, nil
}
