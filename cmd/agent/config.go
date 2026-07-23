package main

import (
	"os"
	"strconv"
)

// agentConfig is populated entirely from environment variables injected by
// the operator's StatefulSet pod template (see
// internal/controller/statefulset.go:agentEnv). Keeping the two in sync is
// the single most important cross-file contract in this repo.
type agentConfig struct {
	KividbAddr   string // KIVIDB_ADDR, e.g. "127.0.0.1:6380"
	AuthUsername string // fixed to "default"; kividb's default ACL user
	AuthPassword string // KIVIDB_AUTH_PASSWORD
	AgentPort    int    // AGENT_PORT
	DataDir      string // DATA_DIR
	ClusterName  string // CLUSTER_NAME
	PodName      string // POD_NAME
	PodIP        string // POD_IP

	S3Endpoint       string
	S3Bucket         string
	S3Region         string
	S3PathPrefix     string
	S3ForcePathStyle bool
	S3InsecureTLS    bool
	S3AccessKeyID    string
	S3SecretKey      string
	S3Retention      int
}

func loadConfig() agentConfig {
	return agentConfig{
		KividbAddr:   getEnv("KIVIDB_ADDR", "127.0.0.1:6380"),
		AuthUsername: "default",
		AuthPassword: os.Getenv("KIVIDB_AUTH_PASSWORD"),
		AgentPort:    getEnvInt("AGENT_PORT", 8081),
		DataDir:      getEnv("DATA_DIR", "/data"),
		ClusterName:  os.Getenv("CLUSTER_NAME"),
		PodName:      os.Getenv("POD_NAME"),
		PodIP:        os.Getenv("POD_IP"),

		S3Endpoint:       os.Getenv("S3_ENDPOINT"),
		S3Bucket:         os.Getenv("S3_BUCKET"),
		S3Region:         os.Getenv("S3_REGION"),
		S3PathPrefix:     os.Getenv("S3_PATH_PREFIX"),
		S3ForcePathStyle: getEnvBool("S3_FORCE_PATH_STYLE", false),
		S3InsecureTLS:    getEnvBool("S3_INSECURE_SKIP_TLS_VERIFY", false),
		S3AccessKeyID:    os.Getenv("S3_ACCESS_KEY_ID"),
		S3SecretKey:      os.Getenv("S3_SECRET_ACCESS_KEY"),
		S3Retention:      getEnvInt("S3_RETENTION", 7),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getEnvBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}
