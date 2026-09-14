package confsource

import (
	"crypto/tls"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/prometheus/client_golang/prometheus"
)

// Validator 提交前校验钩子。对某个 revision 的配置原文做严格展开与试解析，
// 返回 nil 才允许这一版写入快照并交付给 Kratos；失败时内存、快照、revision 都停在上一版。
type Validator func(rev int64, raw []byte) error

// Option 配置源构造选项
type Option func(*options)

type options struct {
	endpoints   []string
	podEnv      string
	service     string
	snapshotDir string
	timeout     time.Duration
	validator   Validator
	tls         *tls.Config
	username    string
	password    string
	logger      log.Logger
	registerer  prometheus.Registerer
	backoffMin  time.Duration
	backoffMax  time.Duration
}

const (
	defaultSnapshotDir = "/var/lib/confsource"
	defaultTimeout     = 3 * time.Second
	defaultBackoffMin  = time.Second
	defaultBackoffMax  = 30 * time.Second
)

func defaultOptions() options {
	return options{
		snapshotDir: defaultSnapshotDir,
		timeout:     defaultTimeout,
		logger:      log.GetLogger(),
		registerer:  prometheus.DefaultRegisterer,
		backoffMin:  defaultBackoffMin,
		backoffMax:  defaultBackoffMax,
	}
}

// WithEndpoints etcd 地址，多个用逗号分隔
func WithEndpoints(csv string) Option {
	return func(o *options) {
		o.endpoints = o.endpoints[:0]
		for _, ep := range strings.Split(csv, ",") {
			if ep = strings.TrimSpace(ep); ep != "" {
				o.endpoints = append(o.endpoints, ep)
			}
		}
	}
}

// WithEnv Pod 的 Env 取值（dev / release / staging / prod），内部映射到平台环境
func WithEnv(podEnv string) Option {
	return func(o *options) { o.podEnv = podEnv }
}

// WithService 服务名，与 bifufx-configmaps 中的文件名一致
func WithService(name string) Option {
	return func(o *options) { o.service = name }
}

// WithSnapshotDir 本地快照目录，默认 /var/lib/confsource
func WithSnapshotDir(dir string) Option {
	return func(o *options) { o.snapshotDir = dir }
}

// WithTimeout 单次 etcd 请求超时，默认 3 秒
func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}

// WithValidator 注入提交前校验钩子，由共享加载器提供
func WithValidator(v Validator) Option {
	return func(o *options) { o.validator = v }
}

// WithTLS etcd 连接的 TLS 配置
func WithTLS(cfg *tls.Config) Option {
	return func(o *options) { o.tls = cfg }
}

// WithAuth etcd 用户名密码
func WithAuth(username, password string) Option {
	return func(o *options) {
		o.username = username
		o.password = password
	}
}

// WithLogger 日志，默认 Kratos 全局 logger
func WithLogger(l log.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithRegisterer 指标注册器，默认 prometheus.DefaultRegisterer
func WithRegisterer(r prometheus.Registerer) Option {
	return func(o *options) { o.registerer = r }
}

// WithBackoff Watch 断连与降级重连的退避区间，默认 1 秒起、30 秒上限
func WithBackoff(minWait, maxWait time.Duration) Option {
	return func(o *options) {
		o.backoffMin = minWait
		o.backoffMax = maxWait
	}
}
