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

// OnCommit 提交回调。Validator 通过后、写快照之前调用，加载器在这里把该版本 Scan 进业务内存；
// 返回 nil 才写快照、推进 revision 与指标、交付 Kratos。所有可能失败的步骤必须放在 Store 之前，
// Store 是回调的最后一步，因为 confsource 无法撤销已经发生的 Store。
type OnCommit func(rev int64, raw []byte) error

// Option 配置源构造选项
type Option func(*options)

type options struct {
	endpoints   []string
	service     string
	snapshotDir string
	timeout     time.Duration
	validator   Validator
	onCommit    OnCommit
	tls         *tls.Config
	username    string
	password    string
	logger      log.Logger
	registerer  prometheus.Registerer
	backoffMin  time.Duration
	backoffMax  time.Duration
	// keepAliveTime 连接空闲多久后发 gRPC 心跳 ping，0 表示不开心跳；keepAliveTimeout 等 ping 回应的时限
	keepAliveTime    time.Duration
	keepAliveTimeout time.Duration
}

const (
	defaultSnapshotDir = "/var/lib/confsource"
	defaultTimeout     = 3 * time.Second
	defaultBackoffMin  = time.Second
	defaultBackoffMax  = 30 * time.Second
	// 心跳默认值：连接死掉后最晚在 time+timeout 左右被 gRPC 判定，之后由 clientv3 重连
	defaultKeepAliveTime    = 30 * time.Second
	defaultKeepAliveTimeout = 10 * time.Second
)

func defaultOptions() options {
	return options{
		snapshotDir: defaultSnapshotDir,
		timeout:     defaultTimeout,
		logger:      log.GetLogger(),
		registerer:  prometheus.DefaultRegisterer,
		backoffMin:  defaultBackoffMin,
		backoffMax:  defaultBackoffMax,

		keepAliveTime:    defaultKeepAliveTime,
		keepAliveTimeout: defaultKeepAliveTimeout,
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

// WithOnCommit 注入提交回调，由共享加载器提供
func WithOnCommit(fn OnCommit) Option {
	return func(o *options) { o.onCommit = fn }
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

// WithKeepAlive gRPC 连接层心跳：连接空闲 keepAliveTime 后发 ping，keepAliveTimeout 内没有回应即判定连接已死，
// 由 clientv3 内部重连并续订 Watch。默认 30 s / 10 s；keepAliveTime 传 0 关闭心跳。
// gRPC 客户端会把小于 10 s 的 keepAliveTime 抬到 10 s；etcd 服务端默认拒绝比 5 s 更密的 ping
func WithKeepAlive(keepAliveTime, keepAliveTimeout time.Duration) Option {
	return func(o *options) {
		o.keepAliveTime = keepAliveTime
		o.keepAliveTimeout = keepAliveTimeout
	}
}
