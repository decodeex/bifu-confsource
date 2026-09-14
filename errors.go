package confsource

import "errors"

var (
	// ErrNoConfig etcd 与本地快照都拿不到配置，服务不得以空配置启动
	ErrNoConfig = errors.New("confsource: no config available from etcd or snapshot")

	// ErrSnapshotMissing 本地没有可用快照
	ErrSnapshotMissing = errors.New("confsource: snapshot missing")

	// ErrWatchStarted 一个 Source 只允许启动一个 Watch 循环
	ErrWatchStarted = errors.New("confsource: watch already started")
)
