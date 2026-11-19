package confsource

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 心跳默认开启且参数固定，WithKeepAlive 能改、传 0 能关
func TestOptions_KeepAliveDefaults(t *testing.T) {
	o := defaultOptions()
	require.Equal(t, 30*time.Second, o.keepAliveTime)
	require.Equal(t, 10*time.Second, o.keepAliveTimeout)

	WithKeepAlive(12*time.Second, 3*time.Second)(&o)
	require.Equal(t, 12*time.Second, o.keepAliveTime)
	require.Equal(t, 3*time.Second, o.keepAliveTimeout)

	WithKeepAlive(0, 0)(&o)
	require.Zero(t, o.keepAliveTime)
}
