package confsource

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// meta 查询失败的归类：压缩、超时，其余归 other
func TestMetaLookupReason(t *testing.T) {
	require.Equal(t, "compacted", metaLookupReason(rpctypes.ErrCompacted))
	require.Equal(t, "compacted", metaLookupReason(rpctypes.ErrGRPCCompacted))
	require.Equal(t, "timeout", metaLookupReason(context.DeadlineExceeded))
	require.Equal(t, "other", metaLookupReason(errors.New("etcdserver: no leader")))
}
