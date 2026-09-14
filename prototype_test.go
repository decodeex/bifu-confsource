package confsource_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testConf struct {
	Server struct {
		Addr    string `json:"addr"`
		Timeout int    `json:"timeout"`
	} `json:"server"`
	Data struct {
		Password string `json:"password"`
		Topic    string `json:"topic"`
	} `json:"data"`
}

// confYAML 带一行仓库里真实存在的那种注释：注释里的 $ 不能被当成变量引用
func confYAML(timeout int, password string) string {
	return fmt.Sprintf(`# 端口必须用 $SVC_*，勿写死；旧写法 ${MT5DB_USERNAME} 已废弃
server:
  addr: 0.0.0.0:8000
  timeout: %d
data:
  password: %s
  topic: user_${RELEASE_BRANCH}
`, timeout, password)
}

func setTestEnvVars(t *testing.T) {
	t.Setenv("TEST_DB_PASSWORD", "s3cret")
	t.Setenv("RELEASE_BRANCH", "feat-x")
}

// 场景一：N 加载成功后发布缺变量的 M，内存、快照、gauge 都停在 N；之后合法的 P 照常生效
func TestPrototype_RejectedRevisionKeepsPreviousEverywhere(t *testing.T) {
	setTestEnvVars(t)
	env := newTestEnv(t)

	yamlN := confYAML(3, "${TEST_DB_PASSWORD}")
	revN := env.publish(yamlN, "pub-n")

	l, err := newLoader[testConf](env.options()...)
	require.NoError(t, err)
	defer l.Close()

	require.Equal(t, 3, l.GetConfig().Server.Timeout)
	require.Equal(t, "s3cret", l.GetConfig().Data.Password)
	require.Equal(t, "user_feat-x", l.GetConfig().Data.Topic)
	env.assertCommitted(l.source, revN, yamlN)

	revM := env.publish(confYAML(5, "${MISSING_SECRET}"), "pub-m")
	require.Eventually(t, func() bool {
		return strings.Contains(env.logs.String(), fmt.Sprintf("revision %d rejected", revM))
	}, 5*time.Second, 20*time.Millisecond, "M should be rejected by the validator")

	require.Equal(t, 3, l.GetConfig().Server.Timeout, "GetConfig() must stay on N")
	require.Equal(t, "s3cret", l.GetConfig().Data.Password)
	env.assertCommitted(l.source, revN, yamlN)

	yamlP := confYAML(7, "${TEST_DB_PASSWORD}")
	revP := env.publish(yamlP, "pub-p")
	require.Eventually(t, func() bool { return l.GetConfig().Server.Timeout == 7 }, 5*time.Second, 20*time.Millisecond)
	env.assertCommitted(l.source, revP, yamlP)

	for {
		select {
		case cfg := <-l.applied:
			require.NotEqual(t, 5, cfg.Server.Timeout, "M must never reach GetConfig()")
			continue
		default:
		}
		break
	}
}

// 场景二：连续两次合法更新按序生效，最终三者都是 M
func TestPrototype_ConsecutiveUpdatesApplyInOrder(t *testing.T) {
	setTestEnvVars(t)
	env := newTestEnv(t)

	env.publish(confYAML(3, "${TEST_DB_PASSWORD}"), "pub-n")
	l, err := newLoader[testConf](env.options()...)
	require.NoError(t, err)
	defer l.Close()
	require.Equal(t, 3, l.GetConfig().Server.Timeout)

	env.publish(confYAML(4, "${TEST_DB_PASSWORD}"), "pub-m1")
	yamlM2 := confYAML(5, "${TEST_DB_PASSWORD}")
	revM2 := env.publish(yamlM2, "pub-m2")

	require.Eventually(t, func() bool { return l.GetConfig().Server.Timeout == 5 }, 5*time.Second, 20*time.Millisecond)
	env.assertCommitted(l.source, revM2, yamlM2)

	var seen []int
	for len(l.applied) > 0 {
		cfg := <-l.applied
		if len(seen) == 0 || seen[len(seen)-1] != cfg.Server.Timeout {
			seen = append(seen, cfg.Server.Timeout)
		}
	}
	require.Equal(t, []int{4, 5}, seen, "M1 must be applied before M2, neither skipped")
}

// 整份替换合并：yaml 里删掉的字段在运行期要变回零值，而不是残留旧值
func TestPrototype_RemovedKeyClearsAtRuntime(t *testing.T) {
	setTestEnvVars(t)
	env := newTestEnv(t)

	env.publish(confYAML(3, "${TEST_DB_PASSWORD}"), "pub-n")
	l, err := newLoader[testConf](env.options()...)
	require.NoError(t, err)
	defer l.Close()
	require.Equal(t, "user_feat-x", l.GetConfig().Data.Topic)

	withoutTopic := "server:\n  addr: 0.0.0.0:8000\n  timeout: 3\ndata:\n  password: ${TEST_DB_PASSWORD}\n"
	env.publish(withoutTopic, "pub-m")
	require.Eventually(t, func() bool { return l.GetConfig().Data.Topic == "" }, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, "s3cret", l.GetConfig().Data.Password)
}

// 启动时 ${VAR} 缺失必须直接失败，不能带着空字符串起来
func TestPrototype_MissingVariableFailsStartup(t *testing.T) {
	t.Setenv("RELEASE_BRANCH", "feat-x")
	env := newTestEnv(t)
	env.publish(confYAML(3, "${TEST_DB_PASSWORD}"), "pub-n")

	_, err := newLoader[testConf](env.options()...)
	require.ErrorContains(t, err, "missing variables: TEST_DB_PASSWORD")
	require.Empty(t, env.snapshot(), "rejected config must not become a snapshot")
}
