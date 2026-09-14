package envexpand

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func mapLookup(vars map[string]string) Lookup {
	return func(name string) (string, bool) {
		v, ok := vars[name]
		return v, ok
	}
}

func TestExpandBothForms(t *testing.T) {
	out, err := Expand([]byte("x=${A} y=$B"), mapLookup(map[string]string{"A": "1", "B": "2"}))
	require.NoError(t, err)
	require.Equal(t, "x=1 y=2", string(out))
}

func TestExpandReportsAllMissingSorted(t *testing.T) {
	_, err := Expand([]byte("${ZED} ${ALPHA} ${ZED}"), mapLookup(nil))

	var missing *MissingError
	require.True(t, errors.As(err, &missing))
	require.Equal(t, []string{"ALPHA", "ZED"}, missing.Names)
	require.EqualError(t, err, "envexpand: missing variables: ALPHA, ZED")
}

func TestUnmarshalYAMLIgnoresCommentsAndKeys(t *testing.T) {
	raw := []byte(`# 端口必须用 $SVC_*，勿写死
server:
  # source: ${MT5DB_USERNAME}:${MT5DB_PASSWORD}@tcp
  addr: 0.0.0.0:${PORT}
  ${NOT_A_KEY}: literal
`)
	var out map[string]any
	require.NoError(t, UnmarshalYAML(raw, mapLookup(map[string]string{"PORT": "8000"}), &out))

	server := out["server"].(map[string]any)
	require.Equal(t, "0.0.0.0:8000", server["addr"])
	require.Equal(t, "literal", server["${NOT_A_KEY}"])
}

func TestUnmarshalYAMLReinfersPlainScalarType(t *testing.T) {
	raw := []byte("timeout: ${TIMEOUT}\nquoted: \"${TIMEOUT}\"\nenabled: ${FLAG}\nlist:\n  - ${TIMEOUT}\n")
	var out struct {
		Timeout int    `yaml:"timeout"`
		Quoted  string `yaml:"quoted"`
		Enabled bool   `yaml:"enabled"`
		List    []int  `yaml:"list"`
	}
	require.NoError(t, UnmarshalYAML(raw, mapLookup(map[string]string{"TIMEOUT": "30", "FLAG": "true"}), &out))
	require.Equal(t, 30, out.Timeout)
	require.Equal(t, "30", out.Quoted)
	require.True(t, out.Enabled)
	require.Equal(t, []int{30}, out.List)
}

func TestUnmarshalYAMLReportsMissingInValuesOnly(t *testing.T) {
	raw := []byte("# ${IN_COMMENT}\npassword: ${DB_PASSWORD}\ntopic: user_${BRANCH}\n")
	var out map[string]any
	err := UnmarshalYAML(raw, mapLookup(nil), &out)

	var missing *MissingError
	require.True(t, errors.As(err, &missing))
	require.Equal(t, []string{"BRANCH", "DB_PASSWORD"}, missing.Names)
}

func TestUnmarshalYAMLExpandsAnchoredValueOnce(t *testing.T) {
	raw := []byte("base: &b ${HOST}\ncopy: *b\n")
	var out map[string]any
	require.NoError(t, UnmarshalYAML(raw, mapLookup(map[string]string{"HOST": "h"}), &out))
	require.Equal(t, "h", out["base"])
	require.Equal(t, "h", out["copy"])
}

func TestUnmarshalYAMLEmptyDocument(t *testing.T) {
	var out map[string]any
	require.NoError(t, UnmarshalYAML([]byte("# only a comment\n"), mapLookup(nil), &out))
	require.Nil(t, out)
}

func TestExpandEnvUsesProcessEnvironment(t *testing.T) {
	t.Setenv("CONFSOURCE_TEST_VAR", "ok")
	out, err := ExpandEnv([]byte("${CONFSOURCE_TEST_VAR}"))
	require.NoError(t, err)
	require.Equal(t, "ok", string(out))

	var m map[string]any
	require.NoError(t, UnmarshalYAMLEnv([]byte("v: ${CONFSOURCE_TEST_VAR}"), &m))
	require.Equal(t, "ok", m["v"])
}
