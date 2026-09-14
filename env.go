package confsource

import "fmt"

// PlatformEnv 平台环境名，与 bifufx-configmaps 的目录名一致
type PlatformEnv string

const (
	// EnvDevelop 开发环境，所有分支 namespace 共享同一份配置
	EnvDevelop PlatformEnv = "develop"

	// EnvStaging 预发环境
	EnvStaging PlatformEnv = "staging"

	// EnvMain 生产环境
	EnvMain PlatformEnv = "main"
)

// Pod 环境变量 Env 的取值到平台环境的映射，平台环境名本身也接受
var podEnvToPlatform = map[string]PlatformEnv{
	"dev":     EnvDevelop,
	"release": EnvDevelop,
	"develop": EnvDevelop,
	"staging": EnvStaging,
	"prod":    EnvMain,
	"main":    EnvMain,
}

// ParsePlatformEnv 把 Pod 的 Env 取值映射为平台环境
func ParsePlatformEnv(podEnv string) (PlatformEnv, error) {
	env, ok := podEnvToPlatform[podEnv]
	if !ok {
		return "", fmt.Errorf("confsource: unknown env %q", podEnv)
	}
	return env, nil
}
