package ci_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestWorkflowActionPins(t *testing.T) {
	var files []string
	for _, pattern := range []string{"*.yml", "*.yaml"} {
		matches, err := filepath.Glob(filepath.Join(repositoryRoot(t), ".github", "workflows", pattern))
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, matches...)
	}
	if len(files) == 0 {
		t.Fatal("no workflows found")
	}
	pin := regexp.MustCompile(`^[^@\s]+@[a-f0-9]{40}$`)
	version := regexp.MustCompile(`^# v[0-9]+\.[0-9]+\.[0-9]+$`)
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var document yaml.Node
		if err := yaml.Unmarshal(data, &document); err != nil {
			t.Fatal(err)
		}
		var walk func(*yaml.Node)
		walk = func(node *yaml.Node) {
			if node.Kind == yaml.MappingNode {
				for i := 0; i < len(node.Content); i += 2 {
					key, value := node.Content[i], node.Content[i+1]
					if key.Value == "uses" && !strings.HasPrefix(value.Value, "./") {
						if !pin.MatchString(value.Value) || !version.MatchString(value.LineComment) {
							t.Errorf("%s:%d action must have an immutable SHA and exact version comment: %s %s", filepath.Base(path), value.Line, value.Value, value.LineComment)
						}
					}
				}
			}
			for _, child := range node.Content {
				walk(child)
			}
		}
		walk(&document)
	}
}

func TestImageTagScripts(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("image publishing runs with Bash")
	}
	workflow := loadWorkflow(t, "build-and-push-image.yml")
	choose := namedStep(t, workflow.Jobs["image"], "Get Image Tag Name")
	prepare := namedStep(t, workflow.Jobs["image"], "Prepare Image Tags")
	for _, tc := range []struct{ name, input, ref, want string }{
		{"release", "", "refs/tags/v1.0.5", "v1.0.5"},
		{"manual", "test-build", "refs/heads/layerv/main", "test-build"},
		{"space", "not a tag", "refs/tags/v1", ""},
		{"newline", "test\nINJECTED=value", "refs/tags/v1", ""},
		{"shell syntax", "$(exit 73)", "refs/tags/v1", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "env")
			if err := os.WriteFile(output, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("bash", "-eu", "-c", choose.Run)
			cmd.Env = append(os.Environ(), "FRP_IMAGE_TAG_INPUT="+tc.input, "GITHUB_REF="+tc.ref, "GITHUB_ENV="+output)
			stdout, err := cmd.CombinedOutput()
			if tc.want == "" {
				if err == nil {
					t.Fatal("invalid image tag was accepted")
				}
				if data, readErr := os.ReadFile(output); readErr != nil || len(data) != 0 {
					t.Fatalf("invalid tag wrote environment data: %q, %v", data, readErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("choose image tag: %v: %s", err, stdout)
			}
			cmd = exec.Command("bash", "-eu", "-c", prepare.Run)
			cmd.Env = append(os.Environ(), "TAG_NAME="+tc.want, "GITHUB_ENV="+output)
			if stdout, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("prepare tags: %v: %s", err, stdout)
			}
			want := map[string]string{
				"TAG_NAME":             tc.want,
				"DOCKERFILE_FRPC_PATH": "dockerfiles/Dockerfile-for-frpc",
				"DOCKERFILE_FRPS_PATH": "dockerfiles/Dockerfile-for-frps",
				"TAG_FRPC":             "fatedier/frpc:" + tc.want,
				"TAG_FRPS":             "fatedier/frps:" + tc.want,
				"TAG_FRPC_GPR":         "ghcr.io/fatedier/frpc:" + tc.want,
				"TAG_FRPS_GPR":         "ghcr.io/fatedier/frps:" + tc.want,
			}
			if got := readOutputs(t, output); !reflect.DeepEqual(got, want) {
				t.Errorf("image metadata = %v, want %v", got, want)
			}
		})
	}
}
