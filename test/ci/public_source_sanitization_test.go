// Copyright 2026 The frp Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ci_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// This repository is PUBLIC. The layerv fork delta is deliberately narrow and
// mechanical, but its docs describe how the fork is consumed, and that is
// exactly where a private repository name, an internal hostname, or a webhook
// slips in without anyone noticing -- FORK.md named a private consumer
// repository from the day it was written until this guard was added.
//
// Ported from the equivalent guard in layervai/qurl-connector, narrowed to
// LayerV-specific material: the generic scans there (bare 12-digit
// identifiers, sandbox/prod hostnames) fire constantly on upstream frp's own
// source and docs, and a guard that must be muted is not a guard.
//
// Forbidden literals are split so this file does not itself contain the terms
// it bans.
var (
	appIDPattern    = regexp.MustCompile(`(?i)app[_ -]?(?:id|client[_ -]?id)[[:space:]]*[:=][[:space:]]*["']?([0-9]+)`)
	layerVRepoRef   = regexp.MustCompile(`(?i)\blayervai/([a-z0-9][a-z0-9-]*)`)
	layerVHostRef   = regexp.MustCompile(`(?i)\b(?:[a-z0-9-]+\.)*layerv\.ai\b`)
	privateRepoName = []string{
		"qurl-" + "service",
		"qurl-" + "reverse-tunnel-server",
		"traefik-" + "plugins",
		"qurl-" + "integrations-infra",
		"layervai/" + "nhp",
	}
	secretEndpoint = []string{
		"https://hooks" + ".slack.com/",
		"discord.com/api/" + "webhooks/",
		"execute-api" + ".amazonaws.com",
	}
)

// publicLayerVRepositories are the LayerV repositories this fork may name.
// Adding an entry is a deliberate disclosure decision: confirm the repository
// is actually public first, because naming a private one here silently
// disarms the check rather than failing it.
var publicLayerVRepositories = map[string]bool{
	"frp":              true,
	"qurl-conformance": true,
	"qurl-connector":   true,
	"qurl-go":          true,
}

// publicLayerVHosts are hostnames already documented as public.
var publicLayerVHosts = map[string]bool{
	"layerv.ai":              true,
	"api." + "layerv.ai":     true,
	"hub.nhp." + "layerv.ai": true,
}

func TestPublicSourceNamesNoPrivateLayerVMaterial(t *testing.T) {
	t.Parallel()

	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	self := filepath.Join("test", "ci", "public_source_sanitization_test.go")

	err := filepath.WalkDir(repoRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch filepath.ToSlash(rel) {
			case ".git", "bin", "node_modules", "dist":
				return filepath.SkipDir
			}
			return nil
		}
		// This file necessarily contains the split literals it bans.
		if rel == self {
			return nil
		}
		// package-lock.json and vendored web assets are large and generated;
		// they carry no LayerV prose.
		if strings.HasSuffix(rel, "package-lock.json") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			// Unreadable entries (sockets, broken symlinks) are not source.
			return nil //nolint:nilerr // skipping non-source entries is intended
		}
		text := string(body)
		lower := strings.ToLower(text)

		for _, name := range privateRepoName {
			if strings.Contains(lower, name) {
				t.Errorf("%s names private LayerV repository %q; describe it by role instead (for example \"the frps-side consumer\")", rel, name)
			}
		}
		for _, match := range layerVRepoRef.FindAllStringSubmatch(text, -1) {
			if !publicLayerVRepositories[strings.ToLower(match[1])] {
				t.Errorf("%s refers to LayerV repository %q, which is not on the reviewed-public list in this test", rel, match[0])
			}
		}
		for _, host := range layerVHostRef.FindAllString(lower, -1) {
			if !publicLayerVHosts[host] {
				t.Errorf("%s contains undocumented LayerV hostname %q", rel, host)
			}
		}
		for _, endpoint := range secretEndpoint {
			if strings.Contains(lower, endpoint) {
				t.Errorf("%s contains a private webhook or cloud endpoint %q", rel, endpoint)
			}
		}
		if match := appIDPattern.FindStringSubmatch(text); match != nil {
			t.Errorf("%s contains a literal GitHub App identifier %s", rel, match[1])
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
