package ci_test

import (
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const (
	claudeAction  = "anthropics/claude-code-action@fa7e2f0a29a126f0b81cdcf360561b36e44cf608"
	claudeModel   = "claude-opus-4-8"
	helperPattern = `^credential(\..*)?\.helper$`
)

type trigger struct {
	Types []string `yaml:"types"`
}

type workflow struct {
	On          map[string]trigger `yaml:"on"`
	Permissions map[string]string  `yaml:"permissions"`
	Jobs        map[string]job     `yaml:"jobs"`
}

type job struct {
	If          string            `yaml:"if"`
	Permissions map[string]string `yaml:"permissions"`
	Steps       []step            `yaml:"steps"`
}

type step struct {
	Name string         `yaml:"name"`
	ID   string         `yaml:"id"`
	If   string         `yaml:"if"`
	Uses string         `yaml:"uses"`
	Run  string         `yaml:"run"`
	With map[string]any `yaml:"with"`
	Env  map[string]any `yaml:"env"`
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func loadWorkflow(t *testing.T, name string) workflow {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repositoryRoot(t), ".github", "workflows", name))
	if err != nil {
		t.Fatal(err)
	}
	var got workflow
	if err := yaml.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func namedStep(t *testing.T, got job, name string) step {
	t.Helper()
	for _, candidate := range got.Steps {
		if candidate.Name == name {
			return candidate
		}
	}
	t.Fatalf("missing step %q", name)
	return step{}
}

func stringInput(t *testing.T, got step, name string) string {
	t.Helper()
	value, ok := got.With[name].(string)
	if !ok {
		t.Fatalf("%s.%s is not a string", got.Name, name)
	}
	return value
}

func requireFragments(t *testing.T, text string, fragments ...string) {
	t.Helper()
	for _, fragment := range fragments {
		if !strings.Contains(text, fragment) {
			t.Errorf("missing contract fragment %q", fragment)
		}
	}
}

func modelFromArgs(t *testing.T, args string) string {
	t.Helper()
	matches := regexp.MustCompile(`(?:^|\s)--model\s+([^\s]+)`).FindAllStringSubmatch(args, -1)
	if len(matches) != 1 || len(matches[0]) != 2 {
		t.Fatalf("model argument occurrence count = %d, want 1", len(matches))
	}
	return matches[0][1]
}

func csvFlagFromArgs(t *testing.T, args, flag string) []string {
	t.Helper()
	aliases := []string{regexp.QuoteMeta(flag)}
	switch flag {
	case "--allowed-tools":
		aliases = append(aliases, regexp.QuoteMeta("--allowedTools"))
	case "--disallowed-tools":
		aliases = append(aliases, regexp.QuoteMeta("--disallowedTools"))
	}
	matches := regexp.MustCompile(`(?:^|\s)(?:`+strings.Join(aliases, "|")+`)\s+"([^"]*)"`).FindAllStringSubmatch(args, -1)
	if len(matches) != 1 {
		t.Fatalf("%s occurrence count = %d, want 1", flag, len(matches))
	}
	return strings.Split(matches[0][1], ",")
}

func sortedInputKeys(got step) []string {
	keys := make([]string, 0, len(got.With))
	for key := range got.With {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func TestClaudeWorkflowContracts(t *testing.T) {
	interactive := loadWorkflow(t, "claude.yml")
	automatic := loadWorkflow(t, "claude-code-review.yml")

	if len(interactive.Permissions) != 0 || len(automatic.Permissions) != 0 {
		t.Fatal("workflow-level token permissions must remain empty")
	}
	if got, want := interactive.On, map[string]trigger{
		"issue_comment": {Types: []string{"created"}},
		"issues":        {Types: []string{"opened"}},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("interactive events = %#v, want %#v", got, want)
	}
	for _, forbidden := range []string{
		"pull_request", "pull_request_target", "pull_request_review", "pull_request_review_comment",
	} {
		if _, ok := interactive.On[forbidden]; ok {
			t.Errorf("secret-bearing interactive workflow must not use PR-ref event %q", forbidden)
		}
	}

	command := interactive.Jobs["claude"]
	if got, want := command.Permissions, map[string]string{
		"contents": "read", "issues": "write", "pull-requests": "write",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("interactive permissions = %#v, want %#v", got, want)
	}
	for _, exact := range []string{
		"github.event.comment.body == '@claude'",
		"startsWith(github.event.comment.body, '@claude ')",
		"github.event.issue.body == '@claude'",
		"startsWith(github.event.issue.body, '@claude ')",
		"github.event.issue.title == '@claude'",
		"startsWith(github.event.issue.title, '@claude ')",
	} {
		requireFragments(t, command.If, exact)
	}
	for _, loose := range []string{
		"startsWith(github.event.comment.body, '@claude')",
		"startsWith(github.event.issue.body, '@claude')",
		"startsWith(github.event.issue.title, '@claude')",
	} {
		if strings.Contains(command.If, loose) {
			t.Errorf("loose trigger grammar remains: %s", loose)
		}
	}
	requireFragments(t, namedStep(t, command, "Validate Claude trigger actor permission").Run,
		`timeout 30s gh api "repos/$GITHUB_REPOSITORY/collaborators/$TRIGGER_ACTOR/permission"`,
		"admin|maintain|write", "authorized=true")

	resolver := namedStep(t, command, "Resolve Claude pull request context").Run
	requireFragments(t, resolver,
		`timeout 30s gh api "repos/$GITHUB_REPOSITORY/pulls/$PR_NUMBER"`,
		`.head.repo.full_name`, `.base.repo.full_name`, `.base.repo.default_branch`, `.commits`, `.state`,
		`"$pr_state" != "open"`, `validate_ref "repository default branch" "$default_branch"`,
		`"$head_ref" == "$default_branch"`, `echo "default_branch=$default_branch"`,
		`head_fetch_depth=$(( commit_count > 20 ? commit_count : 20 ))`,
		`echo "head_fetch_depth=$head_fetch_depth"`,
		`"$base_repo" != "$GITHUB_REPOSITORY"`)
	validators := namedStep(t, command, "Materialize trusted Claude input validators")
	requireFragments(t, validators.Env["VALIDATOR_SOURCE"].(string),
		`^[A-Za-z0-9@_][A-Za-z0-9/_.#+,@-]*$`, `[[ "$ref" == "@" ]]`,
		`validate_sensitive_tree()`, `.claude`, `.mcp.json`, `.claude.json`, `.gitmodules`, `.ripgreprc`,
		`CLAUDE.md`, `CLAUDE.local.md`, `.husky`, `git ls-tree -r -z`, `"$path" == "CLAUDE.md"`,
		`"$agents_mode" == "100644"`, `"$agents_mode" == "100755"`)
	commandCheckout := namedStep(t, command, "Checkout repository")
	if commandCheckout.With["fetch-depth"] != 0 || commandCheckout.With["persist-credentials"] != false {
		t.Fatalf("interactive checkout = %#v", commandCheckout.With)
	}
	const trustedCheckoutRef = "${{ steps.claude_pr.outputs.checkout_allowed == 'true' && " +
		"github.event.repository.default_branch || github.sha }}"
	if got := commandCheckout.With["ref"]; got != trustedCheckoutRef {
		t.Fatalf("interactive checkout ref = %#v", got)
	}

	originStep := namedStep(t, command, "Prepare credential-free Claude origin")
	if got := originStep.Env["PR_DEFAULT_REF"]; got != "${{ steps.claude_pr.outputs.default_branch }}" {
		t.Fatalf("interactive origin PR_DEFAULT_REF = %#v", got)
	}
	origin := originStep.Run
	helperGuard := "git config --local --get-regexp '" + helperPattern + "'"
	extraheaderGuard := "git config --local --get-regexp '^http\\..*\\.extraheader$'"
	if strings.Count(origin, helperGuard) != 2 || strings.Count(origin, extraheaderGuard) != 2 {
		t.Fatal("local origin must reject credential config before and after fetch proofs")
	}
	requireFragments(t, origin,
		`PR_DEFAULT_REF`, `"$ISSUE_REF" != "$PR_DEFAULT_REF"`, `"$PR_HEAD_REF" == "$PR_DEFAULT_REF"`,
		`validate_sensitive_tree "Claude head" "$head_sha"`,
		`git init --bare --quiet --object-format="$object_format" "$local_origin"`,
		`git config --local fetch.recurseSubmodules false`,
		`git checkout --quiet --detach`,
		`trusted_guidance_sha="$base_sha"`,
		`git update-index --skip-worktree AGENTS.md`,
		`echo "path=$local_origin"`,
		`echo "trusted_guidance_oid=$trusted_guidance_oid"`,
		`fetch_depth="$PR_HEAD_FETCH_DEPTH"`,
		`git fetch origin "--depth=$fetch_depth" "$head_ref"`,
		`git fetch origin "$base_ref" --depth=1 --no-recurse-submodules`,
		`git fetch origin "$head_ref" --depth=1`)
	if strings.Index(origin, helperGuard) > strings.Index(origin, `git fetch origin "--depth=$fetch_depth"`) ||
		strings.LastIndex(origin, helperGuard) < strings.Index(origin, `git fetch origin "$base_ref" --depth=1`) {
		t.Fatal("credential helper checks do not bracket the pinned fetch proofs")
	}
	commandTarget := namedStep(t, command, "Verify current Claude target")
	requireFragments(t, commandTarget.Run,
		`if [[ "$PR_MODE" != "true" ]]`, `echo "ready=true"`,
		`repos/$GITHUB_REPOSITORY/pulls/$PR_NUMBER`, `.state`, `.head.ref`, `.base.repo.default_branch`,
		`"$current_state" != "open"`, `"$current_head_ref" == "$current_default_branch"`)

	commandAction := namedStep(t, command, "Run Claude Code")
	if commandAction.ID != "claude" || commandAction.Uses != claudeAction {
		t.Fatalf("interactive action = id %q uses %q", commandAction.ID, commandAction.Uses)
	}
	requireFragments(t, commandAction.If, `steps.current_target.outputs.ready == 'true'`)
	if got, ok := commandAction.With["use_commit_signing"].(bool); !ok || !got {
		t.Fatal("interactive action must skip Git credential installation")
	}
	if got := commandAction.With["exclude_comments_by_actor"]; got != "github-actions[bot]" {
		t.Fatalf("interactive actor exclusion = %#v", got)
	}
	if got, want := sortedInputKeys(commandAction), []string{
		"anthropic_api_key", "claude_args", "exclude_comments_by_actor", "github_token", "use_commit_signing",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("interactive Claude inputs = %v, want %v", got, want)
	}
	commandArgs := stringInput(t, commandAction, "claude_args")

	credentialPostcondition := namedStep(t, command, "Verify credential-free Claude origin")
	if credentialPostcondition.If != "always() && steps.local_origin.outputs.ready == 'true'" {
		t.Fatalf("credential postcondition if = %q", credentialPostcondition.If)
	}
	requireFragments(t, credentialPostcondition.Run,
		`git remote get-url --all origin`, `git remote get-url --push --all origin`,
		`fetch.recurseSubmodules`, `credential_config_pattern=`, `--local --global --system`,
		`git hash-object AGENTS.md`, `git hash-object CLAUDE.md`, `S AGENTS.md`)
	commandTerminal := namedStep(t, command, "Verify terminal Claude result")
	requireFragments(t, commandTerminal.Run,
		`-z "$EXECUTION_FILE"`, `! -f "$EXECUTION_FILE"`, `! -s "$EXECUTION_FILE"`,
		`repos/$GITHUB_REPOSITORY/pulls/$PR_NUMBER`,
		`gh api graphql`, `defaultBranchRef{name target{... on Commit{oid}}}`,
		`git --git-dir="$LOCAL_ORIGIN" rev-parse --verify "refs/heads/$EXPECTED_HEAD_REF"`,
		`git rev-parse --verify "refs/heads/$EXPECTED_HEAD_REF"`,
		`git rev-parse --verify "refs/remotes/origin/$EXPECTED_HEAD_REF"`,
		`"$current_state" != "open"`, `"$current_default_branch" != "$TRUSTED_DEFAULT_REF"`,
		`"$current_head_ref" == "$current_default_branch"`,
		`"$current_head" != "$EXPECTED_HEAD_SHA"`, `"$current_base" != "$EXPECTED_BASE_SHA"`)

	if got, want := automatic.On, map[string]trigger{
		"pull_request_target": {Types: []string{"opened", "synchronize", "reopened", "ready_for_review"}},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("automatic events = %#v, want %#v", got, want)
	}
	if _, ok := automatic.On["pull_request"]; ok {
		t.Fatal("secret-bearing automatic review must not load pull-request-authored workflow YAML")
	}
	review := automatic.Jobs["review"]
	if got, want := review.Permissions, map[string]string{
		"contents": "read", "issues": "read", "pull-requests": "write",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("automatic permissions = %#v, want %#v", got, want)
	}
	requireFragments(t, review.If,
		"github.event.pull_request.state == 'open'",
		"github.event.pull_request.user.type != 'Bot'",
		"github.event.pull_request.draft == false",
		"github.event.pull_request.head.repo.full_name == github.repository",
		"github.event.pull_request.base.repo.full_name == github.repository",
		"github.event.pull_request.head.ref != github.event.repository.default_branch")
	if strings.Contains(review.If, "github.actor") {
		t.Fatal("automatic bot guard must use the PR author, not the event actor")
	}
	reviewCheckout := namedStep(t, review, "Checkout repository")
	if reviewCheckout.With["fetch-depth"] != 0 || reviewCheckout.With["persist-credentials"] != false {
		t.Fatalf("automatic checkout = %#v", reviewCheckout.With)
	}
	if got := reviewCheckout.With["ref"]; got != "${{ github.sha }}" {
		t.Fatalf("automatic checkout must stay on the trusted default-branch event SHA, got %#v", got)
	}
	reviewOrigin := namedStep(t, review, "Prepare credential-free review origin")
	if strings.Count(reviewOrigin.Run, helperGuard) != 2 || strings.Count(reviewOrigin.Run, extraheaderGuard) != 2 {
		t.Fatal("automatic local origin must reject credential config around base restore proof")
	}
	requireFragments(t, reviewOrigin.Run,
		`"$EXPECTED_STATE" != "open"`, `"$EXPECTED_HEAD_REF" == "$TRUSTED_DEFAULT_REF"`,
		`^[A-Za-z0-9@_][A-Za-z0-9/_.#+,@-]*$`,
		`[[ "$1" != "@" ]]`,
		`git checkout --quiet --detach "$trusted_start_sha"`,
		`git config --local fetch.recurseSubmodules false`,
		`git update-index --skip-worktree AGENTS.md`,
		`echo "path=$local_origin"`,
		`echo "trusted_guidance_oid=$trusted_guidance_oid"`,
		`echo "trusted_start_sha=$trusted_start_sha"`,
		`echo "review_marker=<!-- claude-review:$GITHUB_REPOSITORY:pr-$PR_NUMBER:run-$RUN_ID:`+
			`attempt-$RUN_ATTEMPT:head-$EXPECTED_HEAD_SHA -->"`,
		`git fetch origin "$EXPECTED_BASE_REF" --depth=1 --no-recurse-submodules`)
	reviewTarget := namedStep(t, review, "Verify current review target")
	requireFragments(t, reviewTarget.Run,
		`repos/$GITHUB_REPOSITORY/pulls/$PR_NUMBER`, `.state`, `.head.ref`, `.base.repo.default_branch`,
		`"$current_state" != "open"`, `"$current_head_ref" == "$current_default_branch"`)

	reviewAction := namedStep(t, review, "Run Claude Code Review")
	if reviewAction.ID != "claude_review" || reviewAction.Uses != claudeAction {
		t.Fatalf("automatic action = id %q uses %q", reviewAction.ID, reviewAction.Uses)
	}
	requireFragments(t, reviewAction.If, `steps.current_target.outputs.ready == 'true'`)
	if got, ok := reviewAction.With["use_commit_signing"].(bool); !ok || !got {
		t.Fatal("automatic action must skip Git credential installation")
	}
	if _, ok := reviewAction.With["exclude_comments_by_actor"]; ok {
		t.Fatal("automatic agent mode must not carry ignored tag-mode actor exclusions")
	}
	if got, want := sortedInputKeys(reviewAction), []string{
		"anthropic_api_key", "claude_args", "github_token", "prompt", "use_commit_signing",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("automatic Claude inputs = %v, want %v", got, want)
	}
	reviewArgs := stringInput(t, reviewAction, "claude_args")

	wantDisallowed := []string{
		"Bash", "Read", "Glob", "Grep", "LS", "Task", "Edit", "Write", "MultiEdit", "NotebookEdit", "WebFetch", "WebSearch",
		"mcp__github_file_ops__commit_files", "mcp__github_file_ops__delete_files",
		"mcp__github__create_or_update_file", "mcp__github__push_files", "mcp__github__delete_file",
	}
	wantCommandAllowed := []string{
		"mcp__github__get_pull_request", "mcp__github__get_pull_request_diff", "mcp__github__get_pull_request_files",
		"mcp__github__get_pull_request_review_comments", "mcp__github__get_pull_request_reviews", "mcp__github__get_pull_request_status",
		"mcp__github__get_file_contents", "mcp__github__get_issue", "mcp__github__get_issue_comments", "mcp__github__search_issues",
		"mcp__github__search_pull_requests", "mcp__github__list_issues", "mcp__github__list_pull_requests",
		"mcp__github_inline_comment__create_inline_comment",
	}
	wantReviewAllowed := append(append([]string(nil), wantCommandAllowed[:len(wantCommandAllowed)-1]...), "mcp__github__add_issue_comment")
	for name, args := range map[string]string{"interactive": commandArgs, "automatic": reviewArgs} {
		if got := csvFlagFromArgs(t, args, "--disallowed-tools"); !reflect.DeepEqual(got, wantDisallowed) {
			t.Errorf("%s disallowed tools = %v, want %v", name, got, wantDisallowed)
		}
	}
	if got := csvFlagFromArgs(t, commandArgs, "--allowed-tools"); !reflect.DeepEqual(got, wantCommandAllowed) {
		t.Errorf("interactive allowed tools = %v, want %v", got, wantCommandAllowed)
	}
	if got := csvFlagFromArgs(t, reviewArgs, "--allowed-tools"); !reflect.DeepEqual(got, wantReviewAllowed) {
		t.Errorf("automatic allowed tools = %v, want %v", got, wantReviewAllowed)
	}

	if got := []string{modelFromArgs(t, commandArgs), modelFromArgs(t, reviewArgs)}; !reflect.DeepEqual(got, []string{claudeModel, claudeModel}) {
		t.Fatalf("Claude models are not locked: %v", got)
	}
	var claudeInvocations []step
	for _, candidate := range []workflow{interactive, automatic} {
		for _, candidateJob := range candidate.Jobs {
			for _, candidateStep := range candidateJob.Steps {
				if strings.HasPrefix(candidateStep.Uses, "anthropics/claude-code-action@") {
					claudeInvocations = append(claudeInvocations, candidateStep)
				}
			}
		}
	}
	if len(claudeInvocations) != 2 {
		t.Fatalf("Claude action invocation count = %d, want 2", len(claudeInvocations))
	}
	for _, invocation := range claudeInvocations {
		if invocation.Uses != claudeAction || modelFromArgs(t, stringInput(t, invocation, "claude_args")) != claudeModel {
			t.Fatalf("Claude action/model drift in %q", invocation.Name)
		}
	}
	reviewTerminal := namedStep(t, review, "Verify terminal Claude review")
	requireFragments(t, reviewTerminal.Run,
		`-z "$EXECUTION_FILE"`, `! -f "$EXECUTION_FILE"`, `! -s "$EXECUTION_FILE"`,
		`git remote get-url --all origin`, `git remote get-url --push --all origin`, helperGuard,
		`"$local_head" != "$TRUSTED_START_SHA"`,
		`git rev-parse --verify "refs/heads/$EXPECTED_HEAD_REF"`,
		`git rev-parse --verify "refs/remotes/origin/$EXPECTED_HEAD_REF"`,
		`git hash-object AGENTS.md`, `git hash-object CLAUDE.md`,
		`"$current_state" != "open"`, `"$current_default_branch" != "$TRUSTED_DEFAULT_REF"`,
		`"$current_head_ref" == "$current_default_branch"`,
		`"$current_head" != "$EXPECTED_HEAD_SHA"`, `"$current_base" != "$EXPECTED_BASE_SHA"`,
		`timeout 30s gh api --paginate`, `github-actions[bot]`, `endswith("\n" + $marker)`,
		`sub("[\\r\\n]+$"; "")`, `indices($marker)`, `rtrimstr($marker)`,
		`test("[^[:space:]]")`, `length) == 1`)
	reviewCredentialPostcondition := namedStep(t, review, "Verify credential-free Claude review origin")
	if reviewCredentialPostcondition.If != "always() && steps.review_origin.outputs.ready == 'true'" {
		t.Fatalf("automatic credential postcondition if = %q", reviewCredentialPostcondition.If)
	}
	requireFragments(t, reviewCredentialPostcondition.Run,
		`credential_config_pattern=`, `--local --global --system`,
		`git hash-object AGENTS.md`, `git hash-object CLAUDE.md`)
	requireFragments(t, stringInput(t, reviewAction, "prompt"),
		"AUTHORIZED HEAD REPO:", "AUTHORIZED HEAD REF:", "AUTHORIZED HEAD SHA:",
		"AUTHORIZED BASE REPO:", "AUTHORIZED BASE REF:", "AUTHORIZED BASE SHA:",
		"Read CLAUDE.md", "AUTHORIZED BASE SHA", "untrusted review data",
		"exactly one", "steps.review_origin.outputs.review_marker")
}

func TestURLScopedCredentialHelperMatchesGuard(t *testing.T) {
	dir := t.TempDir()
	for _, args := range [][]string{
		{"init", "--quiet", dir},
		{"-C", dir, "config", "--local", "credential.https://github.com.helper", "store"},
	} {
		if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	output, err := exec.Command(
		"git", "-C", dir, "config", "--local", "--get-regexp", helperPattern,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("scoped helper escaped guard: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "credential.https://github.com.helper") {
		t.Fatalf("unexpected guard match: %s", output)
	}
}

type reviewFixture struct {
	seed      string
	workspace string
	baseSHA   string
	headSHA   string
	headRef   string
}

func runCommand(t *testing.T, dir string, env []string, name string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	if env != nil {
		cmd.Env = env
	}
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func mustGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	output, err := runCommand(t, dir, nil, "git", args...)
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
	return strings.TrimSpace(output)
}

func gitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	return mustGit(t, dir, args...)
}

func createReviewFixture(t *testing.T) reviewFixture {
	t.Helper()
	root := t.TempDir()
	seed := filepath.Join(root, "seed")
	workspace := filepath.Join(root, "workspace")
	mustGit(t, root, "init", "--quiet", "-b", "main", seed)
	mustGit(t, seed, "config", "user.email", "fixture@example.com")
	mustGit(t, seed, "config", "user.name", "fixture")
	if err := os.WriteFile(filepath.Join(seed, "AGENTS.md"), []byte("trusted base guidance\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("AGENTS.md", filepath.Join(seed, "CLAUDE.md")); err != nil {
		t.Fatal(err)
	}
	mustGit(t, seed, "add", "AGENTS.md", "CLAUDE.md")
	mustGit(t, seed, "commit", "--quiet", "-m", "trusted guidance")
	for i := range 2 {
		mustGit(t, seed, "commit", "--quiet", "--allow-empty", "-m", fmt.Sprintf("base-%d", i))
	}
	baseSHA := gitOutput(t, seed, "rev-parse", "HEAD")
	mustGit(t, seed, "switch", "--quiet", "-c", "feature/deep")
	if err := os.WriteFile(filepath.Join(seed, "AGENTS.md"), []byte("untrusted pull request guidance\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mustGit(t, seed, "add", "AGENTS.md")
	mustGit(t, seed, "commit", "--quiet", "-m", "untrusted guidance")
	for i := range 24 {
		mustGit(t, seed, "commit", "--quiet", "--allow-empty", "-m", fmt.Sprintf("head-%d", i+1))
	}
	headSHA := gitOutput(t, seed, "rev-parse", "HEAD")
	mustGit(t, root, "clone", "--quiet", "--no-local", seed, workspace)
	mustGit(t, workspace, "checkout", "--quiet", "--detach", baseSHA)
	return reviewFixture{seed: seed, workspace: workspace, baseSHA: baseSHA, headSHA: headSHA, headRef: "feature/deep"}
}

func cleanEnvironment(t *testing.T, home string, overrides map[string]string) []string {
	t.Helper()
	blocked := map[string]bool{
		"GH_TOKEN": true, "GITHUB_TOKEN": true, "OVERRIDE_GITHUB_TOKEN": true,
		"DEFAULT_WORKFLOW_TOKEN": true, "GIT_ASKPASS": true, "SSH_ASKPASS": true,
		"GIT_CONFIG": true, "GIT_CONFIG_GLOBAL": true, "GIT_CONFIG_SYSTEM": true,
		"GIT_CONFIG_COUNT": true, "GIT_CONFIG_PARAMETERS": true,
	}
	values := make(map[string]string)
	for _, item := range os.Environ() {
		name, value, ok := strings.Cut(item, "=")
		if ok && !blocked[name] {
			values[name] = value
		}
	}
	values["HOME"] = home
	values["GIT_CONFIG_NOSYSTEM"] = "1"
	maps.Copy(values, overrides)
	result := make([]string, 0, len(values))
	for name, value := range values {
		result = append(result, name+"="+value)
	}
	sort.Strings(result)
	return result
}

func writeValidator(t *testing.T, wf workflow, path string) {
	t.Helper()
	validator := namedStep(t, wf.Jobs["claude"], "Materialize trusted Claude input validators").Env["VALIDATOR_SOURCE"].(string)
	if err := os.WriteFile(path, []byte(validator+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func readOutputs(t *testing.T, path string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]string)
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		name, value, ok := strings.Cut(line, "=")
		if ok {
			result[name] = value
		}
	}
	return result
}

func writeMockGH(t *testing.T, dir string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "gh")
	script := `#!/usr/bin/env bash
set -euo pipefail
if [[ "${2:-}" == "graphql" ]]; then
  printf '%s\n' "$MOCK_HEAD_REF" "$MOCK_HEAD_SHA"
  exit 0
fi
if [[ "$*" == *"/comments?per_page=100"* ]]; then
  case "${MOCK_COMMENT_MODE:-success}" in
    success)
      printf '[{"user":{"login":"github-actions[bot]","type":"Bot"},"body":"Automated review completed with no findings.\\n%s\\r\\n"}]\n' "$MOCK_REVIEW_MARKER"
      ;;
    missing)
      printf '[]\n'
      ;;
    wrong-actor)
      printf '[{"user":{"login":"claude[bot]","type":"Bot"},"body":"review\\n%s"}]\n' "$MOCK_REVIEW_MARKER"
      ;;
    stale)
      printf '[{"user":{"login":"github-actions[bot]","type":"Bot"},"body":"review\\n<!-- stale-review-marker -->"}]\n'
      ;;
    api-failure)
      exit 1
      ;;
    duplicate)
      printf '%s%s%s%s%s\n' \
        '[{"user":{"login":"github-actions[bot]","type":"Bot"},"body":"Automated review completed with no findings.\\n' \
        "$MOCK_REVIEW_MARKER" \
        '"},{"user":{"login":"github-actions[bot]","type":"Bot"},"body":"Duplicate automated review completed.\\n' \
        "$MOCK_REVIEW_MARKER" '"}]'
      ;;
    repeated-marker)
      printf '[{"user":{"login":"github-actions[bot]","type":"Bot"},"body":"Automated review completed.\\n%s\\n%s"}]\n' \
        "$MOCK_REVIEW_MARKER" "$MOCK_REVIEW_MARKER"
      ;;
    marker-only)
      printf '[{"user":{"login":"github-actions[bot]","type":"Bot"},"body":"%s"}]\n' "$MOCK_REVIEW_MARKER"
      ;;
    *)
      exit 2
      ;;
  esac
  exit 0
fi
printf '{"state":"%s","head":{"repo":{"full_name":"%s"},"sha":"%s","ref":"%s"},"base":{' \
  "${MOCK_PR_STATE:-open}" \
  "$GITHUB_REPOSITORY" "$MOCK_HEAD_SHA" "$MOCK_HEAD_REF"
printf '"repo":{"full_name":"%s","default_branch":"%s"},"sha":"%s","ref":"%s"},"commits":25}\n' \
  "$GITHUB_REPOSITORY" "${MOCK_DEFAULT_BRANCH:-main}" "$MOCK_BASE_SHA" "$MOCK_BASE_REF"
`
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	gitWrapper := fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "config" && "${2:-}" == "--system" ]]; then
  exit 1
fi
exec %q "$@"
`, realGit)
	gitPath := filepath.Join(dir, "git")
	if err := os.WriteFile(gitPath, []byte(gitWrapper), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(gitPath, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func requireScriptResult(t *testing.T, dir, script string, env []string, wantSuccess bool) string {
	t.Helper()
	output, err := runCommand(t, dir, env, "bash", "-c", script)
	if wantSuccess && err != nil {
		t.Fatalf("script failed: %v\n%s", err, output)
	}
	if !wantSuccess && err == nil {
		t.Fatalf("script unexpectedly succeeded:\n%s", output)
	}
	return output
}

func TestInteractiveClaudePRLifecycleFences(t *testing.T) {
	wf := loadWorkflow(t, "claude.yml")
	job := wf.Jobs["claude"]
	fixture := createReviewFixture(t)
	runnerTemp := t.TempDir()
	home := t.TempDir()
	validatorPath := filepath.Join(runnerTemp, "validators.sh")
	writeValidator(t, wf, validatorPath)
	bin := writeMockGH(t, filepath.Join(runnerTemp, "bin"))

	base := map[string]string{
		"PATH": bin + string(os.PathListSeparator) + os.Getenv("PATH"), "GH_TOKEN": "test-token",
		"GITHUB_REPOSITORY": "layervai/frp", "PR_NUMBER": "11", "VALIDATORS": validatorPath,
		"MOCK_HEAD_SHA": fixture.headSHA, "MOCK_HEAD_REF": fixture.headRef,
		"MOCK_BASE_SHA": fixture.baseSHA, "MOCK_BASE_REF": "main",
	}
	resolver := namedStep(t, job, "Resolve Claude pull request context").Run
	resolveValues := make(map[string]string, len(base)+1)
	maps.Copy(resolveValues, base)
	resolveOutput := filepath.Join(runnerTemp, "resolve-output")
	resolveValues["GITHUB_OUTPUT"] = resolveOutput
	requireScriptResult(t, fixture.workspace, resolver, cleanEnvironment(t, home, resolveValues), true)
	if outputs := readOutputs(t, resolveOutput); outputs["default_branch"] != "main" || outputs["checkout_allowed"] != "true" {
		t.Fatalf("resolver outputs = %#v", outputs)
	}

	for name, overrides := range map[string]map[string]string{
		"closed":       {"MOCK_PR_STATE": "closed"},
		"default-head": {"MOCK_HEAD_REF": "main"},
	} {
		t.Run("resolver-"+name, func(t *testing.T) {
			values := make(map[string]string, len(base)+len(overrides)+1)
			maps.Copy(values, base)
			maps.Copy(values, overrides)
			values["GITHUB_OUTPUT"] = filepath.Join(t.TempDir(), "output")
			requireScriptResult(t, fixture.workspace, resolver, cleanEnvironment(t, home, values), false)
		})
	}

	target := namedStep(t, job, "Verify current Claude target").Run
	targetBase := map[string]string{
		"PATH": bin + string(os.PathListSeparator) + os.Getenv("PATH"), "GH_TOKEN": "test-token",
		"GITHUB_REPOSITORY": "layervai/frp", "PR_MODE": "true", "PR_NUMBER": "11",
		"EXPECTED_HEAD_REF": fixture.headRef, "TRUSTED_DEFAULT_REF": "main", "VALIDATORS": validatorPath,
		"MOCK_HEAD_SHA": fixture.headSHA, "MOCK_HEAD_REF": fixture.headRef,
		"MOCK_BASE_SHA": fixture.baseSHA, "MOCK_BASE_REF": "main",
	}
	for name, overrides := range map[string]map[string]string{
		"open-feature": {},
		"closed":       {"MOCK_PR_STATE": "closed"},
		"default-head": {"MOCK_HEAD_REF": "main"},
	} {
		t.Run("pre-action-"+name, func(t *testing.T) {
			values := make(map[string]string, len(targetBase)+len(overrides)+1)
			maps.Copy(values, targetBase)
			maps.Copy(values, overrides)
			values["GITHUB_OUTPUT"] = filepath.Join(t.TempDir(), "output")
			requireScriptResult(t, fixture.workspace, target, cleanEnvironment(t, home, values), name == "open-feature")
		})
	}

	issueOutput := filepath.Join(t.TempDir(), "output")
	issueValues := map[string]string{"PR_MODE": "", "GITHUB_OUTPUT": issueOutput}
	requireScriptResult(t, fixture.workspace, target, cleanEnvironment(t, home, issueValues), true)
	if outputs := readOutputs(t, issueOutput); outputs["ready"] != "true" {
		t.Fatalf("issue-only target outputs = %#v", outputs)
	}
}

func TestAutomaticClaudePRLifecycleFences(t *testing.T) {
	wf := loadWorkflow(t, "claude-code-review.yml")
	target := namedStep(t, wf.Jobs["review"], "Verify current review target").Run
	fixture := createReviewFixture(t)
	runnerTemp := t.TempDir()
	home := t.TempDir()
	bin := writeMockGH(t, filepath.Join(runnerTemp, "bin"))
	base := map[string]string{
		"PATH": bin + string(os.PathListSeparator) + os.Getenv("PATH"), "GH_TOKEN": "test-token",
		"GITHUB_REPOSITORY": "layervai/frp", "PR_NUMBER": "11",
		"EXPECTED_HEAD_REF": fixture.headRef, "TRUSTED_DEFAULT_REF": "main",
		"MOCK_HEAD_SHA": fixture.headSHA, "MOCK_HEAD_REF": fixture.headRef,
		"MOCK_BASE_SHA": fixture.baseSHA, "MOCK_BASE_REF": "main",
	}
	for name, overrides := range map[string]map[string]string{
		"open-feature": {},
		"closed":       {"MOCK_PR_STATE": "closed"},
		"default-head": {"MOCK_HEAD_REF": "main"},
	} {
		t.Run(name, func(t *testing.T) {
			values := make(map[string]string, len(base)+len(overrides)+1)
			maps.Copy(values, base)
			maps.Copy(values, overrides)
			values["GITHUB_OUTPUT"] = filepath.Join(t.TempDir(), "output")
			requireScriptResult(t, fixture.workspace, target, cleanEnvironment(t, home, values), name == "open-feature")
		})
	}
}

func simulatePinnedPRCheckout(t *testing.T, fixture reviewFixture, origin string, depth int) {
	t.Helper()
	gitOutput(t, fixture.workspace, "fetch", "origin", fmt.Sprintf("--depth=%d", depth), fixture.headRef)
	gitOutput(t, fixture.workspace, "checkout", fixture.headRef, "--")
	gitOutput(t, fixture.workspace, "fetch", "origin", "main", "--depth=1", "--no-recurse-submodules")
	gitOutput(t, fixture.workspace, "checkout", "origin/main", "--", "CLAUDE.md")
	gitOutput(t, fixture.workspace, "reset", "--", "CLAUDE.md")
	if got := gitOutput(t, fixture.workspace, "remote", "get-url", "origin"); got != origin {
		t.Fatalf("origin = %q, want %q", got, origin)
	}
}

func TestInteractiveClaudeOriginRejectsDefaultBranchDriftBeforeGitWork(t *testing.T) {
	wf := loadWorkflow(t, "claude.yml")
	prepare := namedStep(t, wf.Jobs["claude"], "Prepare credential-free Claude origin").Run

	for name, overrides := range map[string]map[string]string{
		"event-default-mismatch": {"ISSUE_REF": "renamed-main"},
		"default-head":           {"PR_HEAD_REF": "main"},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := createReviewFixture(t)
			runnerTemp := t.TempDir()
			validatorPath := filepath.Join(runnerTemp, "validators.sh")
			writeValidator(t, wf, validatorPath)
			values := map[string]string{
				"PR_MODE": "true", "PR_HEAD_SHA": fixture.headSHA, "PR_HEAD_REF": fixture.headRef,
				"PR_BASE_SHA": fixture.baseSHA, "PR_BASE_REF": "main", "PR_DEFAULT_REF": "main",
				"PR_HEAD_FETCH_DEPTH": "25", "ISSUE_SHA": fixture.baseSHA, "ISSUE_REF": "main",
				"VALIDATORS": validatorPath, "RUNNER_TEMP": runnerTemp,
				"GITHUB_OUTPUT":    filepath.Join(runnerTemp, "prepare-output"),
				"GITHUB_WORKSPACE": fixture.workspace,
			}
			maps.Copy(values, overrides)
			originalOrigin := gitOutput(t, fixture.workspace, "remote", "get-url", "origin")
			requireScriptResult(t, fixture.workspace, prepare, cleanEnvironment(t, t.TempDir(), values), false)
			if got := gitOutput(t, fixture.workspace, "remote", "get-url", "origin"); got != originalOrigin {
				t.Fatalf("origin changed before default-branch fence: got %q, want %q", got, originalOrigin)
			}
			if got := gitOutput(t, fixture.workspace, "rev-parse", "HEAD"); got != fixture.baseSHA {
				t.Fatalf("HEAD changed before default-branch fence: got %s, want %s", got, fixture.baseSHA)
			}
		})
	}
}

func TestInteractiveClaudeRejectsSensitiveTreeIndirection(t *testing.T) {
	wf := loadWorkflow(t, "claude.yml")
	prepare := namedStep(t, wf.Jobs["claude"], "Prepare credential-free Claude origin").Run

	attacks := map[string]func(*testing.T, reviewFixture, string){
		"root-sensitive-symlink": func(t *testing.T, fixture reviewFixture, secret string) {
			t.Helper()
			if err := os.Symlink(secret, filepath.Join(fixture.seed, ".mcp.json")); err != nil {
				t.Fatal(err)
			}
		},
		"nested-sensitive-symlink": func(t *testing.T, fixture reviewFixture, secret string) {
			t.Helper()
			if err := os.MkdirAll(filepath.Join(fixture.seed, ".claude"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(secret, filepath.Join(fixture.seed, ".claude", "settings.json")); err != nil {
				t.Fatal(err)
			}
		},
		"wrong-root-claude-target": func(t *testing.T, fixture reviewFixture, _ string) {
			t.Helper()
			if err := os.Remove(filepath.Join(fixture.seed, "CLAUDE.md")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("README.md", filepath.Join(fixture.seed, "CLAUDE.md")); err != nil {
				t.Fatal(err)
			}
		},
		"symlinked-agents-target": func(t *testing.T, fixture reviewFixture, secret string) {
			t.Helper()
			if err := os.Remove(filepath.Join(fixture.seed, "AGENTS.md")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(secret, filepath.Join(fixture.seed, "AGENTS.md")); err != nil {
				t.Fatal(err)
			}
		},
		"nested-sensitive-gitlink": func(t *testing.T, fixture reviewFixture, _ string) {
			t.Helper()
			mustGit(t, fixture.seed, "update-index", "--add", "--cacheinfo",
				"160000,"+fixture.baseSHA+",.claude/foreign")
		},
	}

	for name, installAttack := range attacks {
		t.Run(name, func(t *testing.T) {
			fixture := createReviewFixture(t)
			secret := filepath.Join(t.TempDir(), "runner-secret")
			if err := os.WriteFile(secret, []byte("must-not-be-materialized\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			installAttack(t, fixture, secret)
			if name != "nested-sensitive-gitlink" {
				mustGit(t, fixture.seed, "add", "-A")
			}
			mustGit(t, fixture.seed, "commit", "--quiet", "-m", "malicious sensitive path")
			fixture.headSHA = gitOutput(t, fixture.seed, "rev-parse", "HEAD")
			mustGit(t, fixture.workspace, "fetch", "--quiet", "origin", fixture.headRef)

			runnerTemp := t.TempDir()
			validatorPath := filepath.Join(runnerTemp, "validators.sh")
			writeValidator(t, wf, validatorPath)
			env := cleanEnvironment(t, t.TempDir(), map[string]string{
				"PR_MODE": "true", "PR_HEAD_SHA": fixture.headSHA, "PR_HEAD_REF": fixture.headRef,
				"PR_BASE_SHA": fixture.baseSHA, "PR_BASE_REF": "main", "PR_DEFAULT_REF": "main",
				"PR_HEAD_FETCH_DEPTH": "26",
				"ISSUE_SHA":           fixture.baseSHA, "ISSUE_REF": "main", "VALIDATORS": validatorPath,
				"RUNNER_TEMP": runnerTemp, "GITHUB_OUTPUT": filepath.Join(runnerTemp, "prepare-output"),
				"GITHUB_WORKSPACE": fixture.workspace,
			})
			output := requireScriptResult(t, fixture.workspace, prepare, env, false)
			if !strings.Contains(output, "contains unsafe Claude startup path") {
				t.Fatalf("sensitive-tree guard failed for the wrong reason:\n%s", output)
			}
			if _, err := os.Stat(filepath.Join(fixture.workspace, ".claude-pr")); !os.IsNotExist(err) {
				t.Fatalf("action snapshot unexpectedly materialized before guard: %v", err)
			}
		})
	}
}

func TestInteractiveClaudeRuntimeContracts(t *testing.T) {
	wf := loadWorkflow(t, "claude.yml")
	job := wf.Jobs["claude"]
	fixture := createReviewFixture(t)
	runnerTemp := t.TempDir()
	home := t.TempDir()
	outputPath := filepath.Join(runnerTemp, "prepare-output")
	validatorPath := filepath.Join(runnerTemp, "validators.sh")
	writeValidator(t, wf, validatorPath)

	prepareEnv := cleanEnvironment(t, home, map[string]string{
		"PR_MODE": "true", "PR_HEAD_SHA": fixture.headSHA, "PR_HEAD_REF": fixture.headRef,
		"PR_BASE_SHA": fixture.baseSHA, "PR_BASE_REF": "main", "PR_DEFAULT_REF": "main",
		"PR_HEAD_FETCH_DEPTH": "25",
		"ISSUE_SHA":           fixture.baseSHA, "ISSUE_REF": "main", "VALIDATORS": validatorPath,
		"RUNNER_TEMP": runnerTemp, "GITHUB_OUTPUT": outputPath, "GITHUB_WORKSPACE": fixture.workspace,
	})
	requireScriptResult(t, fixture.workspace, namedStep(t, job, "Prepare credential-free Claude origin").Run, prepareEnv, true)
	outputs := readOutputs(t, outputPath)
	if outputs["ready"] != "true" || outputs["path"] == "" || outputs["trusted_guidance_oid"] == "" {
		t.Fatalf("prepare outputs = %#v", outputs)
	}
	if got := gitOutput(t, fixture.workspace, "hash-object", "AGENTS.md"); got != outputs["trusted_guidance_oid"] {
		t.Fatalf("trusted AGENTS.md = %s, want %s", got, outputs["trusted_guidance_oid"])
	}
	simulatePinnedPRCheckout(t, fixture, outputs["path"], 25)
	if got := gitOutput(t, fixture.workspace, "hash-object", "CLAUDE.md"); got != outputs["trusted_guidance_oid"] {
		t.Fatalf("trusted CLAUDE.md = %s, want %s", got, outputs["trusted_guidance_oid"])
	}
	if got := gitOutput(t, fixture.workspace, "rev-list", "--count", "refs/remotes/origin/"+fixture.headRef); got != "25" {
		t.Fatalf("deep head fetch count = %s, want 25", got)
	}

	bin := writeMockGH(t, filepath.Join(runnerTemp, "bin"))
	executionFile := filepath.Join(runnerTemp, "execution.json")
	if err := os.WriteFile(executionFile, []byte("completed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	common := map[string]string{
		"PATH":              bin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"GITHUB_REPOSITORY": "layervai/frp", "GITHUB_WORKSPACE": fixture.workspace,
		"EXPECTED_ORIGIN": outputs["path"], "LOCAL_ORIGIN": outputs["path"],
		"TRUSTED_GUIDANCE_OID": outputs["trusted_guidance_oid"],
		"PR_MODE":              "true", "PR_NUMBER": "11", "EXPECTED_HEAD_SHA": fixture.headSHA,
		"EXPECTED_HEAD_REF": fixture.headRef, "EXPECTED_BASE_SHA": fixture.baseSHA,
		"EXPECTED_BASE_REF": "main", "TRUSTED_DEFAULT_REF": "main", "EXECUTION_FILE": executionFile,
		"MOCK_HEAD_SHA": fixture.headSHA, "MOCK_HEAD_REF": fixture.headRef,
		"MOCK_BASE_SHA": fixture.baseSHA, "MOCK_BASE_REF": "main",
	}
	postEnv := cleanEnvironment(t, home, common)
	requireScriptResult(t, fixture.workspace, namedStep(t, job, "Verify credential-free Claude origin").Run, postEnv, true)
	terminalValues := make(map[string]string, len(common)+1)
	maps.Copy(terminalValues, common)
	terminalValues["GH_TOKEN"] = "test-token"
	terminalEnv := cleanEnvironment(t, home, terminalValues)
	terminal := namedStep(t, job, "Verify terminal Claude result").Run
	requireScriptResult(t, fixture.workspace, terminal, terminalEnv, true)
	for name, overrides := range map[string]map[string]string{
		"closed":       {"MOCK_PR_STATE": "closed"},
		"default-head": {"MOCK_HEAD_REF": "main"},
	} {
		t.Run("terminal-"+name, func(t *testing.T) {
			values := make(map[string]string, len(terminalValues)+len(overrides))
			maps.Copy(values, terminalValues)
			maps.Copy(values, overrides)
			requireScriptResult(t, fixture.workspace, terminal, cleanEnvironment(t, home, values), false)
		})
	}

	gitOutput(t, fixture.workspace, "remote", "set-url", "--add", "--push", "origin", "https://example.invalid/credential")
	requireScriptResult(t, fixture.workspace, namedStep(t, job, "Verify credential-free Claude origin").Run, postEnv, false)
	gitOutput(t, fixture.workspace, "config", "--unset-all", "remote.origin.pushurl")
	gitOutput(t, fixture.workspace, "update-ref", "refs/remotes/origin/"+fixture.headRef, fixture.baseSHA)
	requireScriptResult(t, fixture.workspace, terminal, terminalEnv, false)
	gitOutput(t, fixture.workspace, "update-ref", "refs/remotes/origin/"+fixture.headRef, fixture.headSHA)
	if err := os.WriteFile(filepath.Join(fixture.workspace, "AGENTS.md"), []byte("tampered\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	requireScriptResult(t, fixture.workspace, namedStep(t, job, "Verify credential-free Claude origin").Run, postEnv, false)
}

func TestAutomaticClaudeRuntimeContracts(t *testing.T) {
	wf := loadWorkflow(t, "claude-code-review.yml")
	job := wf.Jobs["review"]
	// Automatic agent mode must start on trusted default-branch content. The
	// PR head exists only as an object/ref for the local origin and GitHub MCP.
	fixture := createReviewFixture(t)
	secret := filepath.Join(t.TempDir(), "automatic-runner-secret")
	if err := os.WriteFile(secret, []byte("must-never-enter-automatic-workspace\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secret, filepath.Join(fixture.seed, ".mcp.json")); err != nil {
		t.Fatal(err)
	}
	mustGit(t, fixture.seed, "add", ".mcp.json")
	mustGit(t, fixture.seed, "commit", "--quiet", "-m", "malicious automatic sensitive path")
	fixture.headSHA = gitOutput(t, fixture.seed, "rev-parse", "HEAD")
	mustGit(t, fixture.workspace, "fetch", "--quiet", "origin", fixture.headRef)
	runnerTemp := t.TempDir()
	home := t.TempDir()
	outputPath := filepath.Join(runnerTemp, "prepare-output")
	prepareEnv := cleanEnvironment(t, home, map[string]string{
		"EXPECTED_STATE":    "open",
		"EXPECTED_HEAD_SHA": fixture.headSHA, "EXPECTED_HEAD_REF": fixture.headRef,
		"EXPECTED_BASE_SHA": fixture.baseSHA, "EXPECTED_BASE_REF": "main",
		"TRUSTED_DEFAULT_REF": "main", "PR_NUMBER": "11", "RUN_ID": "29893894149", "RUN_ATTEMPT": "2",
		"RUNNER_TEMP": runnerTemp, "GITHUB_OUTPUT": outputPath, "GITHUB_WORKSPACE": fixture.workspace,
		"GITHUB_REPOSITORY": "layervai/frp",
	})
	requireScriptResult(t, fixture.workspace, namedStep(t, job, "Prepare credential-free review origin").Run, prepareEnv, true)
	outputs := readOutputs(t, outputPath)
	wantMarker := "<!-- claude-review:layervai/frp:pr-11:run-29893894149:attempt-2:head-" + fixture.headSHA + " -->"
	if outputs["trusted_start_sha"] != fixture.baseSHA || outputs["review_marker"] != wantMarker {
		t.Fatalf("automatic trusted outputs = %#v", outputs)
	}
	if got := gitOutput(t, fixture.workspace, "rev-parse", "HEAD"); got != fixture.baseSHA {
		t.Fatalf("automatic workspace checked out PR head %s, want trusted base %s", got, fixture.baseSHA)
	}
	if _, err := os.Lstat(filepath.Join(fixture.workspace, ".mcp.json")); !os.IsNotExist(err) {
		t.Fatalf("PR-authored sensitive symlink entered automatic workspace: %v", err)
	}
	gitOutput(t, fixture.workspace, "fetch", "origin", "main", "--depth=1", "--no-recurse-submodules")
	gitOutput(t, fixture.workspace, "checkout", "origin/main", "--", "CLAUDE.md")
	gitOutput(t, fixture.workspace, "reset", "--", "CLAUDE.md")

	bin := writeMockGH(t, filepath.Join(runnerTemp, "bin"))
	executionFile := filepath.Join(runnerTemp, "execution.json")
	if err := os.WriteFile(executionFile, []byte("completed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	common := map[string]string{
		"PATH":              bin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"GITHUB_REPOSITORY": "layervai/frp", "GITHUB_WORKSPACE": fixture.workspace,
		"EXPECTED_ORIGIN": outputs["path"], "LOCAL_ORIGIN": outputs["path"],
		"TRUSTED_GUIDANCE_OID": outputs["trusted_guidance_oid"],
		"TRUSTED_START_SHA":    outputs["trusted_start_sha"],
		"PR_NUMBER":            "11", "EXPECTED_HEAD_SHA": fixture.headSHA, "EXPECTED_HEAD_REF": fixture.headRef,
		"EXPECTED_BASE_SHA": fixture.baseSHA, "EXPECTED_BASE_REF": "main", "TRUSTED_DEFAULT_REF": "main",
		"EXECUTION_FILE": executionFile,
		"REVIEW_MARKER":  outputs["review_marker"], "MOCK_REVIEW_MARKER": outputs["review_marker"],
		"MOCK_COMMENT_MODE": "success",
		"MOCK_HEAD_SHA":     fixture.headSHA, "MOCK_HEAD_REF": fixture.headRef,
		"MOCK_BASE_SHA": fixture.baseSHA, "MOCK_BASE_REF": "main",
	}
	postEnv := cleanEnvironment(t, home, common)
	requireScriptResult(t, fixture.workspace, namedStep(t, job, "Verify credential-free Claude review origin").Run, postEnv, true)
	terminalValues := make(map[string]string, len(common)+1)
	maps.Copy(terminalValues, common)
	terminalValues["GH_TOKEN"] = "test-token"
	terminalEnv := cleanEnvironment(t, home, terminalValues)
	terminal := namedStep(t, job, "Verify terminal Claude review").Run
	requireScriptResult(t, fixture.workspace, terminal, terminalEnv, true)
	for name, overrides := range map[string]map[string]string{
		"closed":       {"MOCK_PR_STATE": "closed"},
		"default-head": {"MOCK_HEAD_REF": "main"},
	} {
		t.Run("target-"+name, func(t *testing.T) {
			values := make(map[string]string, len(terminalValues)+len(overrides))
			maps.Copy(values, terminalValues)
			maps.Copy(values, overrides)
			requireScriptResult(t, fixture.workspace, terminal, cleanEnvironment(t, home, values), false)
		})
	}
	for _, mode := range []string{
		"missing", "wrong-actor", "stale", "api-failure", "duplicate", "repeated-marker", "marker-only",
	} {
		t.Run("comment-"+mode, func(t *testing.T) {
			values := make(map[string]string, len(terminalValues))
			maps.Copy(values, terminalValues)
			values["MOCK_COMMENT_MODE"] = mode
			requireScriptResult(t, fixture.workspace, terminal, cleanEnvironment(t, home, values), false)
		})
	}

	gitOutput(t, fixture.workspace, "branch", "-f", fixture.headRef, fixture.baseSHA)
	requireScriptResult(t, fixture.workspace, terminal, terminalEnv, false)
	gitOutput(t, fixture.workspace, "branch", "-f", fixture.headRef, fixture.headSHA)
	gitOutput(t, fixture.workspace, "remote", "set-url", "--add", "origin", "https://example.invalid/credential")
	requireScriptResult(t, fixture.workspace, namedStep(t, job, "Verify credential-free Claude review origin").Run, postEnv, false)
	gitOutput(t, fixture.workspace, "remote", "set-url", "--delete", "origin", "https://example.invalid/credential")
	if err := os.WriteFile(executionFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	requireScriptResult(t, fixture.workspace, terminal, terminalEnv, false)
}

func TestIssueClaudeRuntimeAndShallowRegression(t *testing.T) {
	wf := loadWorkflow(t, "claude.yml")
	job := wf.Jobs["claude"]
	fixture := createReviewFixture(t)
	runnerTemp := t.TempDir()
	home := t.TempDir()
	validatorPath := filepath.Join(runnerTemp, "validators.sh")
	writeValidator(t, wf, validatorPath)
	outputPath := filepath.Join(runnerTemp, "prepare-output")
	prepareValues := map[string]string{
		"PR_MODE": "", "PR_HEAD_SHA": "", "PR_HEAD_REF": "", "PR_BASE_SHA": "", "PR_BASE_REF": "",
		"PR_DEFAULT_REF": "", "PR_HEAD_FETCH_DEPTH": "", "ISSUE_SHA": fixture.baseSHA, "ISSUE_REF": "main",
		"VALIDATORS": validatorPath, "RUNNER_TEMP": runnerTemp, "GITHUB_OUTPUT": outputPath,
		"GITHUB_WORKSPACE": fixture.workspace,
	}
	prepareEnv := cleanEnvironment(t, home, prepareValues)
	requireScriptResult(t, fixture.workspace, namedStep(t, job, "Prepare credential-free Claude origin").Run, prepareEnv, true)
	outputs := readOutputs(t, outputPath)
	gitOutput(t, fixture.workspace, "fetch", "origin", "main", "--depth=1")
	gitOutput(t, fixture.workspace, "checkout", "main", "--")

	bin := writeMockGH(t, filepath.Join(runnerTemp, "bin"))
	executionFile := filepath.Join(runnerTemp, "execution.json")
	if err := os.WriteFile(executionFile, []byte("completed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	terminalValues := map[string]string{
		"PATH": bin + string(os.PathListSeparator) + os.Getenv("PATH"), "GH_TOKEN": "test-token",
		"GITHUB_REPOSITORY": "layervai/frp", "GITHUB_WORKSPACE": fixture.workspace,
		"PR_MODE": "", "PR_NUMBER": "42", "EXPECTED_HEAD_SHA": fixture.baseSHA,
		"EXPECTED_HEAD_REF": "main", "EXPECTED_BASE_SHA": "", "EXPECTED_BASE_REF": "",
		"EXECUTION_FILE": executionFile, "LOCAL_ORIGIN": outputs["path"],
		"TRUSTED_GUIDANCE_OID": outputs["trusted_guidance_oid"],
		"MOCK_HEAD_SHA":        fixture.baseSHA, "MOCK_HEAD_REF": "main", "MOCK_BASE_SHA": "", "MOCK_BASE_REF": "",
	}
	requireScriptResult(t, fixture.workspace, namedStep(t, job, "Verify terminal Claude result").Run,
		cleanEnvironment(t, home, terminalValues), true)
	staleValues := make(map[string]string, len(terminalValues))
	maps.Copy(staleValues, terminalValues)
	staleValues["MOCK_HEAD_REF"] = "renamed-default"
	requireScriptResult(t, fixture.workspace, namedStep(t, job, "Verify terminal Claude result").Run,
		cleanEnvironment(t, home, staleValues), false)

	shallowRoot := t.TempDir()
	shallowWorkspace := filepath.Join(shallowRoot, "workspace")
	mustGit(t, shallowRoot, "clone", "--quiet", "--depth=1", "--branch", "main",
		"file://"+filepath.Dir(fixture.workspace)+"/seed", shallowWorkspace)
	shallowOutput := filepath.Join(shallowRoot, "prepare-output")
	shallowValues := make(map[string]string, len(prepareValues))
	maps.Copy(shallowValues, prepareValues)
	shallowValues["RUNNER_TEMP"] = shallowRoot
	shallowValues["GITHUB_OUTPUT"] = shallowOutput
	shallowValues["GITHUB_WORKSPACE"] = shallowWorkspace
	output := requireScriptResult(t, shallowWorkspace, namedStep(t, job, "Prepare credential-free Claude origin").Run,
		cleanEnvironment(t, home, shallowValues), false)
	if !strings.Contains(output, "shallow update not allowed") {
		t.Fatalf("prior shallow-origin failure was not reproduced:\n%s", output)
	}
}
