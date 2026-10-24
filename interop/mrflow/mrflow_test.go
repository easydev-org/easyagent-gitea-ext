// Package e2e drives the REAL gitea-ext binary over a LOCAL nats-server, with no
// agent runtime and no LLM in the path.
//
// Everything the test needs is started by the test:
//   - a local nats-server (natsrun, memory storage),
//   - a stub worker that actually runs git in a temp dir (no cluster sandbox),
//   - the gitea-ext binary itself, as a child process.
//
// Nothing is borrowed from a shared cluster broker: a test that reuses a running
// extension cannot run in parallel with itself, and its config leaks between runs.
//
// Only the FORGE is external, because the point is to prove the real Gitea API
// round-trip (branch push + merge request) rather than a mock of it.
package e2e

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"

	easybus "github.com/easydev-org/easybus-sdk"
	easybusagent "github.com/easydev-org/easybus-sdk/agent"
	"github.com/easydev-org/easybus-sdk/natsrun"
	"github.com/easydev-org/easybus-sdk/transport/nats"

	workerv1 "github.com/easydev-org/easyagent-gitea-ext/gen/worker/v1"
	"github.com/easydev-org/easyagent-gitea-ext/gen/worker/v1/workerv1connect"
)

// job is one finished command: its output lines and exit status.
type job struct {
	lines []string
	exit  int32
	done  bool
}

// stubWorker runs shell commands in a temp dir, standing in for a real sandbox.
//
// It REPORTS THE EXIT CODE. An earlier version always answered ExitCode=0, which
// made every failing git command look successful: `git add` could fail and the
// tool still printed "Staged 1 path(s)", and the empty commit that followed only
// surfaced much later as "no commits that main does not already have".
type stubWorker struct {
	workerv1connect.UnimplementedWorkerServiceHandler
	root string

	mu   sync.Mutex
	jobs map[string]*job
	// stdins lets JobStdin feed a command that is waiting for a credential.
	stdins map[string]chan []byte
}

func newStubWorker(root string) *stubWorker {
	return &stubWorker{root: root, jobs: map[string]*job{}, stdins: map[string]chan []byte{}}
}

func (s *stubWorker) Execute(_ context.Context, req *workerv1.ExecuteRequest) (*workerv1.ExecuteResponse, error) {
	id := fmt.Sprintf("j%d", time.Now().UnixNano())
	inCh := make(chan []byte, 1)
	s.mu.Lock()
	s.jobs[id] = &job{}
	s.stdins[id] = inCh
	s.mu.Unlock()

	dir := s.root
	if wd := req.GetWorkdir(); wd != "" {
		dir = filepath.Join(s.root, wd)
	}
	// Run in the background and answer immediately, the way a real worker does:
	// the caller learns the outcome through JobWait/JobOutput, not the return.
	go func() {
		cmd := exec.Command("sh", "-c", req.GetCommand())
		cmd.Dir = dir
		stdin, err := cmd.StdinPipe()
		var out strings.Builder
		if err != nil {
			s.finish(id, []string{err.Error()}, 1)
			return
		}
		if req.GetTimeoutMs() > 0 {
			// Best-effort deadline, mirroring the worker's contract.
			t := time.AfterFunc(time.Duration(req.GetTimeoutMs())*time.Millisecond, func() {
				if cmd.Process != nil {
					_ = cmd.Process.Kill()
				}
			})
			defer t.Stop()
		}
		go func() {
			select {
			case data := <-inCh:
				_, _ = stdin.Write(data)
			case <-time.After(3 * time.Second):
				// The command did not ask for a credential.
			}
			_ = stdin.Close()
		}()
		cmd.Stdout = &out
		cmd.Stderr = &out
		exit := int32(0)
		if err := cmd.Run(); err != nil {
			if ee, ok := err.(*exec.ExitError); ok {
				exit = int32(ee.ExitCode())
			} else {
				exit = 1
				out.WriteString("\n" + err.Error())
			}
		}
		lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
		s.finish(id, lines, exit)
	}()
	return &workerv1.ExecuteResponse{JobId: id}, nil
}

func (s *stubWorker) finish(id string, lines []string, exit int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobs[id]
	if j == nil {
		return
	}
	j.lines, j.exit, j.done = lines, exit, true
}

func (s *stubWorker) JobStdin(_ context.Context, req *workerv1.JobStdinRequest) (*workerv1.JobStdinResponse, error) {
	s.mu.Lock()
	ch := s.stdins[req.GetJobId()]
	s.mu.Unlock()
	if ch != nil {
		select {
		case ch <- req.GetData():
		default:
		}
	}
	return &workerv1.JobStdinResponse{}, nil
}

func (s *stubWorker) JobWait(ctx context.Context, req *workerv1.JobWaitRequest) (*workerv1.JobWaitResponse, error) {
	// The caller polls; answer "done" only once the command has actually finished.
	deadline := time.Now().Add(30 * time.Second)
	for {
		s.mu.Lock()
		j := s.jobs[req.GetJobId()]
		done, exit := false, int32(0)
		if j != nil {
			done, exit = j.done, j.exit
		}
		s.mu.Unlock()
		if done {
			return &workerv1.JobWaitResponse{State: "done", ExitCode: exit}, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return &workerv1.JobWaitResponse{State: "timeout"}, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (s *stubWorker) JobOutput(_ context.Context, req *workerv1.JobOutputRequest) (*workerv1.JobOutputResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobs[req.GetJobId()]
	if j == nil {
		return &workerv1.JobOutputResponse{Done: true}, nil
	}
	return &workerv1.JobOutputResponse{Lines: j.lines, TotalLines: int32(len(j.lines)), Done: j.done}, nil
}

// TestMRWorkflow is the E2E: push refused -> clone -> edit -> commit -> submit MR
// -> list -> show -> comment -> merge, all through the extension's own handlers.
func TestMRWorkflow(t *testing.T) {
	extBin := os.Getenv("E2E_EXT_BIN")
	remote := os.Getenv("E2E_REMOTE")
	token := os.Getenv("E2E_GITEA_TOKEN")
	apiURL := os.Getenv("E2E_GITEA_API")
	host := os.Getenv("E2E_GITEA_HOST")
	user := os.Getenv("E2E_GITEA_USER")
	if extBin == "" || remote == "" || token == "" || apiURL == "" || host == "" {
		t.Skip("set E2E_EXT_BIN, E2E_REMOTE, E2E_GITEA_TOKEN, E2E_GITEA_API, E2E_GITEA_HOST")
	}
	if _, err := os.Stat(extBin); err != nil {
		t.Fatalf("extension binary: %v", err)
	}

	// ---- 1. LOCAL bus -------------------------------------------------------
	srv, err := natsrun.Start(natsrun.Config{
		Binary:  os.Getenv("EASYBUS_NATS_SERVER_BIN"),
		Storage: natsrun.Memory,
	})
	if err != nil {
		t.Fatalf("start local nats-server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	t.Logf("local nats: %s", srv.URL())

	// ---- 2. stub worker ----------------------------------------------------
	root := t.TempDir()
	st := newStubWorker(root)
	cs := connect.NewServer()
	workerv1connect.RegisterWorkerServiceHandler(cs, st)
	mux := http.NewServeMux()
	connecthttp.Mount(mux, cs, connecthttp.WithReadMaxBytes(0))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hs := &http.Server{Handler: mux}
	go func() { _ = hs.Serve(ln) }()
	t.Cleanup(func() { _ = hs.Close() })
	workerURL := "http://" + ln.Addr().String()
	t.Logf("stub worker: %s (root %s)", workerURL, root)

	// ---- 3. the REAL extension ---------------------------------------------
	ext := exec.Command(extBin)
	ext.Env = append(os.Environ(), "NATS_URL="+srv.URL())
	ext.Stderr = os.Stderr // surface startup failures instead of hiding them
	if err := ext.Start(); err != nil {
		t.Fatalf("start extension: %v", err)
	}
	t.Cleanup(func() { _ = ext.Process.Kill(); _ = ext.Wait() })

	// ---- 4. configure ------------------------------------------------------
	bus, err := nats.Connect(srv.URL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer bus.Close()
	a := easybusagent.New(bus)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	// The child needs a moment to publish its presence; Discover is a snapshot.
	var manifests []*easybus.ExtensionManifest
	for i := 0; i < 40; i++ {
		if manifests, err = a.Discover(ctx, 500); err == nil && len(manifests) > 0 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if len(manifests) == 0 {
		t.Fatalf("the extension never joined the local bus (started %s)", extBin)
	}
	a.ServeConfig(true)
	ids := make([]string, 0, len(manifests))
	for _, m := range manifests {
		ids = append(ids, m.Id)
	}
	if !containsStr(ids, "gitea") {
		t.Fatalf("the extension did not register as gitea; ids=%v", ids)
	}
	t.Logf("extensions: %v", ids)

	const tenant, extID = "t1", "gitea"
	must(t, a.SetConfig(ctx, tenant, extID, "sandboxes",
		fmt.Sprintf(`[{"name":"pod-a","url":%q,"token":""}]`, workerURL), "", nil, nil))
	must(t, a.SetConfig(ctx, tenant, extID, "servers",
		// `user` is REQUIRED for push: Gitea authenticates <real-user>:<token> and
		// looks the username up as an account, so a placeholder yields the opaque
		// "remote: Verify" (or "Unauthorized" if the token is put there instead).
		fmt.Sprintf(`[{"host":%q,"kind":"gitea","token":%q,"user":%q,"api_url":%q}]`,
			host, token, user, apiURL),
		"", nil, nil))

	call := func(tool string, args map[string]any) (string, error) {
		// With GRANTS: the writing tools are permission-gated, so a call without them is testing
		// the refusal path rather than the behaviour this e2e exists for. The grants mirror what
		// an operator would issue for this repository — the point of the test is the review
		// workflow, and the gate is covered by the extension's own unit tests.
		tr, err := a.CallToolWithGrants(ctx, tenant, "s", extID, tool,
			fmt.Sprintf("c-%d", time.Now().UnixNano()), args,
			grantsForAll())
		if err != nil {
			return "", err
		}
		if tr.Error != nil {
			return "", fmt.Errorf("%s: %s", tool, tr.Error.Message)
		}
		return strings.TrimSpace(tr.Content), nil
	}
	mustCall := func(tool string, args map[string]any) string {
		out, err := call(tool, args)
		if err != nil {
			t.Fatalf("%v", err)
		}
		t.Logf("%-15s %s", tool, clip(out))
		return out
	}

	// ---- 5. THE POLICY -----------------------------------------------------
	if out, err := call("git-push", map[string]any{"sandbox": "pod-a", "path": "repo"}); err == nil {
		t.Fatalf("git-push SUCCEEDED (%q); direct push must be refused", out)
	} else {
		if !strings.Contains(err.Error(), "git-submit-mr") {
			t.Errorf("the refusal must name git-submit-mr: %v", err)
		}
		t.Logf("git-push refused: %s", clip(err.Error()))
	}

	// ---- 6. clone, edit, commit -------------------------------------------
	mustCall("git-clone", map[string]any{
		"sandbox": "pod-a", "path": "repo", "remote": remote, "depth": float64(0),
	})
	repoDir := filepath.Join(root, "repo")
	if _, err := os.Stat(repoDir); err != nil {
		t.Fatalf("the clone reported success but %s does not exist: %v", repoDir, err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "E2E.md"), []byte("e2e review workflow\n"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	mustCall("git-add", map[string]any{"sandbox": "pod-a", "path": "repo", "paths": []any{"E2E.md"}})
	// The staged file must really be staged, or the commit below would be empty.
	staged := mustCall("git-status", map[string]any{"sandbox": "pod-a", "path": "repo"})
	if !strings.Contains(staged, "E2E.md") {
		t.Fatalf("E2E.md is not staged after git-add:\n%s", staged)
	}
	commitOut := mustCall("git-commit", map[string]any{
		"sandbox": "pod-a", "path": "repo", "message": "e2e: review workflow",
	})
	if !strings.Contains(commitOut, "Committed") {
		t.Fatalf("commit did not report a new commit: %s", commitOut)
	}

	// ---- 7. submit, read, comment, merge ----------------------------------
	submit := mustCall("git-submit-mr", map[string]any{
		"sandbox": "pod-a", "path": "repo",
		"title": "e2e: review workflow", "body": "opened by the e2e test",
	})
	idx := firstMRNumber(submit)
	if idx == 0 {
		t.Fatalf("could not read the MR number from %q", submit)
	}
	list := mustCall("git-mr-list", map[string]any{"remote": remote})
	if !strings.Contains(list, "#"+itoa(idx)) {
		t.Errorf("git-mr-list does not mention #%d:\n%s", idx, list)
	}
	mustCall("git-mr-show", map[string]any{"remote": remote, "index": float64(idx)})
	mustCall("git-mr-comment", map[string]any{
		"remote": remote, "index": float64(idx), "body": "review note from the e2e test",
	})
	shown := mustCall("git-mr-show", map[string]any{"remote": remote, "index": float64(idx)})
	if !strings.Contains(shown, "review note from the e2e test") {
		t.Errorf("the comment is not in the MR thread:\n%s", shown)
	}
	merged := mustCall("git-mr-merge", map[string]any{
		"remote": remote, "index": float64(idx), "style": "merge",
	})
	if strings.Contains(merged, "state=open") {
		t.Errorf("the MR is STILL OPEN after merge: %s", merged)
	}
}

// ---- helpers ---------------------------------------------------------------

// grantsForAll issues every permission this extension declares, scoped to the repositories the
// e2e uses.
//
// The writing tools are permission-gated since v0.9.x, so a call with no grants exercises the
// REFUSAL path. That path has its own unit tests; this e2e exists to prove the review workflow
// end to end, which needs the authorisations an operator would issue.
func grantsForAll() []*easybus.Grant {
	repos := []string{"agentdev/mrflow", "agentdev/mrflow-empty", "*"}
	// Only the keys this extension actually declares for the tools the e2e drives. Issuing
	// permissions nothing requires would make the test pass against a manifest that no longer
	// gates what it claims to.
	keys := []string{"mr.merge", "mr.close"}
	out := make([]*easybus.Grant, 0, len(keys)*len(repos))
	for _, k := range keys {
		for _, r := range repos {
			out = append(out, &easybus.Grant{
				Key:    k,
				Params: easybus.Struct(map[string]any{"repo": r}),
			})
		}
	}
	return out
}

func clip(s string) string {
	s = strings.ReplaceAll(s, "\n", " | ")
	if len(s) > 200 {
		return s[:200] + "…"
	}
	return s
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func containsStr(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

// firstMRNumber reads the N out of "MR #N opened: ...".
func firstMRNumber(s string) int {
	i := strings.Index(s, "MR #")
	if i < 0 {
		return 0
	}
	n := 0
	for _, r := range s[i+4:] {
		if r < '0' || r > '9' {
			break
		}
		n = n*10 + int(r-'0')
	}
	return n
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
