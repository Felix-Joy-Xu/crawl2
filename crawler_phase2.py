import os, json, time, requests, threading
from pathlib import Path
from datetime import datetime

# ============================================================
# 从环境变量加载 Tokens
# ============================================================
def load_tokens():
    raw = os.environ.get("GITHUB_TOKENS", "")
    if not raw:
        return []
    tokens = [t.strip() for t in raw.split(",") if t.strip()]
    return list(dict.fromkeys(tokens))

# ============================================================
# 配置
# ============================================================
API_BASE = "https://api.github.com"
PER_PAGE = 100

REPO_LIST = [
    "Aider-AI/aider", "continuedev/continue", "cline/cline",
    "All-Hands-AI/OpenHands", "openai/openai-python",
    "anthropics/anthropic-sdk-python", "ollama/ollama", "vercel/ai",
    "lobehub/lobe-chat", "microsoft/autogen", "crewAIInc/crewAI",
    "langchain-ai/langgraph", "browser-use/browser-use",
    "ChatGPTNextWeb/ChatGPT-Next-Web", "getzep/graphiti",
]
BACKFILL_REPOS = [
    "langchain-ai/langchain", "huggingface/transformers", "vercel/next.js",
    "godotengine/godot", "rust-lang/rust", "yt-dlp/yt-dlp",
    "2dust/v2rayN", "axios/axios", "vuejs/vue", "d3/d3",
]

# 云端运行: data_phase2/ 到仓库目录; 限制运行时间
START_TIME = time.time()
MAX_RUNTIME = 5.5 * 3600  # 5.5小时
OUTPUT_DIR = Path("data_phase2")
OUTPUT_DIR.mkdir(exist_ok=True)

# ============================================================
# Token 管理
# ============================================================
class TokenPool:
    def __init__(self, tokens):
        self.tokens = tokens
        self.lock = threading.Lock()
        self.idx = 0
        self.disabled = {}

    def get(self):
        with self.lock:
            now = time.time()
            for _ in range(len(self.tokens)):
                t = self.tokens[self.idx % len(self.tokens)]
                self.idx += 1
                if now >= self.disabled.get(t, 0):
                    return t
            wait = max(min(self.disabled.values()) - now, 10) if self.disabled else 10
            print(f"  ⏳ 所有token暂时禁用, 等待 {wait:.0f}s")
        time.sleep(wait)
        return self.get()

    def disable(self, token, seconds=3600):
        with self.lock:
            self.disabled[token] = time.time() + seconds

TOKENS = load_tokens()
POOL = TokenPool(TOKENS) if TOKENS else None

# ============================================================
# HTTP
# ============================================================
def api_get(url, params=None, headers=None):
    if not POOL:
        return None
    for _ in range(3):
        if time.time() - START_TIME > MAX_RUNTIME:
            raise TimeoutError("到达5.5h时限, 安全退出")

        token = POOL.get()
        h = {"Authorization": f"Bearer {token}",
             "Accept": "application/vnd.github+json",
             "X-GitHub-Api-Version": "2022-11-28"}
        if headers:
            h.update(headers)
        try:
            r = requests.get(url, headers=h, params=params, timeout=60)
            remaining = int(r.headers.get("X-RateLimit-Remaining", "5000"))
            if remaining < 50:
                POOL.disable(token, 3600)
            if r.status_code == 200:
                return r.json()
            elif r.status_code in [403, 429]:
                reset = int(r.headers.get("X-RateLimit-Reset", "0"))
                wait = max(reset - time.time(), 60) if reset else 3600
                POOL.disable(token, min(wait, 7200))
                time.sleep(2)
                continue
            elif r.status_code in [404, 409, 422]:
                return None
            time.sleep(3)
        except requests.exceptions.Timeout:
            time.sleep(5)
            continue
        except Exception:
            time.sleep(3)
            continue
    return None

def paginate(url, params=None):
    params = dict(params) if params else {}
    params["per_page"] = PER_PAGE
    params["page"] = 1
    while True:
        data = api_get(url, params)
        if data is None or not data:
            break
        for item in data:
            yield item
        if len(data) < PER_PAGE:
            break
        params["page"] += 1

# ============================================================
# 采集
# ============================================================
def repo_prefix(repo):
    return repo.replace("/", "_")

def crawl_issues(repo, outdir, since):
    fb = open(outdir / f"{repo_prefix(repo)}_issue_base.jsonl", "w", encoding="utf-8")
    fl = open(outdir / f"{repo_prefix(repo)}_issues.jsonl", "w", encoding="utf-8")
    cnt = 0
    for issue in paginate(f"{API_BASE}/repos/{repo}/issues",
                          params={"state": "all", "since": since, "sort": "created", "direction": "asc"}):
        if "pull_request" in issue:
            continue
        num = issue["number"]
        fl.write(json.dumps({"number": num, "title": issue.get("title", ""),
                             "createdAt": issue.get("created_at"), "state": issue.get("state"),
                             "repo": repo}, ensure_ascii=False) + "\n")
        fb.write(json.dumps({"repo": repo, "number": num, "title": issue.get("title", ""),
                             "body": issue.get("body"), "state": issue.get("state"),
                             "createdAt": issue.get("created_at"),
                             "author": issue.get("user", {}).get("login") if issue.get("user") else None},
                            ensure_ascii=False) + "\n")
        cnt += 1
    fb.close(); fl.close()
    return cnt

def crawl_issue_comments(repo, outdir):
    with open(outdir / f"{repo_prefix(repo)}_issue_comments.jsonl", "w", encoding="utf-8") as f:
        cnt = 0
        for c in paginate(f"{API_BASE}/repos/{repo}/issues/comments",
                          params={"sort": "created", "direction": "asc"}):
            issue_url = c.get("issue_url", "")
            issue_number = int(issue_url.rstrip("/").split("/")[-1]) if issue_url else None
            f.write(json.dumps({"body": c.get("body"), "createdAt": c.get("created_at"),
                                "author": c.get("user", {}).get("login") if c.get("user") else None,
                                "repo": repo, "issue_number": issue_number}, ensure_ascii=False) + "\n")
            cnt += 1
    return cnt

def crawl_prs(repo, outdir):
    with open(outdir / f"{repo_prefix(repo)}_pr_base.jsonl", "w", encoding="utf-8") as fb, \
         open(outdir / f"{repo_prefix(repo)}_pullRequests.jsonl", "w", encoding="utf-8") as fl:
        cnt = 0
        for pr in paginate(f"{API_BASE}/repos/{repo}/pulls",
                           params={"state": "all", "sort": "created", "direction": "asc"}):
            num = pr["number"]
            fl.write(json.dumps({"number": num, "title": pr.get("title", ""),
                                 "createdAt": pr.get("created_at"), "state": pr.get("state"),
                                 "repo": repo}, ensure_ascii=False) + "\n")
            fb.write(json.dumps({"repo": repo, "number": num, "title": pr.get("title", ""),
                                 "body": pr.get("body"), "state": pr.get("state"),
                                 "createdAt": pr.get("created_at"), "mergedAt": pr.get("merged_at"),
                                 "additions": pr.get("additions"), "deletions": pr.get("deletions"),
                                 "author": pr.get("user", {}).get("login") if pr.get("user") else None},
                                ensure_ascii=False) + "\n")
            cnt += 1
    return cnt

def crawl_pr_comments(repo, outdir):
    with open(outdir / f"{repo_prefix(repo)}_pr_comments.jsonl", "w", encoding="utf-8") as f:
        cnt = 0
        for c in paginate(f"{API_BASE}/repos/{repo}/pulls/comments",
                          params={"sort": "created", "direction": "asc"}):
            pr_url = c.get("pull_request_url", "")
            pr_number = int(pr_url.rstrip("/").split("/")[-1]) if pr_url else None
            f.write(json.dumps({"body": c.get("body"), "createdAt": c.get("created_at"),
                                "author": c.get("user", {}).get("login") if c.get("user") else None,
                                "repo": repo, "pr_number": pr_number}, ensure_ascii=False) + "\n")
            cnt += 1
    return cnt

def crawl_pr_reviews(repo, outdir):
    fname = outdir / f"{repo_prefix(repo)}_pr_reviews.jsonl"
    cnt = 0
    pr_nums = [i["number"] for i in paginate(f"{API_BASE}/repos/{repo}/issues",
                params={"state": "all", "sort": "created", "direction": "asc"}) if "pull_request" in i]
    with open(fname, "w", encoding="utf-8") as f:
        for num in pr_nums:
            for rv in paginate(f"{API_BASE}/repos/{repo}/pulls/{num}/reviews"):
                f.write(json.dumps({"repo": repo, "pr_number": num,
                                    "type": rv.get("type", "review"), "body": rv.get("body"),
                                    "state": rv.get("state"), "createdAt": rv.get("submitted_at"),
                                    "author": rv.get("user", {}).get("login") if rv.get("user") else None},
                                   ensure_ascii=False) + "\n")
                cnt += 1
    return cnt

def crawl_pr_timeline(repo, outdir):
    pr_nums = [p["number"] for p in paginate(f"{API_BASE}/repos/{repo}/pulls",
                params={"state": "all", "sort": "created", "direction": "asc"})]
    with open(outdir / f"{repo_prefix(repo)}_pr_timeline.jsonl", "w", encoding="utf-8") as f:
        cnt = 0
        for num in pr_nums:
            page = 1
            while True:
                data = api_get(f"{API_BASE}/repos/{repo}/issues/{num}/timeline",
                               params={"per_page": PER_PAGE, "page": page},
                               headers={"Accept": "application/vnd.github.mockingbird-preview+json"})
                if data is None or not data:
                    break
                for ev in data:
                    f.write(json.dumps({"__typename": ev.get("event", "Unknown"),
                                        "repo": repo, "pr_number": num}, ensure_ascii=False) + "\n")
                    cnt += 1
                if len(data) < PER_PAGE:
                    break
                page += 1
    return cnt

# ============================================================
# 主流程
# ============================================================
def crawl_repo(repo, outdir, since="2020-01-01T00:00:00Z"):
    prefix = repo_prefix(repo)
    state_path = outdir / f"{prefix}_state.json"

    if state_path.exists():
        state = json.load(open(state_path, "r", encoding="utf-8"))
        if state.get("done"):
            print(f"  [{repo}] 已完成, 跳过")
            return state
    else:
        state = {"repo": repo, "done": False, "steps": {}}

    print(f"\n  === {repo} ===")
    steps = [
        ("issues",         lambda: crawl_issues(repo, outdir, since)),
        ("issue_comments", lambda: crawl_issue_comments(repo, outdir)),
        ("pull_requests",  lambda: crawl_prs(repo, outdir)),
        ("pr_comments",    lambda: crawl_pr_comments(repo, outdir)),
        ("pr_reviews",     lambda: crawl_pr_reviews(repo, outdir)),
        ("pr_timeline",    lambda: crawl_pr_timeline(repo, outdir)),
    ]

    for name, fn in steps:
        if state["steps"].get(name, {}).get("done"):
            print(f"    [{name}] 已完成, 跳过")
            continue
        if time.time() - START_TIME > MAX_RUNTIME:
            print(f"  ⚠️ 已达5.5h时限, 保存状态并退出")
            json.dump(state, open(state_path, "w", encoding="utf-8"), indent=2, ensure_ascii=False)
            return state
        print(f"    [{name}] 采集中...", end=" ", flush=True)
        t0 = time.time()
        try:
            cnt = fn()
            elapsed = time.time() - t0
            print(f"{cnt:,}条 ({elapsed:.0f}s)")
            state["steps"][name] = {"done": True, "count": cnt}
        except Exception as e:
            print(f"异常: {e}")
            state["steps"][name] = {"done": False, "error": str(e)}
        json.dump(state, open(state_path, "w", encoding="utf-8"), indent=2, ensure_ascii=False)

    state["done"] = True
    json.dump(state, open(state_path, "w", encoding="utf-8"), indent=2, ensure_ascii=False)
    print(f"  [{repo}] 完成")
    return state

def main():
    print("=" * 50)
    print(f" GitHub Phase2 Crawler ({len(TOKENS)} tokens)")
    print(f" 开始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    # 第二批: 回溯现有仓库 (先做, 这些是核心研究基础)
    for repo in BACKFILL_REPOS:
        outdir = OUTPUT_DIR / "backfill" / repo_prefix(repo)
        outdir.mkdir(parents=True, exist_ok=True)
        crawl_repo(repo, outdir, since="2020-01-01T00:00:00Z")
        if time.time() - START_TIME > MAX_RUNTIME:
            break

    # 第一批: AI 原生新仓库
    for repo in REPO_LIST:
        outdir = OUTPUT_DIR / repo_prefix(repo)
        outdir.mkdir(parents=True, exist_ok=True)
        crawl_repo(repo, outdir, since="2020-01-01T00:00:00Z")
        if time.time() - START_TIME > MAX_RUNTIME:
            break

    print(f"\n  结束: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
