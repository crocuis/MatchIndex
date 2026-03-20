# CLAUDE.md — CLI 도구 사용 규칙

이 문서는 Claude Code(AI 에이전트)가 터미널 작업 시 따라야 할 도구 사용 규칙을 정의한다.

## 파일 시스템 탐색

| 기존 명령어 | 대체 도구 | 비고 |
|---|---|---|
| `find` | `fd` | 직관적 구문, `.gitignore` 자동 존중, 정규식 기본 |
| `grep` | `rg` (ripgrep) | 멀티스레드, `.gitignore` 자동 존중, `--json` 출력 지원 |
| `cat` | `bat` | 구문 강조, 라인 번호, Git 변경 표시 |
| `ls` | `eza` | 트리 뷰(`--tree`), Git 상태 통합, 아이콘 지원 |

### 사용 예시

```bash
# 파일 탐색 — find 대신 fd
fd -e ts -e tsx                      # .ts, .tsx 파일 찾기
fd 'component' src/                  # src/ 내 'component' 포함 파일명 검색
fd -H -I .env                       # 숨김/무시 파일 포함 검색

# 텍스트 검색 — grep 대신 rg
rg 'getClubByIdDb' --type ts         # TypeScript 파일에서 검색
rg 'TODO|FIXME' -g '!node_modules'   # 특정 디렉토리 제외
rg -l 'interface.*Props'             # 매칭 파일명만 출력

# 파일 읽기 — cat 대신 bat
bat src/app/layout.tsx               # 구문 강조 + 줄 번호
bat -r 10:30 src/data/types.ts       # 특정 범위만 출력
bat -l tsx --plain                   # 장식 없이 순수 구문 강조만

# 디렉토리 목록 — ls 대신 eza
eza -la --git                        # 상세 + Git 상태
eza --tree --level=2 src/            # 트리 뷰 (2단계)
eza -l --sort=modified               # 수정일 기준 정렬
```

## 코드 구조 검색 및 리팩토링

**`ast-grep`을 적극 활용한다.** 정규식이 아닌 AST(추상 구문 트리) 기반으로 코드 패턴을 정확히 매칭한다.

```bash
# 패턴 검색
ast-grep -p 'export function $NAME($$$) { $$$ }' -l ts   # 모든 export 함수 찾기
ast-grep -p 'console.log($MSG)' --json                    # JSON 출력

# 리팩토링 (--update-all로 일괄 적용)
ast-grep -p 'console.log($MSG)' -r 'logger.info($MSG)' --update-all
```

## 데이터 처리 파이프라인

API 응답이나 JSON/YAML 데이터를 파싱할 때는 `jq`와 `yq`를 파이프라인으로 연결하여 사용한다.

```bash
# JSON 파싱
curl -s https://api.example.com/data | jq '.results[] | {id, name}'
cat package.json | jq '.dependencies | keys'

# YAML 파싱/변환
yq '.services' docker-compose.yml
yq -o=json config.yaml | jq '.database'    # YAML → JSON 변환 후 jq 처리
```

## GitHub 작업

GitHub 관련 작업은 반드시 `gh` CLI를 **비대화형 모드**(`--json` 등)로 사용한다.

```bash
# PR 작업
gh pr list --json number,title,state
gh pr create --title "제목" --body "본문" --base main
gh pr view 42 --json state,reviews,checks

# 이슈 작업
gh issue list --json number,title,labels
gh issue create --title "제목" --body "본문"

# API 직접 호출
gh api repos/{owner}/{repo}/pulls --jq '.[].title'
```

## Python 린팅 및 포매팅

Python 코드의 린팅 및 포매팅은 `ruff`를 사용한다.

```bash
ruff check .                  # 린트 검사
ruff check --fix .            # 자동 수정
ruff format .                 # 코드 포매팅
```

## 비대화형 모드 강제

외부 서비스와 상호작용하는 모든 CLI 명령어는 다음 원칙을 따른다:

1. **비대화형 플래그 강제**: `--yes`, `--quiet`, `--no-input`, `--non-interactive`
2. **JSON 출력 포맷 우선**: `--format json`, `--json`, `--output json`
3. **파이프 안전**: stdout/stderr 분리, 에러 시 종료 코드 확인

```bash
# 올바른 예시
npm install --save-exact --no-fund --no-audit
gh pr create --json url --title "..." --body "..."
apt-get install -y --quiet package-name

# 잘못된 예시 (대화형 프롬프트 유발)
npm init            # → npm init -y
gh auth login       # → 비대화형 불가 시 사전 안내
```

## 추가 도구 참조

| 도구 | 용도 | 주요 플래그 |
|---|---|---|
| `fzf` | 퍼지 파인더 | `--preview`, `--bind`, `--multi` |
| `zoxide` | 스마트 cd | `z <keyword>` |
| `atuin` | 셸 히스토리 | `atuin search <query>` |
| `delta` | Git diff 뷰어 | Git 설정으로 자동 적용 |
| `difft` | 구조적 diff | `difft file1 file2`, `GIT_EXTERNAL_DIFF=difft` |
| `lazygit` | Git TUI | 대화형 — 인간 전용 |
| `yazi` | 파일 매니저 TUI | 대화형 — 인간 전용 |
| `starship` | 셸 프롬프트 | 자동 적용 |
| `shellcheck` | 셸 스크립트 린트 | `shellcheck script.sh` |
| `shfmt` | 셸 스크립트 포맷 | `shfmt -w script.sh` |
| `httpie` | HTTP 클라이언트 | `http GET url`, `--print=b` |
