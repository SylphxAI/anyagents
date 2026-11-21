# Read-only; emit a receipt only after every check succeeds. Git's stderr can
# contain credentials and is never forwarded to the caller.
set -eu
export LC_ALL=C GIT_OPTIONAL_LOCKS=0 GIT_TERMINAL_PROMPT=0 GIT_NO_REPLACE_OBJECTS=1
unset GIT_DIR GIT_WORK_TREE GIT_COMMON_DIR GIT_INDEX_FILE GIT_OBJECT_DIRECTORY \
    GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_NAMESPACE GIT_CONFIG GIT_CONFIG_COUNT GIT_CONFIG_PARAMETERS
workdir=$(pwd -P)
[ "$workdir" = "$1" ]
# Refuse bare/linked/nested repositories and interrupted operations. A failed
# traversal is not evidence that there is no repository.
[ ! -L .git ]
markers=$(find . -name .git -prune -print -o -name HEAD -print)
if [ -z "$markers" ]; then
    [ "$(git rev-parse --show-toplevel 2>/dev/null || :)" = '' ]
    # Require Git to be present even when there is no checkout.
    git --version >/dev/null
    printf '%s\n' 'no repository'
    exit 0
fi
[ "$markers" = './.git' ] && [ -d .git ]
[ "$(git rev-parse --show-toplevel)" = "$workdir" ]
[ "$(git rev-parse --git-common-dir)" = .git ]
[ "$(git rev-parse --is-shallow-repository)" = false ]
for artifact in MERGE_HEAD CHERRY_PICK_HEAD REVERT_HEAD BISECT_LOG rebase-apply rebase-merge \
    sequencer index.lock refs/stash info/grafts objects/info/alternates; do
    [ ! -e ".git/$artifact" ] && [ ! -L ".git/$artifact" ]
done
index=$(git ls-files -v)
if printf '%s\n' "$index" | grep -q '^[a-zS] '; then exit 1; fi
[ -z "$(git ls-files --stage | awk '$1 == "160000" { print }')" ]
status=$(git -c core.fsmonitor=false -c core.untrackedCache=false status --porcelain=v1 \
    --untracked-files=all --ignored --ignore-submodules=none)
[ -z "$status" ]
# Only live network advertisements count, never cached origin/* refs or a
# remote inside the sandbox. --get-url expands Git's insteadOf rewriting.
origin=$(git ls-remote --get-url origin)
case "$origin" in
    https://?*|http://?*|ssh://?*|git://?*|?*@?*:?*) ;;
    *) exit 1 ;;
esac
published=$(git -c protocol.allow=never -c protocol.https.allow=always \
    -c protocol.http.allow=always -c protocol.ssh.allow=always -c protocol.git.allow=always \
    ls-remote --heads --tags origin)
set --
while read -r oid ref; do
    case "$oid" in ''|*[!0-9a-f]*) continue ;; esac
    case "$ref" in refs/heads/*|refs/tags/*) ;; *) continue ;; esac
    if git cat-file -e "$oid" 2>/dev/null; then set -- "$@" "^$oid"; fi
done <<EOF_REFS
$published
EOF_REFS
[ "$#" -gt 0 ]
unpublished=$(git rev-list --objects HEAD --all --reflog "$@" --)
[ -z "$unpublished" ]
printf '%s\n' 'clean and pushed'
