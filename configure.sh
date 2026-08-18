#!/usr/bin/env bash
#
# configure.sh — discover your Databricks Lakebase settings via the REST API (OAuth)
# and fill the <PLACEHOLDER>s in app.yaml. Optionally deploys the app at the end.
#
# It calls the Databricks REST API directly with an OAuth bearer token minted from a
# U2M (OAuth) CLI profile, so you can see exactly which endpoints yield each value:
#
#   GET  /api/2.0/preview/scim/v2/Me                              -> your username
#   GET  /api/2.0/postgres/projects                               -> Lakebase projects
#   GET  /api/2.0/postgres/projects/<p>/branches                  -> branches
#   GET  /api/2.0/postgres/projects/<p>/branches/<b>/endpoints    -> endpoints + host
#   GET  /api/2.0/apps/<app>                                      -> service principal + URL
#   PATCH /api/2.0/permissions/database-projects/<p>              -> grant the SP CAN_USE
#
# Usage:
#   ./configure.sh -p <OAUTH_PROFILE> [options]
#
# Options:
#   -p, --profile   Databricks CLI profile (must be OAuth/U2M — see note below)   [required]
#   -a, --app       App name                                        [default: lakebase-snapper]
#       --project   Lakebase project id (skip interactive pick)
#       --branch    Branch name                                     [default: production]
#       --endpoint  Endpoint name                                   [default: primary]
#       --repo-db   Repository database name                        [default: lakebase_snapper]
#       --target-db Database to monitor (SEED_TARGETS)              [default: postgres]
#       --deploy    Skip the prompt and deploy after configuring
#       --yes       Assume "yes" to prompts (non-interactive)
#   -h, --help
#
# Note: logs and OAuth token minting require a U2M profile. Create one with:
#   databricks auth login --host https://<workspace-host> --profile <name>
set -euo pipefail

# ------------------------------------------------------------------ args + deps
PROFILE="" ; APP="lakebase-snapper" ; PROJECT="" ; BRANCH="production" ; ENDPOINT_NAME="primary"
REPO_DB="lakebase_snapper" ; TARGET_DB="postgres" ; DO_DEPLOY=0 ; ASSUME_YES=0
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"   # repo root
APP_DIR="$HERE/app"                                     # the app lives in ./app

die(){ echo "error: $*" >&2; exit 1; }
have(){ command -v "$1" >/dev/null 2>&1; }

while [ $# -gt 0 ]; do case "$1" in
  -p|--profile) PROFILE="$2"; shift 2;;
  -a|--app) APP="$2"; shift 2;;
  --project) PROJECT="$2"; shift 2;;
  --branch) BRANCH="$2"; shift 2;;
  --endpoint) ENDPOINT_NAME="$2"; shift 2;;
  --repo-db) REPO_DB="$2"; shift 2;;
  --target-db) TARGET_DB="$2"; shift 2;;
  --deploy) DO_DEPLOY=1; shift;;
  --yes) ASSUME_YES=1; shift;;
  -h|--help) sed -n '2,40p' "$0"; exit 0;;
  *) die "unknown option: $1 (try --help)";;
esac; done

[ -n "$PROFILE" ] || die "missing -p/--profile. Try: $0 -p <oauth-profile>"
have databricks || die "the 'databricks' CLI is required"
have jq || die "'jq' is required"
have curl || die "'curl' is required"

# Detect a usable controlling terminal once (so prompts degrade quietly when there isn't one).
TTY_OK=0; { : </dev/tty; } 2>/dev/null && TTY_OK=1

ask(){ # ask "prompt" "default" -> echoes answer
  local ans; if [ "$ASSUME_YES" = 1 ] || [ "$TTY_OK" != 1 ]; then echo "$2"; return; fi
  read -r -p "$1 [$2]: " ans </dev/tty || true; echo "${ans:-$2}"
}
confirm(){ # confirm "prompt" -> 0 if yes
  [ "$ASSUME_YES" = 1 ] && return 0
  [ "$TTY_OK" = 1 ] || return 1
  local ans; read -r -p "$1 [y/N]: " ans </dev/tty || true; [[ "$ans" =~ ^[Yy] ]]
}

# ------------------------------------------------------------------ OAuth + REST
echo "→ minting OAuth token from profile '$PROFILE' ..."
HOST="$(databricks auth env --profile "$PROFILE" 2>/dev/null | jq -r '.env.DATABRICKS_HOST // empty')"
[ -n "$HOST" ] || die "could not resolve workspace host for profile '$PROFILE' (is it configured?)"
TOKEN="$(databricks auth token -p "$PROFILE" 2>/dev/null | jq -r '.access_token // empty')" || true
[ -n "$TOKEN" ] || die "could not mint an OAuth token from '$PROFILE'.
  This needs a U2M (OAuth) profile — PAT profiles won't work. Create one with:
  databricks auth login --host $HOST --profile ${PROFILE}"

api(){ # api METHOD PATH [curl-args...]
  local m="$1" p="$2"; shift 2
  curl -sS -X "$m" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" "$HOST$p" "$@"
}

WORKSPACE_HOST="${HOST#https://}"
WHOAMI="$(api GET /api/2.0/preview/scim/v2/Me | jq -r '.userName')"
echo "  workspace : $HOST"
echo "  user      : $WHOAMI"

# ------------------------------------------------------------------ pick project
if [ -z "$PROJECT" ]; then
  PROJECTS=(); while IFS= read -r line; do [ -n "$line" ] && PROJECTS+=("$line"); done \
    < <(api GET /api/2.0/postgres/projects | jq -r '.projects[].name | sub("^projects/";"")')
  [ "${#PROJECTS[@]}" -gt 0 ] || die "no Lakebase projects found in this workspace"
  if [ "${#PROJECTS[@]}" = 1 ]; then PROJECT="${PROJECTS[0]}"
  else
    echo "Select a Lakebase project:"; select p in "${PROJECTS[@]}"; do [ -n "$p" ] && PROJECT="$p" && break; done
  fi
fi
echo "  project   : $PROJECT"

# ------------------------------------------------------------------ pick endpoint
EP_JSON="$(api GET "/api/2.0/postgres/projects/$PROJECT/branches/$BRANCH/endpoints")"
ENDPOINT_PATH="$(echo "$EP_JSON" | jq -r --arg e "$ENDPOINT_NAME" '.endpoints[] | select(.name | endswith("/endpoints/"+$e)) | .name')"
if [ -z "$ENDPOINT_PATH" ]; then
  EPS=(); while IFS= read -r line; do [ -n "$line" ] && EPS+=("$line"); done \
    < <(echo "$EP_JSON" | jq -r '.endpoints[].name')
  [ "${#EPS[@]}" -gt 0 ] || die "no endpoints under projects/$PROJECT/branches/$BRANCH"
  echo "Select an endpoint:"; select e in "${EPS[@]}"; do [ -n "$e" ] && ENDPOINT_PATH="$e" && break; done
fi
PGHOST="$(echo "$EP_JSON" | jq -r --arg n "$ENDPOINT_PATH" '.endpoints[] | select(.name==$n) | .status.hosts.host')"
[ -n "$PGHOST" ] && [ "$PGHOST" != null ] || die "could not resolve host for $ENDPOINT_PATH"
echo "  endpoint  : $ENDPOINT_PATH"
echo "  host      : $PGHOST"

# ------------------------------------------------------------------ app service principal
SP="" ; APP_URL=""
APP_JSON="$(api GET "/api/2.0/apps/$APP" 2>/dev/null || true)"
if echo "$APP_JSON" | jq -e '.service_principal_client_id' >/dev/null 2>&1; then
  SP="$(echo "$APP_JSON" | jq -r '.service_principal_client_id')"
  APP_URL="$(echo "$APP_JSON" | jq -r '.url // empty')"
  echo "  app SP    : $SP"
  echo "  app URL   : ${APP_URL:-<not provisioned yet>}"
else
  echo "  app SP    : <app '$APP' not created yet — SP is assigned on creation>"
fi

# repository db + target db (prompt unless provided)
REPO_DB="$(ask 'Repository database name' "$REPO_DB")"
TARGET_DB="$(ask 'Database to monitor (SEED_TARGETS)' "$TARGET_DB")"
TARGET_LABEL="$PROJECT / $TARGET_DB"

# ------------------------------------------------------------------ write app.yaml
YAML="$APP_DIR/app.yaml"
[ -f "$YAML" ] || die "app.yaml not found in $APP_DIR (run this from the repo root)"
cp "$YAML" "$YAML.bak"
sed -i.tmp \
  -e "s|<LAKEBASE_ENDPOINT>|$ENDPOINT_PATH|g" \
  -e "s|<PGHOST>|$PGHOST|g" \
  -e "s|<MY-DB>|$TARGET_LABEL|g" \
  -e "s|<DATABASE>|$TARGET_DB|g" \
  -e "s|value: 'lakebase_snapper'|value: '$REPO_DB'|g" \
  "$YAML"
[ -n "$SP" ] && sed -i.tmp -e "s|<SERVICE-PRINCIPAL-CLIENT-ID>|$SP|g" "$YAML"
rm -f "$YAML.tmp"

echo
echo "✓ app.yaml updated (backup at app.yaml.bak):"
echo "    REPO_ENDPOINT = $ENDPOINT_PATH"
echo "    PGHOST        = $PGHOST"
echo "    REPO_DBNAME   = $REPO_DB"
echo "    SEED_TARGETS  = [{label:'$TARGET_LABEL', dbname:'$TARGET_DB'}]"
if [ -z "$SP" ]; then
  echo "    PGUSER        = <SERVICE-PRINCIPAL-CLIENT-ID>  (still a placeholder — set after the app exists;"
  echo "                     the deploy step below creates the app and fills this in automatically)"
else
  echo "    PGUSER        = $SP"
fi

# ------------------------------------------------------------------ optional deploy
echo
if [ "$DO_DEPLOY" != 1 ]; then
  if confirm "Deploy '$APP' to $WORKSPACE_HOST now (create app if needed, grant, set up the repo DB, sync & deploy)?"; then
    DO_DEPLOY=1
  fi
fi
[ "$DO_DEPLOY" = 1 ] || { echo "Done. Review app.yaml, then deploy with: databricks apps deploy $APP --source-code-path <ws-path> -p $PROFILE"; exit 0; }

have psql || die "deploy needs 'psql' (brew install postgresql@16) to set up the repository role"

# 1. create the app if it doesn't exist, then wait for its service principal
if [ -z "$SP" ]; then
  echo "→ creating app '$APP' ..."
  databricks apps create "$APP" --description "Lakebase performance sampler" -p "$PROFILE" >/dev/null 2>&1 || true
  for _ in $(seq 1 30); do
    SP="$(databricks apps get "$APP" -p "$PROFILE" -o json 2>/dev/null | jq -r '.service_principal_client_id // empty')"
    [ -n "$SP" ] && break; sleep 4
  done
  [ -n "$SP" ] || die "app created but no service principal appeared; re-run once it's ready"
  echo "  app SP    : $SP"
  sed -i.tmp -e "s|<SERVICE-PRINCIPAL-CLIENT-ID>|$SP|g" "$YAML" && rm -f "$YAML.tmp"
fi

# 2. grant the SP CAN_USE on the project (REST PATCH, additive)
echo "→ granting the service principal CAN_USE on project '$PROJECT' ..."
api PATCH "/api/2.0/permissions/database-projects/$PROJECT" \
  -d "{\"access_control_list\":[{\"service_principal_name\":\"$SP\",\"permission_level\":\"CAN_USE\"}]}" >/dev/null

# 3. repository DB + SP Postgres role + schema ownership (psql, using a minted DB token)
echo "→ setting up repository database '$REPO_DB' and the SP role ..."
DBTOKEN="$(databricks postgres generate-database-credential "$ENDPOINT_PATH" -p "$PROFILE" -o json | jq -r '.token')"
export PGPASSWORD="$DBTOKEN"
# CREATE DATABASE if absent (Postgres has no CREATE DATABASE IF NOT EXISTS):
if ! PGPASSWORD=$DBTOKEN psql "host=$PGHOST dbname=postgres user=$WHOAMI sslmode=require" -tAc \
      "SELECT 1 FROM pg_database WHERE datname='$REPO_DB'" | grep -q 1; then
  PGPASSWORD=$DBTOKEN psql "host=$PGHOST dbname=postgres user=$WHOAMI sslmode=require" -c "CREATE DATABASE $REPO_DB;"
fi
PGPASSWORD=$DBTOKEN psql "host=$PGHOST dbname=postgres user=$WHOAMI sslmode=require" -v ON_ERROR_STOP=1 <<SQL
CREATE EXTENSION IF NOT EXISTS databricks_auth;
SELECT CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname='$SP') THEN 'role exists'
            ELSE databricks_create_role('$SP','SERVICE_PRINCIPAL')::text END;
GRANT pg_monitor TO "$SP";
GRANT CONNECT ON DATABASE $REPO_DB TO "$SP";
SQL
PGPASSWORD=$DBTOKEN psql "host=$PGHOST dbname=$REPO_DB user=$WHOAMI sslmode=require" -v ON_ERROR_STOP=1 <<SQL
GRANT "$SP" TO "$WHOAMI";
CREATE SCHEMA IF NOT EXISTS snap AUTHORIZATION "$SP";
SQL

# 4. sync source + deploy
echo "→ syncing source and deploying ..."
WS="/Workspace/Users/$WHOAMI/$APP"
databricks sync "$APP_DIR" "$WS" --exclude .venv --exclude __pycache__ --exclude .git --exclude .databricks --full -p "$PROFILE"
databricks apps deploy "$APP" --source-code-path "$WS" -p "$PROFILE"

APP_URL="$(databricks apps get "$APP" -p "$PROFILE" -o json | jq -r '.url // empty')"
echo
echo "✓ Deployed. App URL: ${APP_URL:-<see: databricks apps get $APP>}"
echo "  Logs:  databricks apps logs $APP -p $PROFILE"
echo "  Diag:  open  ${APP_URL}/api/diag   (in a browser signed in to the workspace)"
