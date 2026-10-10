#!/usr/bin/env bash
# Checks what AQUIFER needs on macOS or Linux and, with --install, installs what is missing.
# Never installs silently: every package step is shown and confirmed (skip the prompts with --yes).
# Compatible with the bash 3.2 that ships with macOS.
set -u

INSTALL=0
ASSUME_YES=0
for argument in "$@"; do
  case "$argument" in
    --install|-i) INSTALL=1 ;;
    --yes|-y) ASSUME_YES=1 ;;
    --help|-h)
      printf 'Usage: ./setup.sh [--install] [--yes]\n  (no flags)  check prerequisites and print what to do\n  --install   install missing prerequisites (asks first)\n  --yes       do not ask for confirmation\n'
      exit 0 ;;
    *) printf 'Unknown option: %s\n' "$argument" >&2; exit 2 ;;
  esac
done

cd "$(dirname "$0")" || exit 1

if [ -t 1 ]; then GREEN=$'\033[32m'; YELLOW=$'\033[33m'; RED=$'\033[31m'; BOLD=$'\033[1m'; RESET=$'\033[0m'; else GREEN=''; YELLOW=''; RED=''; BOLD=''; RESET=''; fi
ok()   { printf '  %s[ ok ]%s %s\n' "$GREEN" "$RESET" "$1"; }
warn() { printf '  %s[warn]%s %s\n' "$YELLOW" "$RESET" "$1"; }
bad()  { printf '  %s[FAIL]%s %s\n' "$RED" "$RESET" "$1"; PROBLEMS=$((PROBLEMS + 1)); }
have() { command -v "$1" >/dev/null 2>&1; }
PROBLEMS=0
OS="$(uname -s)"
DISTRO=""
if [ "$OS" = "Linux" ] && [ -r /etc/os-release ]; then
  # shellcheck disable=SC1091
  DISTRO="$(. /etc/os-release; printf '%s %s' "${ID:-}" "${ID_LIKE:-}")"
fi
case "$DISTRO" in *debian*|*ubuntu*) DEBIAN_LIKE=1 ;; *) DEBIAN_LIKE=0 ;; esac

confirm() {
  [ "$ASSUME_YES" -eq 1 ] && return 0
  printf '  %s [y/N] ' "$1"
  read -r answer
  case "$answer" in y|Y|yes|YES) return 0 ;; *) return 1 ;; esac
}

run_step() {
  printf '  $ %s\n' "$*"
  "$@"
}

printf '%sAQUIFER setup check%s (%s)\n' "$BOLD" "$RESET" "$OS"

# --- Python -------------------------------------------------------------------------------
printf '\n%sPython%s\n' "$BOLD" "$RESET"
PYTHON=""
for candidate in python3 python; do
  if have "$candidate" && "$candidate" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)' >/dev/null 2>&1; then PYTHON="$candidate"; break; fi
done
if [ -n "$PYTHON" ]; then
  ok "$($PYTHON --version 2>&1)"
  if ! "$PYTHON" -c 'import venv, ensurepip' >/dev/null 2>&1; then
    bad "Python venv support is missing (the launcher needs it once to install PyYAML)"
    [ "$DEBIAN_LIKE" -eq 1 ] && printf '         Fix: sudo apt install python3-venv\n'
    NEED_VENV=1
  fi
else
  bad "Python 3.8 or newer not found"
  NEED_PYTHON=1
fi

# --- Docker -------------------------------------------------------------------------------
printf '\n%sDocker%s\n' "$BOLD" "$RESET"
if have docker; then
  ok "docker CLI: $(docker --version 2>&1)"
  COMPOSE_VERSION="$(docker compose version --short 2>/dev/null || true)"
  if [ -n "$COMPOSE_VERSION" ]; then
    ok "docker compose $COMPOSE_VERSION"
    MAJOR="${COMPOSE_VERSION#v}"; MINOR="${MAJOR#*.}"; MAJOR="${MAJOR%%.*}"; MINOR="${MINOR%%.*}"
    if [ "$MAJOR" -lt 2 ] 2>/dev/null || { [ "$MAJOR" -eq 2 ] && [ "$MINOR" -lt 20 ]; } 2>/dev/null; then
      bad "Compose 2.20 or newer is required"
      NEED_COMPOSE=1
    fi
  else
    bad "Docker Compose v2 plugin not found"
    NEED_COMPOSE=1
  fi
  if docker info >/dev/null 2>&1; then
    MEMORY_GB="$(docker info --format '{{.MemTotal}}' 2>/dev/null | awk '{ printf "%.1f", $1 / 1073741824 }')"
    ok "Docker daemon is reachable ($(docker info --format '{{.OperatingSystem}}, {{.NCPU}} CPUs' 2>/dev/null), ${MEMORY_GB:-?} GB memory)"
    if awk -v m="${MEMORY_GB:-0}" 'BEGIN { exit !(m < 7) }'; then
      warn "Docker has under 7 GB of memory; raise it in Docker Desktop > Settings > Resources"
    fi
  else
    bad "Docker daemon is not reachable"
    if [ "$OS" = "Darwin" ]; then
      printf '         Start Docker Desktop, then run this script again.\n'
    else
      printf '         Start it:  sudo systemctl enable --now docker\n'
      printf '         Allow your user: sudo usermod -aG docker "$USER"   (then log out and in again)\n'
    fi
  fi
else
  bad "Docker is not installed"
  NEED_DOCKER=1
fi

# --- Optional tools -----------------------------------------------------------------------
printf '\n%sOptional%s\n' "$BOLD" "$RESET"
if have git; then ok "git $(git --version | awk '{print $3}')"; else warn "git not found (only needed to clone and contribute)"; fi

# --- Installation -------------------------------------------------------------------------
if [ "$PROBLEMS" -gt 0 ] && [ "${NEED_DOCKER:-0}${NEED_PYTHON:-0}${NEED_COMPOSE:-0}${NEED_VENV:-0}" != "0000" ]; then
  printf '\n%sMissing prerequisites%s\n' "$BOLD" "$RESET"
  if [ "$OS" = "Darwin" ]; then
    printf '  Install with Homebrew:\n    brew install --cask docker        # Docker Desktop (or: brew install colima docker docker-compose)\n    brew install python\n'
    if [ "$INSTALL" -eq 1 ]; then
      if have brew; then
        [ "${NEED_DOCKER:-0}" = 1 ] && confirm "Install Docker Desktop with brew?" && run_step brew install --cask docker
        [ "${NEED_PYTHON:-0}" = 1 ] && confirm "Install Python with brew?" && run_step brew install python
        printf '  Open Docker Desktop once, accept its prompts, then run ./setup.sh again.\n'
      else
        warn "Homebrew not found: install it from https://brew.sh or download Docker Desktop from https://www.docker.com/products/docker-desktop/"
      fi
    fi
  elif [ "$DEBIAN_LIKE" -eq 1 ]; then
    printf '  Install on Ubuntu/Debian:\n    sudo apt-get update\n    sudo apt-get install -y docker.io docker-compose-v2 python3 python3-venv\n    sudo systemctl enable --now docker\n    sudo usermod -aG docker "$USER"   # then log out and back in\n'
    printf '  If docker-compose-v2 is not offered for your release, use Docker'"'"'s official repository:\n    https://docs.docker.com/engine/install/ubuntu/\n'
    if [ "$INSTALL" -eq 1 ]; then
      if confirm "Run the apt commands above (uses sudo)?"; then
        run_step sudo apt-get update && run_step sudo apt-get install -y docker.io docker-compose-v2 python3 python3-venv &&
          run_step sudo systemctl enable --now docker && run_step sudo usermod -aG docker "$USER"
        printf '  Log out and back in so the docker group applies, then run ./setup.sh again.\n'
      fi
    fi
  else
    printf '  Install Docker Engine with the Compose plugin and Python 3.8+ for your distribution:\n    https://docs.docker.com/engine/install/\n'
  fi
  printf '\n  Details for every platform: docs/INSTALL.md\n'
  exit 1
fi

if [ "$PROBLEMS" -gt 0 ]; then
  printf '\nFix the items above, then run ./setup.sh again. See docs/INSTALL.md.\n'
  exit 1
fi

# --- Project setup ------------------------------------------------------------------------
printf '\n%sProject%s\n' "$BOLD" "$RESET"
chmod +x lakehouse lakehouse.py up.sh 2>/dev/null || true
./lakehouse init || exit 1
printf '\n%sAll set.%s Next:\n  ./lakehouse quickstart      # start everything, load the demo, open the home page\n  ./lakehouse doctor          # re-check ports and memory any time\n' "$BOLD" "$RESET"
