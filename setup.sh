#!/usr/bin/env bash
# One-shot dependency installer for a fresh machine (bare VM, container, etc.).
#
# Installs everything the rest of the toolchain assumes is already there:
# git, a JDK 21 (with javac, not just a JRE), Maven, and Python 3 (needed by
# ./run, the wrapper around Maven used for every other task in this repo).
#
# Deliberately plain bash with no Python dependency of its own, since Python
# missing is one of the things this script needs to be able to fix.
#
# Usage:
#   ./setup.sh          # install anything missing (prompts for sudo as needed)
#   ./setup.sh -y        # same, but never prompts (for CI / unattended installs)

set -euo pipefail

ASSUME_YES=false
for arg in "$@"; do
    case "$arg" in
        -y|--yes) ASSUME_YES=true ;;
        *) echo "Unknown option: $arg" >&2; exit 1 ;;
    esac
done

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIN_JAVA_VERSION=21

green() { printf '\033[32m%s\033[0m\n' "$1"; }
yellow() { printf '\033[33m%s\033[0m\n' "$1"; }
red() { printf '\033[31m%s\033[0m\n' "$1"; }

SUDO=""
if [ "$(id -u)" -ne 0 ]; then
    if command -v sudo >/dev/null 2>&1; then
        SUDO="sudo"
    else
        yellow "Warning: not root and no 'sudo' found; package installs below may fail."
    fi
fi

# Detect the system package manager so this works on Debian/Ubuntu, Fedora/RHEL,
# Arch, and macOS (Homebrew) without the caller having to know which.
PM=""
if command -v apt-get >/dev/null 2>&1; then PM="apt"
elif command -v dnf >/dev/null 2>&1; then PM="dnf"
elif command -v yum >/dev/null 2>&1; then PM="yum"
elif command -v pacman >/dev/null 2>&1; then PM="pacman"
elif command -v brew >/dev/null 2>&1; then PM="brew"
fi

if [ -z "$PM" ]; then
    red "Could not detect a supported package manager (apt/dnf/yum/pacman/brew)."
    red "Install manually: a JDK $MIN_JAVA_VERSION, Maven, git, and Python 3, then re-run ./run doctor."
    exit 1
fi
green "Detected package manager: $PM"

pm_install() {
    case "$PM" in
        apt)
            $SUDO apt-get update -y
            $SUDO apt-get install -y "$@"
            ;;
        dnf) $SUDO dnf install -y "$@" ;;
        yum) $SUDO yum install -y "$@" ;;
        pacman) $SUDO pacman -Sy --noconfirm "$@" ;;
        brew) brew install "$@" ;;
    esac
}

package_name() {
    # Maps a logical dependency to this package manager's actual package name.
    case "$1:$PM" in
        jdk:apt) echo "openjdk-${MIN_JAVA_VERSION}-jdk" ;;
        jdk:dnf|jdk:yum) echo "java-${MIN_JAVA_VERSION}-openjdk-devel" ;;
        jdk:pacman) echo "jdk${MIN_JAVA_VERSION}-openjdk" ;;
        jdk:brew) echo "openjdk@${MIN_JAVA_VERSION}" ;;
        maven:*) echo "maven" ;;
        git:*) echo "git" ;;
        python3:apt) echo "python3" ;;
        python3:dnf|python3:yum) echo "python3" ;;
        python3:pacman) echo "python" ;;
        python3:brew) echo "python3" ;;
    esac
}

confirm_install() {
    local label="$1"
    if [ "$ASSUME_YES" = true ]; then
        return 0
    fi
    read -r -p "Install $label now? [Y/n] " reply
    [[ -z "$reply" || "$reply" =~ ^[Yy] ]]
}

java_major_version() {
    if ! command -v java >/dev/null 2>&1; then
        echo 0
        return
    fi
    local ver
    ver="$(java -version 2>&1 | head -1)"
    # Handles both "1.8.0_x" (old scheme) and "21.0.x" (current scheme).
    if [[ "$ver" =~ \"1\.([0-9]+)\. ]]; then
        echo "${BASH_REMATCH[1]}"
    elif [[ "$ver" =~ \"([0-9]+)\. ]] || [[ "$ver" =~ \"([0-9]+)\" ]]; then
        echo "${BASH_REMATCH[1]}"
    else
        echo 0
    fi
}

ensure_dependency() {
    local name="$1" check_cmd="$2" label="$3"
    if eval "$check_cmd"; then
        green "$label already present"
        return
    fi
    yellow "$label not found."
    local pkg
    pkg="$(package_name "$name")"
    if [ -z "$pkg" ]; then
        red "Don't know the $PM package name for $label; install it manually."
        exit 1
    fi
    if confirm_install "$label ($pkg)"; then
        pm_install "$pkg"
    else
        red "$label is required; aborting."
        exit 1
    fi
}

ensure_dependency git 'command -v git >/dev/null 2>&1' "git"

if [ "$(java_major_version)" -ge "$MIN_JAVA_VERSION" ] && command -v javac >/dev/null 2>&1; then
    green "JDK $MIN_JAVA_VERSION+ with javac already present"
else
    yellow "No JDK $MIN_JAVA_VERSION (with javac) found."
    pkg="$(package_name jdk)"
    if confirm_install "OpenJDK $MIN_JAVA_VERSION ($pkg)"; then
        pm_install "$pkg"
    else
        red "A JDK is required; aborting."
        exit 1
    fi
fi

ensure_dependency maven 'command -v mvn >/dev/null 2>&1' "Maven"
ensure_dependency python3 'command -v python3 >/dev/null 2>&1' "Python 3"

echo
green "Dependencies installed. Running ./run doctor to finish Maven/JAVA_HOME setup..."
chmod +x "$DIR/run"
"$DIR/run" doctor

echo
green "Setup complete. Start the app with:  ./run dev"
