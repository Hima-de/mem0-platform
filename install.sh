#!/bin/bash
#
# Mem0 Universal Installer
# =======================
#
# One-command installation to turn ANY computer into a Mem0 sandbox provider.
#
# Usage:
#   curl -sSL https://install.mem0.ai | bash
#   curl -sSL https://install.mem0.ai | bash -s -- --server coordinator.mem0.ai
#   ./install.sh --server coordinator.mem0.ai --region us-east-1
#

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
MEM0_VERSION="1.0.0"
MEM0_REPO="Hima-de/mem0-platform"
INSTALL_DIR="${HOME}/.mem0"
LOG_FILE="${INSTALL_DIR}/install.log"

# Parse arguments
SERVER_URL="http://localhost:8000"
REGION="local"
AUTO_START=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --server)
            SERVER_URL="$2"
            shift 2
            ;;
        --region)
            REGION="$2"
            shift 2
            ;;
        --auto-start)
            AUTO_START=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --server URL    Coordinator server URL (default: http://localhost:8000)"
            echo "  --region REGION Region label (default: local)"
            echo "  --auto-start    Start agent automatically after installation"
            echo "  --help          Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                                      # Install with defaults"
            echo "  $0 --server https://coordinator.mem0.ai  # Register with coordinator"
            echo "  $0 --region us-east-1 --auto-start      # Auto-start in us-east-1"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Functions
log() {
    local level=$1
    shift
    local message="[$level] $*"
    echo -e "$message"
    echo "$(date '+%Y-%m-%d %H:%M:%S') $message" >> "${LOG_FILE}" 2>/dev/null || true
}

info() {
    log "INFO" "${BLUE}$*${NC}"
}

success() {
    log "SUCCESS" "${GREEN}$*${NC}"
}

warn() {
    log "WARNING" "${YELLOW}$*${NC}"
}

error() {
    log "ERROR" "${RED}$*${NC}"
    exit 1
}

header() {
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  Mem0 - Universal Sandbox Provider${NC}"
    echo -e "${CYAN}  Version ${MEM0_VERSION}${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
}

check_requirements() {
    info "Checking requirements..."

    # Check Python
    if ! command -v python3 &> /dev/null; then
        error "Python 3 is required but not installed. Please install Python 3.10+"
    fi

    PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
    if [[ "$(printf '%s\n' "3.10" "$PYTHON_VERSION" | sort -V | head -n1)" != "3.10" ]]; then
        warn "Python 3.10+ recommended. Found: $PYTHON_VERSION"
    fi

    # Check platform
    OS=$(uname -s)
    info "Detected platform: $OS"

    # Check available memory
    TOTAL_MEM=$(free -m 2>/dev/null | awk '/^Mem:/{print $2}' || sysctl -n hw.memsize 2>/dev/null | awk '{print $1/1024/1024}')
    if [[ -n "$TOTAL_MEM" && "$TOTAL_MEM" -lt 1 ]]; then
        warn "Less than 1GB RAM available. Mem0 may have limited functionality."
    fi

    success "Requirements check passed"
}

install_dependencies() {
    info "Installing dependencies..."

    # Upgrade pip
    python3 -m pip install --upgrade pip -q 2>/dev/null || true

    # Install required packages
    DEPS="aiohttp psutil"
    python3 -m pip install $DEPS -q 2>/dev/null || {
        warn "Failed to install some dependencies, continuing..."
    }

    # Install Mem0 Platform
    info "Installing Mem0 Platform..."
    pip install git+https://github.com/${MEM0_REPO}.git -q 2>/dev/null || {
        error "Failed to install Mem0 Platform"
    }

    success "Dependencies installed"
}

configure_agent() {
    info "Configuring Mem0 Agent..."

    # Create configuration directory
    mkdir -p "${INSTALL_DIR}"

    # Create config file
    cat > "${INSTALL_DIR}/config.json" << EOF
{
    "node_id": null,
    "coordinator_url": "${SERVER_URL}",
    "region": "${REGION}",
    "environment": "production",
    "tags": ["mem0-agent"],
    "labels": {
        "region": "${REGION}",
        "installed_at": "$(date -Iseconds)"
    }
}
EOF

    # Create systemd service (Linux)
    if [[ "$OS" == "Linux" ]]; then
        mkdir -p "${HOME}/.config/systemd/user"
        cat > "${HOME}/.config/systemd/user/mem0-agent.service" << EOF
[Unit]
Description=Mem0 Universal Agent
After=network.target

[Service]
Type=simple
ExecStart=%h/.local/bin/mem0 agent run --coordinator ${SERVER_URL}
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
EOF

        info "Created systemd service at ${HOME}/.config/systemd/user/mem0-agent.service"
    fi

    # Create launchd plist (macOS)
    if [[ "$OS" == "Darwin" ]]; then
        mkdir -p "${HOME}/Library/LaunchAgents"
        cat > "${HOME}/Library/LaunchAgents/ai.mem0.agent.plist" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>ai.mem0.agent</string>
    <key>ProgramArguments</key>
    <array>
        <string>python3</string>
        <string>-m</string>
        <string>mem0_agent.agent</string>
        <string>run</string>
        <string>--coordinator</string>
        <string>${SERVER_URL}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>${INSTALL_DIR}/agent.log</string>
    <key>StandardErrorPath</key>
    <string>${INSTALL_DIR}/agent.error.log</string>
</dict>
</plist>
EOF

        info "Created launchd plist at ${HOME}/Library/LaunchAgents/ai.mem0.agent.plist"
    fi

    success "Configuration complete"
}

show_summary() {
    echo ""
    echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  Installation Complete!${NC}"
    echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "Configuration:"
    echo -e "  ${CYAN}Server:${NC}      ${SERVER_URL}"
    echo -e "  ${CYAN}Region:${NC}      ${REGION}"
    echo -e "  ${CYAN}Config:${NC}      ${INSTALL_DIR}/config.json"
    echo ""
    echo "Quick Commands:"
    echo -e "  ${YELLOW}mem0 agent run${NC}              # Start the agent"
    echo -e "  ${YELLOW}mem0 agent status${NC}           # Check status"
    echo -e "  ${YELLOW}mem0 sandbox create${NC}         # Create a sandbox"
    echo -e "  ${YELLOW}mem0 sandbox list${NC}            # List sandboxes"
    echo ""
    echo "Next Steps:"
    echo "  1. Start the agent:"
    if [[ "$AUTO_START" == "true" ]]; then
        echo -e "     ${GREEN}Agent will start automatically${NC}"
        if [[ "$OS" == "Linux" ]]; then
            echo "     Or manually: systemctl --user start mem0-agent"
        elif [[ "$OS" == "Darwin" ]]; then
            echo "     Or manually: launchctl load ${HOME}/Library/LaunchAgents/ai.mem0.agent.plist"
        fi
    else
        echo -e "     ${CYAN}mem0 agent run${NC}"
    fi
    echo ""
    echo "  2. Register with coordinator:"
    echo -e "     ${CYAN}mem0 agent register --server ${SERVER_URL}${NC}"
    echo ""
    echo "  3. Create your first sandbox:"
    echo -e "     ${CYAN}mem0 sandbox create --runtime python${NC}"
    echo ""
    echo -e "${BLUE}Documentation:${NC} https://github.com/${MEM0_REPO}"
    echo -e "${BLUE}Issues:${NC}       https://github.com/${MEM0_REPO}/issues"
    echo ""
}

# Main execution
header
check_requirements
install_dependencies
configure_agent

# Auto-start if requested
if [[ "$AUTO_START" == "true" ]]; then
    info "Starting Mem0 Agent..."
    mem0 agent run --coordinator "${SERVER_URL}" &
    sleep 2
    mem0 agent status
fi

show_summary
