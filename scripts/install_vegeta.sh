#!/bin/bash
# Install Vegeta on Debian-based systems
# Based on: https://lindevs.com/install-vegeta-on-ubuntu

set -e  # Exit on error

echo "Installing Vegeta HTTP load testing tool..."

# Check if vegeta is already installed
if command -v vegeta &> /dev/null; then
    echo "Vegeta is already installed:"
    vegeta --version
    read -p "Do you want to reinstall? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Installation cancelled."
        exit 0
    fi
fi

# Detect architecture
ARCH=$(uname -m)
case $ARCH in
    x86_64)
        ARCH="amd64"
        ;;
    aarch64|arm64)
        ARCH="arm64"
        ;;
    *)
        echo "Unsupported architecture: $ARCH"
        echo "Please install Vegeta manually from: https://github.com/tsenart/vegeta/releases"
        exit 1
        ;;
esac

echo "Detected architecture: $ARCH"

# Check for required commands
for cmd in curl tar; do
    if ! command -v $cmd &> /dev/null; then
        echo "Error: $cmd is required but not installed."
        echo "Please install it first: sudo apt-get install $cmd"
        exit 1
    fi
done

# Get the latest version tag
echo "Fetching latest Vegeta version..."
VEGETA_VERSION=$(curl -s "https://api.github.com/repos/tsenart/vegeta/releases/latest" | grep -Po '"tag_name": "v\K[0-9.]+' || echo "")

if [ -z "$VEGETA_VERSION" ]; then
    echo "Error: Could not fetch latest version from GitHub API."
    echo "Please check your internet connection or install manually."
    exit 1
fi

echo "Latest version: v$VEGETA_VERSION"

# Create temporary directory
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Download Vegeta
DOWNLOAD_URL="https://github.com/tsenart/vegeta/releases/latest/download/vegeta_${VEGETA_VERSION}_linux_${ARCH}.tar.gz"
echo "Downloading Vegeta from: $DOWNLOAD_URL"

cd "$TEMP_DIR"
if ! curl -Lo vegeta.tar.gz "$DOWNLOAD_URL"; then
    echo "Error: Failed to download Vegeta."
    exit 1
fi

# Extract the archive
echo "Extracting Vegeta..."
if ! tar xf vegeta.tar.gz; then
    echo "Error: Failed to extract Vegeta archive."
    exit 1
fi

# Check if vegeta binary exists
if [ ! -f "vegeta" ]; then
    echo "Error: vegeta binary not found in archive."
    exit 1
fi

# Make it executable
chmod +x vegeta

# Move to /usr/local/bin (requires sudo)
echo "Installing Vegeta to /usr/local/bin (requires sudo privileges)..."
if ! sudo mv vegeta /usr/local/bin/; then
    echo "Error: Failed to move vegeta to /usr/local/bin."
    echo "You may need to run this script with sudo or ensure you have write permissions."
    exit 1
fi

# Clean up
cd - > /dev/null
rm -rf "$TEMP_DIR"

# Verify installation
echo ""
echo "Verifying installation..."
if command -v vegeta &> /dev/null; then
    vegeta --version
    echo ""
    echo "✓ Vegeta installed successfully!"
    echo ""
    echo "Test it with:"
    echo "  echo 'GET http://localhost:8181/v1/is_authorized' | vegeta attack -rate=10 -duration=1s | vegeta report"
else
    echo "Error: Installation completed but vegeta command not found in PATH."
    echo "Please ensure /usr/local/bin is in your PATH."
    exit 1
fi

