#!/bin/bash
# Build ~/Applications/cellMatcher.app (Spotlight: "cellMatcher").
# The app runs whatever is checked out in this repository with the given
# Python environment; rerun only if the repository or environment moves.
#   tools/install_cellmatcher_app.sh [/path/to/python]
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="${1:-$HOME/miniforge3/envs/manualIdentity/bin/python}"
APP="$HOME/Applications/cellMatcher.app"

if [[ ! -x "$PYTHON" ]]; then
  echo "Python not found: $PYTHON" >&2
  exit 1
fi
"$PYTHON" -c "import napari, ants" >/dev/null 2>&1 || {
  echo "$PYTHON lacks napari/ANTsPy; install with: $PYTHON -m pip install -e '$REPO[manual-identity]'" >&2
  exit 1
}

rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"
cp "$REPO/src/codeants_2pf_hcr/apps/resources/cellMatcher.icns" "$APP/Contents/Resources/cellMatcher.icns"

cat > "$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key><string>cellMatcher</string>
  <key>CFBundleDisplayName</key><string>cellMatcher</string>
  <key>CFBundleIdentifier</key><string>ch.unil.larschlab.cellmatcher</string>
  <key>CFBundleExecutable</key><string>cellMatcher</string>
  <key>CFBundleIconFile</key><string>cellMatcher</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleShortVersionString</key><string>1.0</string>
  <key>LSMinimumSystemVersion</key><string>12.0</string>
  <key>NSHighResolutionCapable</key><true/>
</dict>
</plist>
PLIST

cat > "$APP/Contents/MacOS/cellMatcher" <<LAUNCH
#!/bin/bash
LOGS="\$HOME/Library/Logs/cellMatcher"
mkdir -p "\$LOGS"
if [[ ! -x "$PYTHON" || ! -d "$REPO/src" ]]; then
  /usr/bin/osascript -e 'display alert "cellMatcher" message "Python environment or codeANTs checkout is missing. Rerun tools/install_cellmatcher_app.sh."'
  exit 1
fi
cd "$REPO"
export PYTHONPATH="$REPO/src" KMP_DUPLICATE_LIB_OK=TRUE
exec "$PYTHON" -m codeants_2pf_hcr.apps.cellmatcher >>"\$LOGS/launcher.log" 2>&1
LAUNCH
chmod +x "$APP/Contents/MacOS/cellMatcher"

# Register with LaunchServices/Spotlight so Cmd+Space finds it immediately.
/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister -f "$APP" || true
mdimport "$APP" 2>/dev/null || true
echo "Installed $APP (repo: $REPO, python: $PYTHON)"
