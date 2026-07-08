#!/bin/bash
set -e

echo "------ startup installation ------"
echo "Current user:"
whoami || true

echo "Installing ClamAV"
apt-get update
apt-get install -y clamav clamav-freshclam || true

echo "Stopping freshclam service"
systemctl stop clamav-freshclam || true
pkill freshclam || true

echo "Preparing log folder.."
mkdir -p /var/log/clamav
touch /var/log/clamav/freshclam.log
chown clamav:clamav /var/log/clamav/freshclam.log || true

echo "Updating virus database.."
freshclam --verbose || true

echo "Checking clamscan.."
which clamscan || true
clamscan --version
ls -lh /var/lib/clamav

echo "Starting python app.."
python -m uvicorn main:app --host 0.0.0.0 --port 8000
