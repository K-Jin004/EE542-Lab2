#!/bin/bash

set -e

JUMP_HOST="vyos@192.168.183.135"
CLIENT_VM="mj_student@192.168.20.100"
SERVER_VM="mj_student@192.168.10.100"
REMOTE_DIR="~/lab2"

echo "Uploading client.cpp to client VM..."
scp -J "$JUMP_HOST" client.cpp "$CLIENT_VM:$REMOTE_DIR/"

echo "Uploading server.cpp to server VM..."
scp -J "$JUMP_HOST" server.cpp "$SERVER_VM:$REMOTE_DIR/"

echo "Done."