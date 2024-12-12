#!/bin/bash

echo "Deploy to Vultr"
echo "请关闭VPN"



ip="64.176.47.167"
port="22"
username="root"
password="Rk]4=UqL.sToPkQs"


local_path="/Users/alex/codebase/zen/stock-receiver/"
remote_path="/root/zen/stock-receiver/"

# Using sshpass to handle password authentication
rsync --progress -avz --delete -e "sshpass -p '$password' ssh -p $port" "${local_path}." "$username@$ip:$remote_path"



