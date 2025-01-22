#!/bin/bash
aws s3 cp s3://hello-data-terraform-backend/requirements.txt /tmp/requirements.txt
sudo python3 -m pip install --ignore-installed -r /tmp/requirements.txt
