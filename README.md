# Nifi Auto Testing #

This repo contains a simple python script to run NiFi as a container, load the 
saved NiFi flow in as the root-level processor group, and then run a simple
test against it. Very much a PoC, it'll get improved on.

## Running it ##

```
$> virtualenv .env
$> source .env/bin/activate
$> pip install -r requirements.txt
$> ./flow_test.py
```
