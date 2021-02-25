#!/usr/bin/env python3

import sys, os, json, time
import threading
import signal
import configparser

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# Disable warnings about red https
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Configure logging
import logging

# Input parameters
#onezoneUrl="https://onezone-acc-test.cnaf.infn.it"
#sourceProvider="oneprovider-acc-test.cnaf.infn.it"
#apiToken="MDAyYmxvY2F00aW9uIG9uZXpvbmUtYWNjLXRlc3QuY25hZi5pbmZuLml00CjAwNmJpZGVudGlmaWVyIDIvbm1kL3Vzci1hODBlOTk4ZjUwNTE00YWJmZTI3ZWExNWRkMTBkOGZkN2NoNTE00YS9hY3QvOWQzZDgxZTNhM2I5MTEzMzRmYjUyYjcyYTZkZjQzN2ZjaGRkNDAKMDAyZnNpZ25hdHVyZSD8g6yUb00qbfHed58azTrck4017fQOoFD300FPSYqaUVHSAo"
#logging.propagate=False

logdir = "/var/log/changes-stream/"

def read_config(filename="/etc/changes-stream.conf"):
  config = configparser.ConfigParser()
  if not config.read(filename):
    lm.error("ERROR: Unable to read config file {}".format(filename))
    sys.exit(1)
  for i in ["onezoneUrl","sourceProvider","apiToken"]:
    if i not in config['conf'] or config['conf'][i] == "":
       lm.error("ERROR: Config value {} not defined".format(i))
       sys.exit(1)
    return(config['conf']['onezoneUrl'], config['conf']['sourceProvider'], config['conf']['apiToken'])


stop_requested = False
def signal_handler(signum, frame):
  lm.info("Interrupt Requested (signal: %s)\n" % signum)

  global stop_requested
  stop_requested = True

def setup_logger(logger_name):

  log_setup = logging.getLogger(logger_name)
  formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%Y/%m/%d %I:%M:%S')
  fileHandler = logging.FileHandler(logdir+"onedata_"+logger_name+".log", mode='a')
  fileHandler.setFormatter(formatter)
  log_setup.setLevel(logging.INFO)
  log_setup.addHandler(fileHandler)
  return (log_setup)


def read_last_seq (spaceName):
  try:
    f=open("{}onedata_{}_last_seq_number.txt".format(logdir,spaceName),"r")
    last_seq=f.readlines()[0]
    f.close()
    return int(last_seq)
  except Exception as e:
    return -1

def update_last_seq (spaceName,num):
  try:
    f=open("{}onedata_{}_last_seq_number.txt".format(logdir,spaceName),"w")
    f.write(str(num))
    f.close()
  except Exception as e:
    lm.error ("start exception update {} {}".format(spaceName,num))
    return 0

def get_spaces():
  spaces_dict=dict()
  # Get spaceIds and spaces names
  with requests.Session() as session:
    session.headers.update({'X-Auth-Token': apiToken })
    url="{}/api/v3/onezone/user/spaces".format(onezoneUrl)
    response = session.get(url,verify=False)

    for space in response.json()['spaces']:
      url="{}/api/v3/onezone/spaces/{}".format(onezoneUrl,space)
      response = session.get(url,verify=False)
      spaceId=response.json()['spaceId']
      spaceName=response.json()['name']
      spaces_dict[spaceId]=spaceName
  if not spaces_dict:
    lm.error("No spaces exists in the provider {}".format(sourceProvider))
    sys.exit(1)
  return (spaces_dict)

def collect_changes(spaces_dict):
  for id in spaces_dict:
    name=spaces_dict[id]
    lm.info("Main : create and start thread {}".format(name))
    x = threading.Thread(target=thread_collect_changes, args=(id,name))
    #threads.append(x)
    x.start()

def thread_collect_changes(spaceId,spaceName):
#  lm.info("Thread %s: starting", spaceName)
  lt=setup_logger(spaceName)
  lastSeq=read_last_seq(spaceName)

  # Start listening the changes API
  changesJSON='{ "triggers": ["fileMeta", "fileLocation", "times" ], "fileMeta": { "fields": ["name", "type", "mode", "owner", "provider_id", "shares", "deleted"], "always": false }, "fileLocation": {"fields": ["provider_id", "storage_id", "size", "space_id", "storage_file_created"], "always": false }, "times": {"fields": ["atime", "mtime", "ctime"], "always": false} }'
  while not stop_requested:
    with requests.Session() as session:
      session.headers.update({'X-Auth-Token': apiToken })
      session.headers.update({'content-type': 'application/json' })
      url="https://{}/api/v3/oneprovider/changes/metadata/{}?timeout=10000&last_seq={}".format(sourceProvider,spaceId,lastSeq+1)
#      lt.info(str(lastSeq))
      response = session.post(url,data=changesJSON,verify=False,stream=True)

      lines = response.iter_lines()
      for line in lines:
        if not line:
          continue
        dl = json.loads(line.decode('utf-8')) #decoded_line
        lastSeq = int(dl['seq'])
        lt.info(f"{dl['filePath']} {json.dumps(dl,sort_keys=True)}")
        update_last_seq(spaceName,lastSeq)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

lm=setup_logger("main")

(onezoneUrl,sourceProvider,apiToken)=read_config()
lm.info("CONFIG READ with values: onezoneUrl: {}, sourceProvider: {}, apiToken: {}".format(onezoneUrl,sourceProvider,apiToken))

# Collect all Spaces of Provider
sp = get_spaces()

# Start threads of changes
collect_changes(sp)
