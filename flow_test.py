#!/usr/bin/env python3

import docker
import requests
import time
import json
import sys


class NifiTester:
  base_url = "http://localhost:8080/nifi-api"
  test_url = "http://localhost:8081/testInput"
  
  wait_limit = 12
  wait_interval = 5
  conn_timeout = 2
  container_image = "apache/nifi:1.12.1"
  
  nifi_counter_name = "testCounterSuccess"
  nifi_counter_context = "All UpdateCounter's"
  
  docker_client = docker.from_env()
  
  docker_ports = {
    '8080': '8080',
    '8081':'8081'
  }
  start_time = time.time()
  
  def start_container(self):
    print(f"starting {self.container_image} as a container")
    return self.docker_client.containers.run(
      self.container_image,
      detach=True,
      ports=self.docker_ports,
      remove=True
    )
  
  def wait_for_start(self):
    print("waiting for nifi container to initialize")
    startup_counter = 1
    while startup_counter <= self.wait_limit:
      print(f"connection attempt {startup_counter}", end=' ')
      try:
        resp = requests.get(f"{self.base_url}/flow/status", timeout=self.conn_timeout)
        print(f"response code: {resp.status_code}")
        if resp.status_code == requests.codes.ok:
          break
      except requests.exceptions.ConnectionError:
        print(f"connection failed, sleeping for {self.wait_interval} seconds")
      startup_counter += 1
      time.sleep(self.wait_interval)
  
  def get_root_id(self):
    resp = requests.get(f"{self.base_url}/process-groups/root")
    resp.raise_for_status()
    data = resp.json()
    return data['id']
  
  def put_data(self, url, data):
    resp = requests.put(url, json=data)
    resp.raise_for_status()
  
  def munge_template(self, data):
    output = {
      "processGroupRevision": {
        "clientId": "root",
        "version": 0
      },
      "versionedFlowSnapshot": data
    }
    return output
  
  def start_root_flow(self, root_id):
    data = {
      "id": root_id,
      "state": "RUNNING"
    }
    resp = requests.put(f"{self.base_url}/flow/process-groups/{root_id}", json=data)
    resp.raise_for_status()
    time.sleep(1)
    print(resp.text)
  
  def load_flow(self):
    try:
      input_file = sys.argv[1]
    except IndexError:
      input_file = "NiFi_Flow.json"
    with open(input_file, 'r') as f:
      data = json.load(f)
    munged_data = self.munge_template(data)
  
    print("getting ID for root processor group")
    root_id = self.get_root_id()
    print("sending flow to nifi")
    self.put_data(f"{self.base_url}/process-groups/root/flow-contents", munged_data)
    print("starting flow")
    self.start_root_flow(root_id)
  
  def check_counter_value(self):
    resp = requests.get(f"{self.base_url}/counters")
    
    resp.raise_for_status()
    data = resp.json()
    
    all_counters = data['counters']['aggregateSnapshot']['counters']
    for counter in all_counters:
      context = counter['context']
      name = counter['name']
      if name == self.nifi_counter_name and context == self.nifi_counter_context:
        return counter['value']
    return "0"
  
  def post_data(self, url, data):
    # print(f"posting data to {url}")
    resp = requests.post(url, json=data)
    resp.raise_for_status()
  
  def send_and_check(self):
    initial_value = self.check_counter_value()
    print(f"initial value of {self.nifi_counter_name} is {initial_value}")
    
    data = {"foo": "bar"}
    print("sending data")
    self.post_data(self.test_url, data)
    time.sleep(1)
    new_value = self.check_counter_value() 
    print(f"new value of {self.nifi_counter_name} is {new_value}")
  
    if new_value <= initial_value:
      print("an error occurred, counter did not increase")
    else:
      print("counter incremented successfully")
  
  def teardown(self, container):
    print("stopping container")
    # container = self.docker_client.containers.get(self.container_name)
    container.stop()

  def check_time(self):
    print("%s seconds elapsed since start" % (time.time() - self.start_time))

if __name__=="__main__":
  tester = NifiTester()

  nifi_container = tester.start_container()
  tester.wait_for_start()
  tester.check_time()
  tester.load_flow()
  tester.send_and_check()
  tester.teardown(nifi_container)
  tester.check_time()
