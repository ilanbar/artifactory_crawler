import bdb
from typing import final
from bs4 import BeautifulSoup
from six.moves import urllib
from urllib.parse import urljoin
from urllib import request
import pathlib
import subprocess
from concurrent.futures import ThreadPoolExecutor, wait
from threading import current_thread
import json # ijson will be used for large files
import ssl
import re
import os
import sys
import pdb, traceback
import time

disable_cache = False
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
global crawler

def flatten( list_of_items, out_list ):
    for item in list_of_items:
        if type(item) is list:
            flatten(item, out_list)
        else:
            out_list.append(item)

def Unpack(crawler_access, file_path, dir_path, file, file_type, filter):
    print(f"[Info][{current_thread().name}] Downloading [{file_path}] ...")
    local_filename, _ = request.urlretrieve(file_path)
    print(f"[Info][{current_thread().name}] Local file is [{local_filename}]")
    if file_type != "std":
        result  = subprocess.run(["7z", "l", local_filename], capture_output=True,  text=True)
        print(f"[Info][{current_thread().name}] Decompressing {os.path.join(dir_path, file)}]")
        file_log = os.path.join(dir_path, file)+".log"
        with open(file_log, "w") as f:
            for line in result.stdout.split("\n"):
                for ext in filter:
                    if line.endswith(ext):
                        f.write(f"{line}\n")
        if os.path.getsize(file_log) == 0:
            os.remove(file_log)
        else:
            crawler_access["log-path"] = file_log
    else:
        outfile = os.path.join(dir_path, file)+".stat"
        with open(outfile, "w") as f:
            f.write(str(os.stat(local_filename)))
    print(f"[info][{current_thread().name}] Removing file]")
    os.remove(local_filename)

def worker(item, crawler_access, url_path, dirs, dir_path, filter):
  if item.text.endswith("/"):
      dir = item.text[:-1]
      if dir != "..":
          print(f"[Info][{current_thread().name}] Found dir [{dir}]")
          if disable_cache or (dir not in crawler_access):
              print(f"[Info] Adding [{dir}][{current_thread().name}] to crawler")
              crawler_access[dir] = {
                  "path": url_path,
                  "type": "dir"
              }
          run(url, filter, _file, list(dirs)+[dir])
  else:
      file = item.text
      file_path = urljoin(f"{url_path}/", file)
      file_extension = pathlib.Path(file).suffix
      file_type = "std"
      if file_extension in [".zip", ".7z"]:
          file_type = file_extension
      print(f"[Info][{current_thread().name}] Found file [{file}]")
      while True:
        try:
            conn = urllib.request.urlopen(file_path, timeout=120)
        except:
            print(f"[Warning][{current_thread().name}] Failed to open {url_path}, Retrying...")
            # extype, value, tb = sys.exc_info()
            # traceback.print_exc()
            # pdb.post_mortem(tb)
            time.sleep(60)
        else:
            break
      last_modified = conn.headers['last-modified']
      if disable_cache or (file not in crawler_access):
          print(f"[Info][{current_thread().name}] Adding [{file}] to crawler")
          crawler_access[file] = {
              "path": file_path,
              "type": "file",
              "file-type": file_type,
              "last-modified": last_modified
          }

          Unpack(crawler_access[file], file_path, dir_path, file, file_type, filter)
      else:
          print(f"[Info][{current_thread().name}] File already inside the crawler")
          print(f"[Info][{current_thread().name}] Checking timestamp")
          if crawler_access[file]["last-modified"] != last_modified:
              print(f"[Info][{current_thread().name}] file [{file}] time-stamp changed, re-parsing..")
              Unpack(crawler_access[file], file_path, dir_path, file, file_type, filter)
              crawler_access[file]["last-modified"] = last_modified

def run(url, filter, _file, *dirs):
    global q
    print(f"[Info][{current_thread().name}] Scanning url [", end="")
    dir_path = url.rsplit('/', 1)[1]
    _dirs = list()
    if dirs != ():
        flatten(dirs, _dirs)
    url_path = url
    crawler_access_path = ""
    for dir in _dirs:
        crawler_access_path += f"['{dir}']"
        url_path += f"/{dir}"
        dir_path += f"/{dir}"
    print(f"[{current_thread().name}]{url_path}]")
    pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)
    crawler_access = eval(f"crawler['{url}']{crawler_access_path}")
    while True:
        try:
            result = urllib.request.urlopen(url_path, context=ctx)
        except:
            print(f"[Warning][{current_thread().name}] Failed to open {url_path}, Retrying...")
            # extype, value, tb = sys.exc_info()
            # traceback.print_exc()
            # pdb.post_mortem(tb)
            time.sleep(60)
        else:
            break
    result = urllib.request.urlopen(url_path, context=ctx)
    soup = BeautifulSoup(result, "lxml")
    href_items = soup.find_all('a', href=True)
    pool = ThreadPoolExecutor(max_workers=40) # len(href_items)
    workers = []
    for item in href_items:
        if item.text:
          workers.append(pool.submit(worker, item, crawler_access, url_path, dirs, dir_path, filter))

    pool.shutdown(wait=True,cancel_futures=False)
    for worker_task in workers:
        if worker_task._exception != None:
            print(f"[Error][{current_thread().name}] state {worker_task._state} / exception {worker_task._exception}")

if __name__=="__main__":
    _file = open("crawler.json", "r+")
    crawler=json.load(_file)
    with open("crawler.json", "r+") as _file:
        for url, db in crawler.items():
            run(url, db["file_info_filter"], _file)
        _file.seek(0)
        json.dump(crawler, _file, indent=4)
        _file.truncate()
