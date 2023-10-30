from bs4 import BeautifulSoup
from six.moves import urllib
from urllib.parse import urljoin
from urllib import request
import pathlib
import subprocess
import threading
import queue
import json # ijson will be used for large files
from retry import retry
import ssl
import re
import os

q = queue.Queue()

# "https://ubit-artifactory-or.intel.com/artifactory/server-bios-staging-local"
# verbose = True
disable_cache = False
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
global crowler

def sync(_file):
    # save results
    _file.seek(0)  # rewind
    json.dump(crowler, _file, indent=4)
    _file.truncate()

def flatten( list_of_items, out_list ):
    for item in list_of_items:
        if type(item) is list:
            flatten(item, out_list)
        else:
            out_list.append(item)

def Unpack(crowler_access, file_path, dir_path, file, file_type, filter):
    print(f"[info] Downloading [{file_path}] ...")
    local_filename, _ = request.urlretrieve(file_path)
    print(f"[Info] Local file is [{local_filename}]")
    if file_type != "std":
        result  = subprocess.run(["7z", "l", local_filename], capture_output=True,  text=True)
        file_log = os.path.join(dir_path, file)+".log"
        with open(file_log, "w") as f:
            for line in result.stdout.split("\n"):
                for ext in filter:
                    if line.endswith(ext):
                        f.write(f"{line}\n")
        if os.path.getsize(file_log) == 0:
            os.remove(file_log)
        else:
            crowler_access["log-path"] = file_log

    else:
        outfile = os.path.join(dir_path, file)+".stat"
        with open(outfile, "w") as f:
            f.write(str(os.stat(local_filename)))
    print(f"[info] Removing file]")
    os.remove(local_filename)

def run(url, filter, _file, *dirs):
    print(f"[Info] Scanning url [", end="")
    dir_path = url.rsplit('/', 1)[1]
    _dirs = list()
    if dirs != ():
        flatten(dirs, _dirs)
    url_path = url
    crowler_access_path = ""
    for dir in _dirs:
        crowler_access_path += f"['{dir}']"
        url_path += f"/{dir}"
        dir_path += f"/{dir}"
    print(f"{url_path}]")
    pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)
    crowler_access = eval(f"crowler['{url}']{crowler_access_path}")
    result = urllib.request.urlopen(url_path, context=ctx)
    soup = BeautifulSoup(result, "lxml")
    for item in soup.find_all('a', href=True):
        if item.text:
            if item.text.endswith("/"):
                dir = item.text[:-1]
                if dir != "..":
                    print(f"[Info] Found dir [{dir}]")
                    if disable_cache or (dir not in crowler_access):
                        print(f"[Info] Adding [{dir}] to crowler")
                        crowler_access[dir] = {
                            "path": url_path,
                            "type": "dir"
                        }
                        sync(_file)
                    run(url, filter, _file, list(dirs)+[dir])
            else:
                file = item.text
                file_path = urljoin(f"{url_path}/", file)
                print(f"[Info] Found file [{file}]")
                conn = urllib.request.urlopen(file_path, timeout=30)
                last_modified = conn.headers['last-modified']
                if disable_cache or (file not in crowler_access):
                    print(f"[Info] Adding [{file}] to crowler")
                    file_extension = pathlib.Path(file).suffix
                    file_type = "std"
                    if any(ext in file for ext in (".zip", ".7z")):
                        file_type = file_extension

                    crowler_access[file] = {
                        "path": file_path,
                        "type": "file",
                        "file-type": file_type,
                        "last-modified": last_modified
                    }

                    Unpack(crowler_access[file], file_path, dir_path, file, file_type, filter)
                else:
                    print("[Info] File already inside the crowler")
                    print(f"[Info] Checking timestamp")
                    if crowler_access[file]["last-modified"] != last_modified:
                        print(f"[Info] file [{file}] time-stamp changed, re-parsing..")
                        Unpack(crowler_access[file], file_path, dir_path, file, file_type, filter)
                        crowler_access[file]["last-modified"] = last_modified

                sync(_file)

_file = open("crowler.json", "r+")
crowler=json.load(_file)
for url, db in crowler.items():
    run(url, db["file_info_filter"], _file)
_file.close()
