from dataclasses import dataclass
from threading import current_thread
from urllib import request
from urllib.parse import urljoin, urlsplit
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import json
import ssl
import time
import os
import re
import subprocess

@dataclass
class Runner:
    artifactory_url: str
    directory_name_filter: list
    filter_in_zip_file: list
    time_filter: str
    output_json_file: str = None
    disable_cache : bool = False
    debug_prints : bool = True
    multi_threaded: bool = True

    def __post_init__(self) -> None:
      # Create SSL context
      self.ctx = ssl.create_default_context()
      self.ctx.check_hostname = False
      self.ctx.verify_mode = ssl.CERT_NONE

      # fix (add '/' suffix) to the artifactory_url if needed
      if not self.artifactory_url.endswith('/'):
        self.artifactory_url+="/"

      self.__load_dict__()

      # create time object for the self.time_filter
      self.date_format = "%d-%b-%Y %H:%M"
      self.time_filter_obj = time.strptime(self.time_filter, self.date_format)

    def __load_dict__(self):
      # define the output_json_file is needed
      if self.output_json_file == None:
        self.output_json_file = self.artifactory_url.split('/')[-2]+".json"

      # create empty json file if not exist
      if not os.path.isfile(self.output_json_file):
        with open(self.output_json_file, "w") as fp:
          json.dump(fp, dict())

      pass
      # self.crawler = dict()



      #   if os.path.isfile(self.output_json_file):
      #     self.json_fp = open(self.output_json_file, "r+")
      #   else:
      #     self.json_fp = open(self.output_json_file, "w")

      #   if self.json_fp != None:
      #     self.crawler = json.load(self.json_fp)

      # # Create empty dict entry for the artifactory_url string
      # self.crawler[self.artifactory_url] = dict()

    '''
    The method does recursive scan of artifactoery files and folders
    '''
    def start(self, url_path=None):
      # First time this function is called we set url_path to artifactory_url
      if url_path == None:
        url_path = self.artifactory_url

      if self.multi_threaded:
        workers = list()

      if self.debug_prints:
        print(f"[Info][{current_thread().name}] Scanning url {url_path}")

      result = self.__urlopen__(url_path)

      if url_path.endswith('/'):
        # Extract list of HREF items
        soup = BeautifulSoup(result, "lxml")
        href_items = soup.find_all('a', href=True)
        # Create ThreadPoolExecutor for all HREF items
        if self.multi_threaded:
          pool = ThreadPoolExecutor(max_workers=len(href_items))
        for href_item in href_items:
            href_string = href_item.attrs['href']
            if href_string and href_string != "../": # with skip parent folder ..
              if href_string.endswith('/'):
                if self.debug_prints:
                  print(f"[Info][{current_thread().name}] Adding [{href_string}][{current_thread().name}] to crawler")
                temp_url_path = urljoin(f"{url_path}/", href_string)
                if not self.__match_date_filter__(href_item.nextSibling.strip()):
                  if self.debug_prints:
                    print(f"[Info][{current_thread().name}] Skipping [{href_string}][{current_thread().name}] due to date filter")
                  continue
                if self.multi_threaded:
                    workers.append(pool.submit(self.start, temp_url_path))
                else:
                    self.start(temp_url_path)
              else:
                  if self.__match_folder_filter__(urljoin(url_path, href_string)) and \
                    self.__match_date_filter__(href_item.nextSibling.strip()):
                      if self.debug_prints:
                        print(f"[Info][{current_thread().name}] Found file [{href_string}]")
                      self.Unpack(urljoin(url_path, href_string))


      # Wait for all threads to complete
      if self.multi_threaded:
        pool.shutdown(wait=True) # , cancel_futures=False
        for worker_task in workers:
            if worker_task._exception != None:
                print(f"[Error][{current_thread().name}] state {worker_task._state} / exception {worker_task._exception}")

    #################################################################
    def __urlopen__(self, url_path):
      while True:
          try:
              result = request.urlopen(url_path, context=self.ctx)
          except:
              print(f"[Warning][{current_thread().name}] Failed to open {url_path}, Retrying...")
              time.sleep(1)
          else:
              return result

    #################################################################
    def __match_folder_filter__(self, url):
      for dirname in self.directory_name_filter:
          if dirname in url:
            return True
      return False

    ##################################################################
    def __match_date_filter__(self, date_string):
      m = re.match("(\d{1,}-.*-\d{4} \d{2}:\d{2})", date_string)
      if m:
        date_string = m.group(1)
        time_object = time.strptime(date_string, self.date_format)
        if time_object >= self.time_filter_obj:
          return True
      return False

    ##################################################################
    def Unpack(self, file_path):
        if self.debug_prints:
          print(f"[Info][{current_thread().name}] Downloading [{file_path}] ...")

        local_filename, _ = request.urlretrieve(file_path)
        if self.debug_prints:
          print(f"[Info][{current_thread().name}] Local file is [{local_filename}]")
        file_list = list()
        if any(ext in file_path for ext in (".zip", ".7z")):
          result  = subprocess.run(["7z", "l", local_filename], capture_output=True,  text=True)
          for line in result.stdout.split("\n"):
              for ext in self.filter_in_zip_file:
                if line.endswith(ext):
                    file_list.append(list(filter(None, line.split(" ")))[-1])
          os.remove(local_filename)
        else:
          # Non compressed files
          file_list.append(file_path)
        if self.debug_prints and file_list != list():
          print(f"[Info][{current_thread().name}] [{file_path}] has these files {file_list}")
        if file_list != list():
          for item in file_path.split(self.artifactory_url)[1].split('/'):
            print(item)
        #crawler
@dataclass
class Factory(Runner):

    @classmethod
    def lunarlake_family_runner(cls):
      return cls(
          artifactory_url="https://ubit-artifactory-or.intel.com/artifactory/client-bios-or-local/Daily/LunarLake/lunarlake_family",
          directory_name_filter=["/FSP_Wrapper_X64_VS_Release/", "/FSP_Wrapper_X64_VS_Debug/"],
          filter_in_zip_file=[
              ".rom", "Simics.bin",
              "X64\\LunarLakePlatformDriver.efi", "X64\\MerlinX.efi", "X64\\XmlWriter.efi", "X64\\ValSharedMailbox.efi"
              ],
          time_filter="01-Oct-2023 16:06",
          # multi_threaded=False
      )

if __name__ == "__main__":
    crawler = Factory.lunarlake_family_runner()
    crawler.start()
