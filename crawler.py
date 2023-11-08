from dataclasses import dataclass
from threading import current_thread
from urllib.parse import urljoin, urlsplit
from six.moves import urllib
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, wait
import ssl
import time
from datetime import datetime
import os

date_format = "%d-%b-%Y %H:%M"

@dataclass
class Runner:
    artifactory_url: str
    directory_name_filter: list
    file_extentions_filter: list
    datetime_filter: str
    output_json_file: str = None
    disable_cache : bool = False
    debug_prints : bool = True
    multi_threaded: bool = True

    def __post_init__(self) -> None:
      # Create the output dictionary
      self.crawler = dict()

      # Create SSL context
      self.ctx = ssl.create_default_context()
      self.ctx.check_hostname = False
      self.ctx.verify_mode = ssl.CERT_NONE

      # fix (add '/' suffix) to the artifactory_url if needed
      if not self.artifactory_url.endswith('/'):
        self.artifactory_url+="/"
      # Create empty dict entry for the artifactory_url string
      self.crawler[self.artifactory_url] = dict()

    '''
    The method does recursive scan of artifactoery files and folders
    The The folder_path contains the list of folders after the artifactory_url
    '''
    def start(self, url_path=None, folder_path=list()):
      # First time this function is called we set url_path to artifactory_url
      # The folder_path contains the list of folders after the artifactory_url
      if url_path == None:
        url_path = self.artifactory_url
        folder_path = [urlsplit(url_path).path.split("/")[-2]]

      if self.multi_threaded:
        workers = list()

      if self.debug_prints:
        print(f"[Info][{current_thread().name}] Scanning url {url_path}")

      # Create direct access into the crawler dictionary
      while True:
          try:
              result = urllib.request.urlopen(url_path, context=self.ctx)
          except:
              print(f"[Warning][{current_thread().name}] Failed to open {url_path}, Retrying...")
              time.sleep(60)
          else:
              break

      # href_items = set([directory.text for directory in href_items]) & set(self.directory_name_filter)
      # datetime_object = datetime.strptime(date_string, date_format)

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
                folder_path.append(href_string)
                if self.debug_prints:
                  print(f"[Info][{current_thread().name}] Adding [{href_string}][{current_thread().name}] to crawler")
                temp_url_path = urljoin(f"{url_path}/", href_string)
                if self.multi_threaded:
                    workers.append(pool.submit(self.start, temp_url_path, folder_path))
                else:
                    self.start(temp_url_path, folder_path)
              else:
                  if self.debug_prints:
                    print(f"[Info][{current_thread().name}] Found file [{href_string}]")

      # Wait for all threads to complete
      if self.multi_threaded:
        pool.shutdown(wait=True, cancel_futures=False)
        for worker_task in workers:
            if worker_task._exception != None:
                print(f"[Error][{current_thread().name}] state {worker_task._state} / exception {worker_task._exception}")

    #################################################################3
    def thread_worker(self, href_string, url_path, dir_path):
      if href_string.endswith("/"):  # Directory case
        crawler_access_path = ""
        flatten_list = list()

        if self.debug_prints:
          print(f"[Info][{current_thread().name}] thread_worker {href_string} started")

        # check if we have nested list
        # if dirs != ():
        #     self.__flatten__(dirs, flatten_list)

        # Create:
        # 1. Crawler_access_path - access string to the crawler dictionary
        # 2. url_path - artifactory_url with the subfolder path
        # 3. dir_path - local folder path
        for dir in flatten_list:
            crawler_access_path += f"['{dir}']"
            url_path += f"/{dir}"
            dir_path += f"/{dir}"

        crawler_access = eval(f"self.crawler['{self.artifactory_url}']{crawler_access_path}")
        directory = href_string[:-1]  # Remove the folder '/' suffix
        if self.disable_cache or (directory not in crawler_access):
            if self.debug_prints:
              print(f"[Info][{current_thread().name}] Adding [{directory}][{current_thread().name}] to crawler")
            crawler_access[directory] = {
                "path": url_path,
                "type": "dir"
            }
        flatten_list.append(directory)
        self.start(flatten_list)
      else:
          # Crawler found file case
          print(f"[Info][{current_thread().name}] Found file [{href_string}]")

@dataclass
class Factory(Runner):

    @classmethod
    def lunarlake_bios_runner(cls):
      return cls(
          artifactory_url="https://ubit-artifactory-or.intel.com/artifactory/client-bios-or-local/Daily/LunarLake/lunarlake_family",
          directory_name_filter=["FSP_Wrapper_X64_VS_Release/", "FSP_Wrapper_X64_VS_Debug/"],
          file_extentions_filter=[".bin", ".efi", ".rom"],
          # multi_threaded=False,
          datetime_filter="01-Oct-2023 16:06"
      )

if __name__ == "__main__":
    crawler = Factory.lunarlake_bios_runner()  # multi_threaded=False
    crawler.start()
