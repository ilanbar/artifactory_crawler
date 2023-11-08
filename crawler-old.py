from dataclasses import dataclass
from msilib.schema import Directory
from threading import current_thread
from six.moves import urllib
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, wait
import ssl
import pathlib
import time

@dataclass
class Runner:
    artifactory_url: str
    directory_name_filter: list
    file_extentions_filter: list
    output_json_file: str = None
    disable_cache : bool = False
    debug_prints : bool = True

    def __post_init__(self) -> None:
      # Create the output dictionary
      self.crawler = dict()

      # Create SSL context
      self.ctx = ssl.create_default_context()
      self.ctx.check_hostname = False
      self.ctx.verify_mode = ssl.CERT_NONE

      # Create empty dict entry for the artifactory_url string
      self.crawler[self.artifactory_url] = dict()

    '''
    The method does recursive scan of artifactoery files and folders
    The dirs parameter *args list contains the sub folder nesting strings
    '''
    def start(self, *dirs):
      # initialization part
      url_path = self.artifactory_url
      dir_path = self.artifactory_url.rsplit('/', 1)[1]
      workers = list()

      if self.debug_prints:
        print(f"[Info][{current_thread().name}] Scanning url {self.artifactory_url}")

      # Create direct access into the crawler dictionary
      while True:
          try:
              result = urllib.request.urlopen(url_path, context=self.ctx)
          except:
              print(f"[Warning][{current_thread().name}] Failed to open {url_path}, Retrying...")
              time.sleep(60)
          else:
              break

      # Extract list of HREF items
      soup = BeautifulSoup(result, "lxml")
      href_items = soup.find_all('a', href=True)

      href_items = set([directory.text for directory in href_items]) & set(self.directory_name_filter)
      # Create ThreadPoolExecutor for all HREF items
      pool = ThreadPoolExecutor(max_workers=len(href_items))

      # Submit the crawler threads here
      for href_item in href_items:
          if href_item and href_item != "../": # with skip parent folder ..
            workers.append(
              pool.submit(self.thread_worker, href_item, url_path, dirs, dir_path )
            )

      # Wait for all threads to complete
      pool.shutdown(wait=True, cancel_futures=False)
      for worker_task in workers:
          if worker_task._exception != None:
              print(f"[Error][{current_thread().name}] state {worker_task._state} / exception {worker_task._exception}")

    #################################################################3
    def thread_worker(self, href_item, url_path, dirs, dir_path):
      if href_item.endswith("/"):  # Directory case
        crawler_access_path = ""
        flatten_list = list()

        if self.debug_prints:
          print(f"[Info][{current_thread().name}] thread_worker {href_item} started")

        # check if we have nested list
        if dirs != ():
            self.__flatten__(dirs, flatten_list)

        # Create:
        # 1. Crawler_access_path - access string to the crawler dictionary
        # 2. url_path - artifactory_url with the subfolder path
        # 3. dir_path - local folder path
        for dir in flatten_list:
            crawler_access_path += f"['{dir}']"
            url_path += f"/{dir}"
            dir_path += f"/{dir}"

        # Create local folder structure
        pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)

        crawler_access = eval(f"self.crawler['{self.artifactory_url}']{crawler_access_path}")
        directory = href_item[:-1]  # Remove the folder '/' suffix
        if self.disable_cache or (directory not in crawler_access):
            if self.debug_prints:
              print(f"[Info] Adding [{directory}][{current_thread().name}] to crawler")
            crawler_access[directory] = {
                "path": url_path,
                "type": "dir"
            }
        self.start(list(dirs)+[directory])
      else:
          # Crawler found file case
          print(f"[Info][{current_thread().name}] Found file [{href_item}]")

    def __flatten__(self, list_of_items, out_list):
      for item in list_of_items:
        if type(item) is list:
          self.__flatten__(item, out_list)
        else:
          out_list.append(item)

if __name__ == "__main__":
    # test the Runner class
    crawler = Runner(
        "https://ubit-artifactory-or.intel.com/artifactory/client-bios-or-local/Daily/LunarLake/lunarlake_family/LunarLake_2324_00",
        [
          "FSP_Wrapper_X64_VS_Release/",
          "FSP_Wrapper_X64_VS_Debug/"
        ],
        [
          ".bin",
          ".efi",
          ".rom"
        ]
    )
    crawler.start()
r_X64_VS_Debug/"
        ],
        [
          ".bin",
          ".efi",
          ".rom"
        ]
    )
    crawler.start()
r_X64_VS_Debug/"
        ],
        [
          ".bin",
          ".efi",
          ".rom"
        ]
    )
    crawler.start()
