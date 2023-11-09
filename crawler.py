from dataclasses import dataclass
from threading import current_thread
from urllib.parse import urljoin, urlsplit
from six.moves import urllib
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import ssl
import time
import os
import re
import subprocess

@dataclass
class Runner:
    artifactory_url: str
    directory_name_filter: list
    file_extentions_filter: list
    time_filter: str
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

      # create time object for the self.time_filter
      self.date_format = "%d-%b-%Y %H:%M"
      self.time_filter_obj = time.strptime(self.time_filter, self.date_format)

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


      # Wait for all threads to complete
      if self.multi_threaded:
        pool.shutdown(wait=True, cancel_futures=False)
        for worker_task in workers:
            if worker_task._exception != None:
                print(f"[Error][{current_thread().name}] state {worker_task._state} / exception {worker_task._exception}")

    #################################################################
    def __urlopen__(self, url_path):
      while True:
          try:
              result = urllib.request.urlopen(url_path, context=self.ctx)
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

    #################################################################3
    def __match_date_filter__(self, date_string):
      m = re.match("(\d{1,}-.*-\d{4} \d{2}:\d{2})", date_string)
      if m:
        date_string = m.group(1)
        time_object = time.strptime(date_string, self.date_format)
        if time_object >= self.time_filter_obj:
          return True
      return False


    def Unpack(crowler_access, file_path, dir_path, file, file_type, filter):
        print(f"[info] Downloading [{file_path}] ...")
        local_filename, _ = request.urlretrieve(file_path)
        print(f"[Info] Local file is [{local_filename}]")
        if file_type != "std":
            result = subprocess.run(
                ["7z", "l", local_filename], capture_output=True,  text=True)
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


@dataclass
class Factory(Runner):

    @classmethod
    def lunarlake_family_runner(cls):
      return cls(
          artifactory_url="https://ubit-artifactory-or.intel.com/artifactory/client-bios-or-local/Daily/LunarLake/lunarlake_family",
          directory_name_filter=["/FSP_Wrapper_X64_VS_Release/", "/FSP_Wrapper_X64_VS_Debug/"],
          file_extentions_filter=[".bin", ".efi", ".rom"],
          time_filter="01-Oct-2023 16:06"
      )

if __name__ == "__main__":
    crawler = Factory.lunarlake_family_runner()  # multi_threaded=False
    crawler.start()
