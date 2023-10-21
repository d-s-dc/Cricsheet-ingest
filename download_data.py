import urllib.request
from zipfile import ZipFile
from os import remove, path, mkdir, rename


# This will download data and store it in "data" sub-folder
class downloadData:
    def __init__(self, url : str) -> None:
        self.url = url
        self.downloadZip(url)
    
    def downloadZip(self, url : str) -> None:
        folder_path = "./data/" + url.split("/")[-1].rstrip(".zip")
        self.data_path = folder_path

        # Check if data folder is already there
        if not path.exists("./data") or not path.isdir("./data"):
            mkdir("./data")

        # If files are already downloaded then return
        if path.exists(self.data_path):
            return

        # Download the zip file and extract it's data
        print("Downloading data...")
        zip_path, _ = urllib.request.urlretrieve(url)
        with ZipFile(zip_path, "r") as f:
            f.extractall(folder_path)
        
        # Remove the zip file
        remove(zip_path)

        # Minor Correction in a file
        if "female" not in self.url:
            with open(self.data_path + "/64933.json", "r") as f:
                lc = 1

                with open(self.data_path + "/64933_1.json", "w") as f2:
                    for line in f:
                        if lc != 4063:
                            f2.write(line)
                        lc += 1
        
            remove(self.data_path + "/64933.json")
            rename(self.data_path + "/64933_1.json", self.data_path + "/64933.json")
        print("Data Stored")