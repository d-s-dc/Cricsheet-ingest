# Cricsheet-ingest 🏏

This code will download ODI data from the website, pre-process it using spark and finally transfer to MySQL DB for analysis

## Requirements

1. MySQL, Java & Python should’ve been installed. I cannot provide code for that since the process is different for different OS
2. `JAVA_HOME` variable should be set
    - Ubuntu - then write the following on terminal
      ```
      JAVA_HOME=$(readlink -f $(which java) | sed -E 's/\/b.*//')
      echo "export JAVA_HOME=${JAVA_HOME} >> ~/.bashrc"
      ```
    - Mac - [Link](https://stackoverflow.com/questions/22842743/how-to-set-java-home-environment-variable-on-mac-os-x-10-9)
    - Windows - [Link](https://confluence.atlassian.com/doc/setting-the-java_home-variable-in-windows-8895.html)


## Running

Open the folder on the terminal and run the following
```
pip install -r requirements.txt  && python main.py
```
