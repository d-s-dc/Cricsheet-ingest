Requirements

MySQL, Java & Python should’ve been installed. I cannot provide code for that since the process is different for different OS
JAVA_HOME variable should be set
Ubuntu - then write the following on terminal
JAVA_HOME=$(readlink -f $(which java) | sed -E 's/\/b.*//')
echo "export JAVA_HOME=${JAVA_HOME} >> ~/.bashrc"
Mac - Link
Windows - Link


Running

Open the folder on the terminal and run the following
pip install -r requirements.txt  && python main.py
