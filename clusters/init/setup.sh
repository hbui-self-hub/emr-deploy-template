
#!/bin/bash
requirements="$1"

aws s3 cp $requirements /home/hadoop/requirements.txt

# Install requirements for Python script
sudo /usr/bin/python3 -m pip install --upgrade pip
sudo /usr/bin/python3 -m pip install --upgrade pip setuptools
sudo /usr/bin/python3 -m pip install pypandoc
sudo /usr/bin/python3 -m pip install -r /home/hadoop/requirements.txt
