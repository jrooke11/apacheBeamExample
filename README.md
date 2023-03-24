# apacheBeamExample
A repository to hold the code for the Apache Beam technical test



# Pre-Requisites
Create a python virtual environment and activate it:
```
python3 -m venv venv
source venv/bin/activate
```

<br>

Configure credentials by running the below command inside your venv once activated (you can authenticate with any GCP project 
as we don't actually need to connect to a specific one, but it enables line 25 on [task1.py](task1.py) to run)

```
gcloud auth application-default login
```

<br>

Install required dependencies:
```
pip install -r requirements.txt
```


<br>

To run task 1, please enter the below command at the terminal:
```
python3 task1.py
```
<br>

To run task 2, please enter the below command at the terminal:
```
python3 task2.py
```
