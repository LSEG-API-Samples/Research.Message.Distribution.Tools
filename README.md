# Research Message Distribution Tools
## Overview

![](flow.png)

## Setup

1. If you didn't have python3.7 yet please install it via https://www.python.org/ftp/python/3.7.11/Python-3.7.11.tgz
2. Run command to install python libraries 
   - python3 -m pip install -r libs.txt
3. If you encounter error ModuleNotFoundError: No module named 'Crypto' please follow step below
   - python3 -m pip uninstall crypto
   - python3 -m pip install pycrypto
   - python3 -m pip install -r libs.txt
4. Open file credentials.ini and specify all information (If you don't know information please contact https://developers.refinitiv.com)
5. Run Program please check Tool Description section
6. Messages will be stored under metadata/<subscriptionId> folder
7. Financial Research file will be downloaded as pdf/txt under reports/<subscriptionId> folder (Report will be downloaded once command contains -r)

## Tools Description

1. **Get subscriptions**
   - Get all subscriptions
        ```
        python researchMessages.py -g
        ```
   - Get a specific subscription
        ```
   	    python researchMessages.py -g -s <subscriptionId>
        ```
2. **Create a new subscription**
    - Create a new subscription but not download research report
        ```
        python researchMessages.py -c
        ```
    - Create a new subscription and download research reports with the default file type
        ```
        python researchMessages.py -c -r
        ```
    - Create a new subscription and download research reports with the pdf or txt file type
        ```
        python researchMessages.py -c -r -t <pdf or txt>
        ```
    - Create a new subscription and download research reports as signed url links
        ```
        python researchMessages.py -c -r -l
        ```
3. **Poll the message queue from the existing subscriptions**
    - Poll the message queue but not download research report
        ```
        python researchMessages.py -p -s <subscription id>
        ```
    - Poll the message queue and download research report with default file type
        ```
        python researchMessages.py -p -r -s <subscription id>
        ```
    - Poll the message queue and download research report with the pdf or txt file type
        ```
        python researchMessages.py -p -r -s <subscription id> -t <pdf or txt>
        ```
    - Poll the message queue and download research report as signed url link
        ```
        python researchMessages.py -p -r -s <subscription id> -l
        ```
	
4. **Delete all subscriptions**
    - Delete all subscriptions
        ```
        python researchMessages.py -d
        ```
    - Delete a specific subscription
        ```
        python researchMessages.py -d -s <subscriptionId>
        ```
	        

