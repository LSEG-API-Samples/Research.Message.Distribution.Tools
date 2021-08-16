# =============================================================================
# Refinitiv Data Platform demo app to subscribe to Research messages
# -----------------------------------------------------------------------------
#   This source code is provided under the Apache 2.0 license
#   and is provided AS IS with no warranty or guarantee of fit for purpose.
#   Copyright (C) 2021 Refinitiv. All rights reserved.
# =============================================================================
import requests
import json
import rdpToken
import sqsQueue
import atexit
import sys
import boto3
import os
from botocore.exceptions import ClientError
import traceback
import time
import argparse, textwrap

# Application Constants
base_URL = "https://api.refinitiv.com"
RDP_version = "/v1"
REPORTS_DIR_NAME = "reports"
currentSubscriptionID = None


# ==============================================
def subscribeToResearch():
    # ==============================================
    # get the latest access token first
    accessToken = rdpToken.getToken()

    category_URL = "/message-services"
    endpoint_URL = "/research/subscriptions"
    RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL
    requestData = {
        "transport": {
            "transportType": "AWS-SQS"
        },
        "userID": rdpToken.UUID
    }

    hdrs = {
        "Authorization": "Bearer " + accessToken,
        "Content-Type": "application/json"
    }

    print(requestData)
    dResp = requests.post(RESOURCE_ENDPOINT, headers=hdrs, data=json.dumps(requestData))
    if dResp.status_code != 200:
        raise ValueError("Unable to subscribe. Code %s, Message: %s" % (dResp.status_code, dResp.text))
    else:
        jResp = json.loads(dResp.text)
        return jResp["transportInfo"]["endpoint"], jResp["transportInfo"]["cryptographyKey"], jResp["subscriptionID"]


# ==============================================
def getCloudCredentials(endpoint):
    # ==============================================
    category_URL = "/auth/cloud-credentials"
    endpoint_URL = "/"
    RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL
    requestData = {
        "endpoint": endpoint
    }

    # get the latest access token
    accessToken = rdpToken.getToken()
    dResp = requests.get(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken}, params=requestData)
    if dResp.status_code != 200:
        raise ValueError("Unable to get credentials. Code %s, Message: %s" % (dResp.status_code, dResp.text))
    else:
        jResp = json.loads(dResp.text)
        return jResp["credentials"]["accessKeyId"], jResp["credentials"]["secretKey"], jResp["credentials"][
            "sessionToken"]


# ==============================================
def downloadReport(rMessage, subscriptionId, fileTypeValue, isRawResponse):
    # ==============================================
    print('--------- Download research report -----------')
    # print(json.dumps(rMessage, indent=2))
    pl = rMessage['payload']
    fileType = pl['DocumentFileType'] if fileTypeValue is None else fileTypeValue
    if fileTypeValue == 'txt':
        fileTypeInput = 'text'
    else:
        fileTypeInput = fileType
    docID = pl['DocumentId']
    filename = pl['DocumentFileName']

    try:
        print('Headline: %s' % pl['Headline']['DocumentHeadlineValue'])
        print('Document name: %s of type: %s, size: %s, subscriptionId: %s' % (
        filename, fileType, pl['DocumentFileSize'], subscriptionId))
    except Exception as err:
        traceback.print_exc()
        print(err)
        print(pl)

    folder_name = "{}/{}".format(REPORTS_DIR_NAME, subscriptionId)
    time_str = time.strftime("%Y%m%d-%H%M%S")
    if isRawResponse:
        try:
            filename_output = filename.replace(".pdf", ".json")
            filename = "retrieval_raw_response_{}_{}".format(time_str, filename_output)
        except Exception as err:
            print("could not prepare retrieval file name: " + str(filename))
            print(err)
    else:
        if fileTypeValue == str('txt').lower():
            try:
                filename_output = filename.replace(".pdf", ".txt")
                filename = "report_{}_{}".format(time_str, filename_output)
            except Exception as err:
                print("could not prepare report file name: " + str(filename))
                print(err)
        else:
            filename = "report_{}_{}".format(time_str, filename)

    if fileType == 'pdf' or fileType == 'txt' or fileType == 'htm':
        print('Downloading the file: %s' % filename)
        if fileType == 'htm':
            fileTypeInput = 'txt'
        category_URL = "/data/research"
        endpoint_URL = "/documents/"
        requestData = {
            "uuid": rdpToken.UUID,
            "doNotRedirect": isRawResponse
        }
        RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL + str(docID) + "/" + fileTypeInput

        # get the latest access token
        accessToken = rdpToken.getToken()
        dResp = requests.get(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken}, params=requestData)
        if dResp.status_code != 200:
            print("Error - Unable to get the research report. Code %s, Message: %s" % (dResp.status_code, dResp.text))
        else:
            try:
                if isRawResponse or fileTypeInput == 'txt':
                    with open(folder_name + "/" + filename, 'w') as f:
                        f.write(json.dumps(str(dResp.content), indent=2))
                        f.close()
                else:
                    with open(folder_name + "/" + filename, 'wb') as f:
                        f.write(dResp.content)
                        f.close()
            except Exception as err:
                print(str(dResp.content))
                print(err)

    elif fileType == 'URL':
        print('Saving the link to URL: %s' % filename)
        with open(folder_name + "/" + str(docID) + "_" + time_str + "_" + ".link", 'wb') as f:
            f.write(filename.encode())
            f.close()

    else:
        if docID is not None:
            try:
                with open(folder_name + "/" + str(docID) + "_" + time_str + "_" + "metadata_deleted.json", 'wb') as f:
                    f.write(json.dumps(rMessage, indent=2))
                    f.close()
            except Exception as err:
                print(err)
                print(rMessage)
                try:
                    with open(folder_name + "/" + str(docID) + "_" + time_str + "_" + "metadata_deleted.json", 'w') as f:
                        f.write(json.dumps(rMessage, indent=2))
                        f.close()
                except Exception as err:
                    print(err)
                    print(rMessage)

        print(json.dumps(rMessage))
        #print(json.dumps(rMessage, indent=2))


# ==============================================
def startResearchAlerts(downloadReports, subscriptionId=None, isRawResponse=False, fileType='pdf'):
    # ==============================================
    global currentSubscriptionID
    try:
        print("Subscribing to research stream ...")
        if subscriptionId is None:
            endpoint, cryptographyKey, currentSubscriptionID = subscribeToResearch()
        else:
            endpoint, cryptographyKey, currentSubscriptionID = showActiveSubscriptions(subscriptionId)
            if currentSubscriptionID is None:
                raise Exception("subscriptionID {0} is not found".format(subscriptionId))

        print("  Queue endpoint: %s" % (endpoint))
        print("  Subscription ID: %s" % (currentSubscriptionID))

        while 1:
            try:
                print("Getting credentials to connect to AWS Queue...")
                accessID, secretKey, sessionToken = getCloudCredentials(endpoint)
                print("Queue access ID: %s" % (accessID))
                print("Getting research, press BREAK to exit...")
                if downloadReports:
                    try:
                        os.mkdir("{}/{}".format(REPORTS_DIR_NAME, currentSubscriptionID))
                    except:
                        pass
                    sqsQueue.startPolling(accessID, secretKey, sessionToken, endpoint, cryptographyKey,
                                          currentSubscriptionID, isRawResponse, downloadReport, fileType)
                else:
                    sqsQueue.startPolling(accessID, secretKey, sessionToken, endpoint, cryptographyKey,
                                          currentSubscriptionID, isRawResponse)

            except ClientError as e:
                print("Cloud credentials exprired!")
    except KeyboardInterrupt:
        print("User requested break, cleaning up...")
        sys.exit(0)


# ==============================================
def removeSubscription(subscription_id=None):
    # ==============================================

    # get the latest access token
    accessToken = rdpToken.getToken()

    category_URL = "/message-services"
    endpoint_URL = "/research/subscriptions" if subscription_id is None else "/research/subscriptions?subscriptionID={}&userID={}".format(
        subscription_id, rdpToken.UUID)
    RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL

    if currentSubscriptionID:
        print("Deleting the open research subscription")
        dResp = requests.delete(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken},
                                params={"subscriptionID": subscription_id, "userID": rdpToken.UUID})
    else:
        print("Deleting ALL open research subscriptions")
        dResp = requests.delete(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken},
                                params={"userID": rdpToken.UUID})

    if dResp.status_code > 299:
        print(dResp)
        print(rdpToken.UUID)
        print("Warning: unable to remove subscription. Code %s, Message: %s" % (dResp.status_code, dResp.text))
    else:
        print("Research unsubscribed!")


# ==============================================
def showActiveSubscriptions(subscription_id=None):
    # ==============================================

    # get the latest access token
    accessToken = rdpToken.getToken()

    category_URL = "/message-services"
    endpoint_URL = "/research/subscriptions" if subscription_id is None else "/research/subscriptions?subscriptionID={}&userID={}".format(
        subscription_id, rdpToken.UUID)
    RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL

    print("Getting all open research subscriptions {0}".format(RESOURCE_ENDPOINT))
    dResp = requests.get(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken})

    if dResp.status_code != 200:
        print("uuid = " + str(rdpToken.UUID))
        raise ValueError("Unable to get subscriptions. Code %s, Message: %s" % (dResp.status_code, dResp.text))
    else:
        jResp = json.loads(dResp.text)
        print(json.dumps(jResp, indent=2))

    if 'subscriptions' in jResp:
        subscription_list = jResp['subscriptions']
        if len(subscription_list) > 0:
            endpoint = subscription_list[0]['transportInfo']['endpoint']
            cryptography_key = subscription_list[0]['transportInfo']['cryptographyKey']
            current_subscription_id = subscription_list[0]['subscriptionID']
            return endpoint, cryptography_key, current_subscription_id

    return None, None, None


def create_download_report_folder(report_dir_name_value):
    try:
        os.mkdir(report_dir_name_value)
    except:
        pass


# ==============================================
if __name__ == "__main__":
    # ==============================================

    description = """Research Message Tool description
	1) Get all subscriptions 
	 - python researchMessages.py -g
	2) Get specific subscription
	 - python researchMessages.py -g -s <subscriptionId>

	3) create a new subscription
	   3.1) No download research report
	        - python researchMessages.py -c
	   3.2) Download research report with default file type
	        - python researchMessages.py -c -r
	   3.3) Download research report with pdf/txt file type
	        - python researchMessages.py -c -r -t <pdf/txt>
	   3.4) Download research report as signed url link
	        - python researchMessages.py -c -r -l

	4) poll message queue from existing subscription
	   4.1) No download research report
	        - python researchMessages.py -p -s <subscription id>
	   4.2) Download research report with default file type
	        - python researchMessages.py -p -r -s <subscription id>
	   4.3) Download research report with pdf/txt file type
			- python researchMessages.py -p -r -s <subscription id> -t <pdf/txt>
	   4.4) Download research report as signed url link
	        - python researchMessages.py -p -r -s <subscription id> -l
	
	5) Delete all subscriptions
	   5.1) Delete all subscriptions
	        - python researchMessages.py -d
	   5.2) Delete specific subscription
	        - python researchMessages.py -d -s <subscriptionId>
	"""

    # Initialize parser
    parser = argparse.ArgumentParser(description=description, formatter_class=argparse.RawTextHelpFormatter)

    # Adding optional argument
    parser.add_argument("-g", "--get", action='store_true',
                        help="get all of subscriptions information")  # required=True

    parser.add_argument("-c", "--create", action='store_true', help="create a new subscription")

    parser.add_argument("-p", "--poll", action='store_true', help="resume polling message queue from existing subscription")

    parser.add_argument("-d", "--delete", action='store_true', help="delete all subscriptions")

    parser.add_argument("-s", "--subscriptionId", help="specify subscription id")

    parser.add_argument("-r", "--report", action='store_true',
                        help="download research report PDF/Text and store into {}/<subscriptionId> and folder will be created automatically".format(
                            REPORTS_DIR_NAME))

    parser.add_argument("-l", "--link", action='store_true',
                        help="download report as signed url link (-d must be enabled otherwise it does not work)")

    parser.add_argument("-t", "--type",
                        help="specify research report file type (Research service support pdf/txt and default value is pdf")

    # Read arguments from command line
    args = parser.parse_args()

    args_dict = vars(parser.parse_args())
    print(args_dict)

    if args.get:
        if args.subscriptionId:
            showActiveSubscriptions(args.subscriptionId)
        else:
            showActiveSubscriptions()
    elif args.delete:
        if args.subscriptionId:
            removeSubscription(args.subscriptionId)
        else:
            removeSubscription()
    elif args.create or args.poll:
        if args.poll and args.subscriptionId is None:
            raise Exception("subscriptionId is missing please check via 'python researchMessages.py -h'")
        if args.report or args.type or args.link or args.link:
            download_report = args.report if args.report is not None else False
            is_raw_response = args.link if args.link is not None else False
            if args.type is None:
                file_type = 'pdf'
            else:
                if str(args.type).lower() != 'pdf' and str(args.type).lower() != 'txt':
                    raise Exception("file type {} is not supported please check via 'python researchMessages.py -h'")
                else:
                    file_type = args.type
            startResearchAlerts(download_report, subscriptionId=args.subscriptionId, isRawResponse=is_raw_response,
                                fileType=str(file_type).lower())
        else:
            startResearchAlerts(False, subscriptionId=args.subscriptionId)
    else:
        raise Exception("Found invalid command please check via 'python researchMessages.py -h'")

    '''
    # Read arguments from command line
    args = parser.parse_args()
    print("args = " + str(args))

    check_list = [2, 3]  # download report parameter index list
    subscription_list = [3, 2]  # subscription parameter index list
    error = False
    if len(sys.argv) > 1:
        if sys.argv[1] == '-l':
            if len(sys.argv) > 2:
                showActiveSubscriptions(sys.argv[2])
            else:
                showActiveSubscriptions()
        elif sys.argv[1] == '-d':
            if len(sys.argv) > 2:
                removeSubscription(sys.argv[2])
            else:
                removeSubscription()
        elif sys.argv[1] == '-s':
            if len(sys.argv) > 2:
                startResearchAlerts(False, sys.argv[2])
            else:
                startResearchAlerts(False)
        elif sys.argv[1] == '-sr':
            create_download_report_folder(REPORTS_DIR_NAME)
            if len(sys.argv) > 2:
                startResearchAlerts(True, subscriptionId=sys.argv[2])
            else:
                startResearchAlerts(True)
        elif sys.argv[1] == '-sfr':
            create_download_report_folder(REPORTS_DIR_NAME)
            if len(sys.argv) > 2:
                startResearchAlerts(True, subscriptionId=sys.argv[2], file_type=sys.argv[3])
            else:
                startResearchAlerts(True, file_type=sys.argv[2])
        elif sys.argv[1] == '-srt':
            create_download_report_folder(REPORTS_DIR_NAME)
            if len(sys.argv) > 2:
                startResearchAlerts(True, subscriptionId=sys.argv[2], file_type=sys.argv[3], isRawResponse=True)
            else:
                startResearchAlerts(True, file_type=sys.argv[2], isRawResponse=True)
        else:
            error = True
    else:
        error = True

    if error:
        print("Found Invalid, Please check Arguments:")
        print("  -l (List active subscriptions)")
        print("  -l <subscription id> (Show specific active subscription)")
        print("  -d (Delete all subscriptions)")
        print("  -d <subscription id> (Delete specific subscription)")
        print("  -s (Subscribe to research without download research reports)")
        print("  -s <subscription id> (Subscribe to research without download research reports)")
        print("  -sr (Subscribe to research and download pdf file into {}/ directory)".format(REPORTS_DIR_NAME))
        print("  -sr <subscription id> (Subscribe existing subscription and download pdf as default format file into {}/ directory)".format(REPORTS_DIR_NAME))
        print("  -srf <pdf/txt> (Subscribe to research and download pdf/text file into {}/ directory)".format(REPORTS_DIR_NAME))
        print("  -srf <subscription id> <pdf/txt> (Subscribe existing subscription and download pdf/text file into {}/ directory)".format(REPORTS_DIR_NAME))
        print("  -srt <pdf/text> (Subscribe to research and download signed url into {}/ directory)".format(REPORTS_DIR_NAME))
        print("  -srt <subscription id> <pdf/txt> (Subscribe existing subscription and download signed url into {}/ directory)".format(REPORTS_DIR_NAME))
    '''
