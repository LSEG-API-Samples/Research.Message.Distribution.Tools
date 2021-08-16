#=============================================================================
# Refinitiv Data Platform demo app to subscribe to NEWS messages
#-----------------------------------------------------------------------------
#   This source code is provided under the Apache 2.0 license
#   and is provided AS IS with no warranty or guarantee of fit for purpose.
#   Copyright (C) 2021 Refinitiv. All rights reserved.
#=============================================================================
import requests
import json
import rdpToken
import sqsQueue
import atexit
import sys
import boto3
from botocore.exceptions import ClientError

# Application Constants
base_URL = "https://api.refinitiv.com"
RDP_version = "/v1"
category_URL = "/message-services"
endpoint_URL_headlines = "/news-headlines/subscriptions"
endpoint_URL_stories = "/news-stories/subscriptions"

currentSubscriptionID = None
gHeadlines = True

#==============================================
def subscribeToNews():
#==============================================
	if gHeadlines:
		RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL_headlines
	else:
		RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL_stories

	requestData = {
		"transport": {
			"transportType": "AWS-SQS"
		},
		"payloadVersion": "2.0"
	}

	# Optional filters can be applied to subscription. For e.g. following gets the english language top news only
	#requestData = {
	#	"transport": {
	#		"transportType": "AWS-SQS"
	#	},
	#	"filter": {
	#		"query": {
	#			"type": "operator",
	#			"operator": "and",
	#			"operands": [{
	#					"type": "freetext",
	#					"match": "contains",
	#					"value": "TOP NEWS"
	#				}, {
	#					"type": "language",
	#					"value": "L:en"
	#				}
	#			]
	#		}
	#	},
	#	"payloadVersion": "2.0"
	#}

	# get the latest access token
	accessToken = rdpToken.getToken()
	hdrs = {
		"Authorization": "Bearer " + accessToken,
		"Content-Type": "application/json"
	}

	dResp = requests.post(RESOURCE_ENDPOINT, headers = hdrs, data = json.dumps(requestData))
	if dResp.status_code != 200:
		raise ValueError("Unable to subscribe. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		jResp = json.loads(dResp.text)
		return jResp["transportInfo"]["endpoint"], jResp["transportInfo"]["cryptographyKey"], jResp["subscriptionID"]


#==============================================
def getCloudCredentials(endpoint):
#==============================================
	CC_category_URL = "/auth/cloud-credentials"
	CC_endpoint_URL = "/"
	RESOURCE_ENDPOINT = base_URL + CC_category_URL + RDP_version + CC_endpoint_URL
	requestData = {
		"endpoint": endpoint
	}

	# get the latest access token
	accessToken = rdpToken.getToken()
	dResp = requests.get(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken}, params = requestData)
	if dResp.status_code != 200:
		raise ValueError("Unable to get credentials. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		jResp = json.loads(dResp.text)
		return jResp["credentials"]["accessKeyId"], jResp["credentials"]["secretKey"], jResp["credentials"]["sessionToken"]


#==============================================
def startNewsMessages(headlines = True):
#==============================================
	global currentSubscriptionID
	global gHeadlines
	try:
		gHeadlines = headlines
		if gHeadlines:
			print("Subscribing to news headline messages...")
		else:
			print("Subscribing to news stories messages...")

		endpoint, cryptographyKey, currentSubscriptionID = subscribeToNews()
		print("  Queue endpoint: %s" % (endpoint) )
		print("  Subscription ID: %s" % (currentSubscriptionID) )

		# unsubscribe before shutting down
		atexit.register(removeSubscription)

		while 1:
			try:
				print("Getting credentials to connect to AWS Queue...")
				accessID, secretKey, sessionToken = getCloudCredentials(endpoint)
				print("Queue access ID: %s" % (accessID) )
				print("Getting news, press BREAK to exit and delete subscription...")
				sqsQueue.startPolling(accessID, secretKey, sessionToken, endpoint, cryptographyKey)
			except ClientError as e:
				print("Cloud credentials exprired!")
	except KeyboardInterrupt:
		print("User requested break, cleaning up...")
		sys.exit(0)


#==============================================
def removeSubscription():
#==============================================
	if gHeadlines:
		RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL_headlines
	else:
		RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL_stories

	# get the latest access token
	accessToken = rdpToken.getToken()

	if currentSubscriptionID:
		print("Deleting the open subscription")
		dResp = requests.delete(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken}, params = {"subscriptionID": currentSubscriptionID})
	else:
		print("Deleting ALL open headline and stories subscription")
		dResp = requests.delete(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken})

	if dResp.status_code > 299:
		print(dResp)
		print("Warning: unable to remove subscription. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		print("News messages unsubscribed!")


#==============================================
def showActiveSubscriptions():
#==============================================
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version

	# get the latest access token
	accessToken = rdpToken.getToken()

	print("Getting all open headlines subscriptions")
	dResp = requests.get(RESOURCE_ENDPOINT + endpoint_URL_headlines, headers = {"Authorization": "Bearer " + accessToken})

	if dResp.status_code != 200:
		raise ValueError("Unable to get subscriptions. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		jResp = json.loads(dResp.text)
		print(json.dumps(jResp, indent=2))

	print("Getting all open stories subscriptions")
	dResp = requests.get(RESOURCE_ENDPOINT + endpoint_URL_stories, headers = {"Authorization": "Bearer " + accessToken})

	if dResp.status_code != 200:
		raise ValueError("Unable to get subscriptions. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		jResp = json.loads(dResp.text)
		print(json.dumps(jResp, indent=2))



#==============================================
if __name__ == "__main__":
#==============================================
	if len(sys.argv) > 1:
		if sys.argv[1] == '-l':
			showActiveSubscriptions()
		elif sys.argv[1] == '-d':
			removeSubscription()
		elif sys.argv[1] == '-h':
			startNewsMessages()
		elif sys.argv[1] == '-s':
			startNewsMessages(headlines = False)
	else:
		print("Arguments:")
		print("  -l List active subscriptions")
		print("  -d Delete all subscriptions")
		print("  -h Subscribe to news headlines")
		print("  -s Subscribe to news stories")
