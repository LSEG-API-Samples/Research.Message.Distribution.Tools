# =============================================================================
# Refinitiv Data Platform demo app to convert instrument symbology
#-----------------------------------------------------------------------------
#   This source code is provided under the Apache 2.0 license
#   and is provided AS IS with no warranty or guarantee of fit for purpose.
#   Copyright (C) 2021 Refinitiv. All rights reserved.
# =============================================================================
import requests
import json
import rdpToken

# Application Constants
RDP_version = "/v1"
base_URL = "https://api.refinitiv.com"
category_URL = "/discovery/symbology"
endpoint_URL = "/lookup"



#==============================================
def convertSymbology(requestData):
#==============================================
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL
	dResp = requests.post(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken}, data=json.dumps(requestData))
	if dResp.status_code != 200:
		print("Unable to get data. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		# Display data
		jResp = json.loads(dResp.text)
		print(json.dumps(jResp, indent=2))



#==============================================
if __name__ == "__main__":
#==============================================
	# Get latest access token
	print("Getting OAuth access token...")
	accessToken = rdpToken.getToken()
	print("Invoking symbology conversion requests")

	identifier_to_organization_PermID = {
		"from": [
			{
				"identifierTypes": ["RIC"],
				"values": ["IBM.N", "TRI.TO"]
			}
		],
		"to": [
			{
				"objectTypes": ["organization"],
				"identifierTypes": ["PermID"]
			}
		],
		"type": "auto"
	}

	print("<identifier_to_organization_PermID>")
	convertSymbology(identifier_to_organization_PermID)


	identifier_to_instrument_PermID = {
		"from": [
			{
				"identifierTypes": ["RIC"],
				"values": ["IBM.N", "TRI.TO"]
			}
		],
		"to": [
			{
				"objectTypes": ["anyinstrument"],
				"identifierTypes": ["PermID"]
			}
		],
		"type": "auto"
	}
	print("<identifier_to_instrument_PermID>")
	convertSymbology(identifier_to_instrument_PermID)

	identifier_to_quote_PermID = {
		"from": [
			{
				"identifierTypes": ["RIC"],
				"values": ["IBM.N", "TRI.TO"]
			}
		],
		"to": [
			{
				"objectTypes": ["anyquote"],
				"identifierTypes": ["PermID"]
			}
		],
		"type": "auto"
	}
	print("<identifier_to_quote_PermID>")
	convertSymbology(identifier_to_quote_PermID)

	identifier_to_identifier = {
		"from": [
			{
				"identifierTypes": ["ISIN"],
				"values": ["US30303M1027", "US0231351067"]
			}
		],
		"to": [
			{
				"identifierTypes": ["LEI"]
			}
		],
		"type": "auto"
	}
	print("<identifier_to_identifier>")
	convertSymbology(identifier_to_identifier)

	identifier_to_multiple_identifiers = {
		"from": [
			{
				"identifierTypes": ["RIC"],
				"values": ["DPWGn.DE"]
			}
		],
		"to": [
			{
				"identifierTypes": ["ISIN", "LEI", "ExchangeTicker"]
			}
		],
		"type": "auto"
	}
	print("<identifier_to_multiple_identifiers>")
	convertSymbology(identifier_to_multiple_identifiers)

	identifier_to_all_identifiers_for_the_entity = {
		"from": [
			{
				"identifierTypes": [
					"ISIN"
				],
				"values": [
					"US4592001014"
				]
			}
		],
		"to": [
			{
				"identifierTypes": ["ANY"]
			}
		],
		"type": "strict"
	}
	print("<identifier_to_all_identifiers_for_the_entity>")
	convertSymbology(identifier_to_all_identifiers_for_the_entity)

	organization_identifier_to_primary_equity_RIC = {
		"from": [
			{
				"identifierTypes": ["LEI"],
				"values": ["549300561UZND4C7B569"]
			}
		],
		"to": [
			{
				"identifierTypes": ["RIC"]
			}
		],
		"path": [
			{
				"relationshipTypes": ["InverseIsPrimarySecurityOf"],
				"objectTypes": [
					{
						"from": "Organization",
						"to": "AnyInstrument"
					}
				]
			},
			{
				"relationshipTypes": ["InverseIsValuationQuoteOf"],
				"objectTypes": [
					{
						"from": "AnyInstrument",
						"to": "AnyQuote"
					}
				]
			}
		],
		"type": "strict"
	}
	print("<organization_identifier_to_primary_equity_RIC>")
	convertSymbology(
		organization_identifier_to_primary_equity_RIC)

	equity_instrument_to_primary_RIC = {
		"from": [
			{
				"identifierTypes": ["ISIN"],
				"values": ["CA8849037095"]
			}
		],
		"to": [
			{
				"identifierTypes": ["RIC"]
			}
		],
		"path": [
			{
				"relationshipTypes": [
					"InverseIsValuationQuoteOf"
				],
				"objectTypes": [
					{
						"from": "AnyInstrument",
						"to": "AnyQuote"
					}
				]
			}
		],
		"type": "strict"
	}
	print("<equity_instrument_to_primary_RIC>")
	convertSymbology(equity_instrument_to_primary_RIC)

	SEDOL_to_primary_equity_RIC = {
		"from": [
			{
				"identifierTypes": ["Sedol"],
				"values": ["2005973"]
			}
		],
		"to": [
			{
				"identifierTypes": ["RIC"]
			}
		],
		"path": [
			{
				"relationshipTypes": [
					"IsQuoteOf"
				],
				"objectTypes": [
					{
						"from": "AnyQuote",
						"to": "AnyInstrument"
					}
				]
			},
			{
				"relationshipTypes": ["InverseIsValuationQuoteOf"],
				"objectTypes": [
					{
						"from": "AnyInstrument",
						"to": "AnyQuote"
					}
				]
			}
		],
		"type": "strict"
	}
	print("<SEDOL_to_primary_equity_RIC>")
	convertSymbology(SEDOL_to_primary_equity_RIC)
