import argparse
import sys
import os
import json
from sodapy import Socrata
import requests
from requests.auth import HTTPBasicAuth


DATASET_ID = os.environ["DATASET_ID"]
APP_TOKEN = os.environ["APP_TOKEN"]
ES_HOST = os.environ["ES_HOST"]
INDEX_NAME = os.environ["INDEX_NAME"]
ES_USERNAME = os.environ["ES_USERNAME"]
ES_PASSWORD = os.environ["ES_PASSWORD"]


parser =argparse.ArgumentParser(description = "Process data opacv")
parser.add_argument("--page_size", type=int, help="How many rows to fetch per page", required=True)
parser.add_argument("--num_pages", type=int, help="How many pages to fetch", required=True)

args = parser.parse_args(sys.argv[1:])
print(args)


parser = argparse.ArgumentParser(description='Process data from OpenDataNYC')
parser.add_argument('--page_size', help='Enter the number of records per page', type=int, required=True)
parser.add_argument('--num_pages', help='Enter the number of pages to fetch', type=int)
args = parser.parse_args()


if __name__ == '__main__':
	try:
		resp = requests.get(ES_HOST, auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD))
		resp = requests.put(
			f"{ES_HOST}/{INDEX_NAME}",
			auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
			json={
				 "settings": {
					"number_of_shards": 1,
					"number_of_replicas":1,
				 },
				"mappings": {
					"properties": {
						"plate": { "type": "keyword" },
						"state": { "type": "keyword" },
						"license_type": { "type": "keyword" },
						"summons_number": { "type": "text" },
						"issue_date": { "type": "date", "format": "MM/dd/yyyy" },
						"violation_time": { "type": "text" },
						"violation": { "type": "keyword" },
						"fine_amount": { "type": "float" },
						"penalty_amount": { "type": "float" },
						"interest_amount": { "type": "float" },
						"reduction_amount": { "type": "float" },
						"payment_amount": { "type": "float" },
						"amount_due": { "type": "float" },
						"precinct": { "type": "keyword" },
						"county": { "type": "keyword" },
						"issuing_agency": { "type": "keyword" },
						"violation_status": { "type": "keyword" }
					}
				},
			}
		)
		resp.raise_for_status()
	except Exception as e:
		print("Index already exist!, Skipping")
	
	
	for i in range(0, args.num_pages):
		es_rows=[]
		rows = Socrata("data.cityofnewyork.us", APP_TOKEN).get(DATASET_ID, limit=args.page_size, offset=i*(args.page_size), order = "summons_number")
		for row in rows:
			try:
				es_row={}
				es_row["plate"] = row["plate"]
				es_row["state"] = row["state"]
				es_row["license_type"] = row["license_type"]
				es_row["summons_number"] = row["summons_number"]
				es_row["issue_date"] = row["issue_date"]
				if "violation" in row:
					es_row["violation"] = row["violation"]
				else:
					es_row["violation"] = "N/A"
				if "fine_amount" in row:
					es_row["fine_amount"] = float(row["fine_amount"])
				else:
					es_row["fine_amount"] = None
				if "penalty_amount" in row:
					es_row["penalty_amount"] = float(row["penalty_amount"])
				else:
					es_row["penalty_amount"] = None
				if "interest_amount" in row:
					es_row["interest_amount"] = float(row["interest_amount"])
				else:
					es_row["interest_amount"] = None
				if "reduction_amount" in row:
					es_row["reduction_amount"] = float(row["reduction_amount"])
				else:
					es_row["reduction_amount"] = None
				if "payment_amount" in row:
					es_row["payment_amount"] = float(row["payment_amount"])
				else:
					es_row["payment_amount"] = None
				if "amount_due" in row:
					es_row["amount_due"] = float(row["amount_due"])
				else:
					es_row["amount_due"] = None
				if "precinct" in row:    
					es_row["precinct"] = row["precinct"]
				else:
					es_row["precinct"] = "N/A"
				if "county" in row:
					es_row["county"] = row["county"]
				else:
					es_row["county"] = "N/A"
				if "issuing_agency" in row:
					es_row["issuing_agency"] = row["issuing_agency"]
				else:
					es_row["issuing_agency"] = "N/A"
				if "violation_status" in row:
					es_row["violation_status"] = row["violation_status"]
				else:
					es_row["violation_status"] = "N/A"
				print(es_row)
				
			except Exception as e:
				print(f"Error!: {e}, Skipping row: {row}")
				continue
			
			es_rows.append(es_row)
			
		bulk_upload_data = ""  
		for line in es_rows:
			print(f'Handling row {line["summons_number"]}')
			
			action = '{"index": {"_index": "' + INDEX_NAME + '", "_type": "_doc", "_id": "' + line["summons_number"] + '"}}'
			data = json.dumps(line)
			bulk_upload_data += f"{action}\n"
			bulk_upload_data += f"{data}\n"
			
		try:
			resp = requests.post(f"{ES_HOST}/_bulk", auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD), headers = {"Content-type": "application/x-ndjson"}, data=bulk_upload_data)
			resp.raise_for_status()
			print('Done')
		except Exception as e:
			print(f"Failed to upload to elasticsearch!: {e}", "skipping row: {row}")