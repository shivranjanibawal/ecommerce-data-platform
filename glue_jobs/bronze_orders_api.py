import sys, boto3, requests, json
from datetime import date
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['BUCKET', 'API_URL', 'API_KEY'])

s3 = boto3.client('s3')
page, results = 1, []

while True:
    resp = requests.get(
        args['API_URL'],
        params={'page': page, 'page_size': 1000, 'date': str(date.today())},
        headers={'Authorization': f"Bearer {args['API_KEY']}"}
    )
    data = resp.json()
    results.extend(data['orders'])
    if not data.get('next_page'):
        break
    page += 1

# Partition by date for efficient querying later
key = f"orders/year={date.today().year}/month={date.today().month:02d}/day={date.today().day:02d}/orders.json"

s3.put_object(
    Bucket=args['BUCKET'],
    Key=key,
    Body=json.dumps(results).encode('utf-8')
)
print(f"Ingested {len(results)} orders to s3://{args['BUCKET']}/{key}")