from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

dynamodb = boto3.resource("dynamodb",endpoint_url="http://localhost:8000")

table = dynamodb.Table('Movies')

title = "The Big New Movie"
year = 2015

try:
    table.put_item(
        Item={
            'year': year,
            'title': title,
            'info':{
                'plot':"Nothing happens at all.",
                'rating': decimal.Decimal(0)
            }
        },
        ConditionExpression=Attr("year").ne(year) & Attr("title").ne(title)
    )
except ClientError as e:
    if e.response['Error']['Code'] == "ConditionalCheckFailedException":
        print(e.response['Error']['Message'])
    else:
        raise
else:
    print("PutItem succeeded:")

response = table.get_item(
    Key={
        'year': year,
        'title': title
    }
)
item = response['Item']

print(json.dumps(item, indent=4, cls=DecimalEncoder))
