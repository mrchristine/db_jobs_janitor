import boto3
import json
from botocore.exceptions import ClientError
import datetime, pytz

# This must be verified by SES
SENDER = "mwc+automation@databricks.com"
# This will be updated after reading payload
AWS_REGION = "us-east-1"

def get_current_date_pt():
  # get the current UTC time and do not depend on the TZ of where this is run
  u = datetime.datetime.utcnow()
  u = u.replace(tzinfo=pytz.utc)
  return u.astimezone(pytz.timezone("America/Los_Angeles")).strftime("%Y-%m-%d")

def log_to_s3(bucket_name, folder, contents):
  date_str = get_current_date_pt()
  runtime_str = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
  fname = "cluster_cleanup_{}.json".format(runtime_str)
  full_fname = "{0}/date={1}/{2}".format(folder, date_str, fname)
  s3 = boto3.resource('s3')
  # creating a json file and putting it in the folder
  s3.Object(bucket_name, full_fname).put(Body=json.dumps(contents))

# Helper to pretty print json
def pprint_j(i):
  return json.dumps(i, indent=4, sort_keys=True)

def get_html(report):
  # report is a json object
  html = "<h2> Usage Info for {0} </h2>".format(report['env_name'][0])
  html += """
    <figure>
      <figcaption>Usage Data</figcaption>
      <pre><code>{0}</code></pre>
    </figure>
    """.format(pprint_j(report))
  return html

def respond(err, res=None):
  return {
    'statusCode' : '400' if err else '200',
    'body' : err.response["Error"]["Message"] if err else json.dumps(res),
    'headers' : {
      'Content-Type' : 'application/json'
    }
  }
  
def send_email(subject, recipient, body_text, body_html):
    charset = "UTF-8"    
    client = boto3.client('ses', region_name=AWS_REGION)
    try:
      response = client.send_email(
        Destination={
          'ToAddresses' : recipient,
          },
        Message={
          'Body': {
            'Html' : {
              'Charset' : charset,
              'Data' : body_html
            },
            'Text' : {
              'Charset' : charset,
              'Data' : body_text
            }
          },
          'Subject' : {
            'Charset' : charset,
            'Data' : subject
          }
        },
        Source=SENDER
        )
    except ClientError as e:
      print(e.response["Error"]["Message"])
      respond(e, 'Error')
    

