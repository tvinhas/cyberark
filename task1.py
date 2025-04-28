import boto3
import re # needed for regex
from collections import Counter # used to initialize the counts as 0

# S3 client and bucket
s3 = boto3.client('s3')
bucket = 'cyberark-sr-sre-candidate-bucket'

# Counter and regex
counts = Counter()
unassigned_re = re.compile(r'^unassigned_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.txt$')
assigned_re = re.compile(r'^assigned_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.txt$')

try:
    # list S3 objects
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.txt'):
                file_name = obj['Key'].split('/')[-1]
                counts['unassigned' if unassigned_re.match(file_name) else
                'assigned' if assigned_re.match(file_name) else
                'random'] += 1
                counts['over_5kb'] += obj['Size'] > 5120
                counts['over_15kb'] += obj['Size'] > 15360

    # Print results
    for key in ['unassigned', 'assigned', 'random', 'over_5kb', 'over_15kb']:
        print(f"{key.replace('_', ' ').title()}: {counts[key]}")

except Exception as e:
    print(f"Error: {e}")