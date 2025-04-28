import boto3

# S3 client and bucket
s3 = boto3.client('s3')
bucket = 'cyberark-sr-sre-candidate-bucket'
files = [
    '1f4881c1-a539-4297-b29c-561a16e497cb.txt',
    '1f2e42fa-21c7-4511-957a-7b396d8c30d0.txt',
    '1f2a1a1a-eb27-4cf6-a1f2-f540498f868b.txt',
    '1eeddb9b-a0ef-44f8-b267-4a2a48c9fac9.txt'
]

try:
    for key in files:
        # Get versions
        versions = s3.list_object_versions(Bucket=bucket, Prefix=key).get('Versions', [])

        if len(versions) < 2:
            print(f"{key}: Less than 2 versions, skipping.")
            continue

        # Latest and previous versions
        latest, previous = sorted(versions, key=lambda x: x['LastModified'], reverse=True)[:2]

        # Compare ETags
        if latest['ETag'] == previous['ETag']:
            s3.delete_object(Bucket=bucket, Key=key, VersionId=previous['VersionId'])
            print(f"{key}: Deleted previous version {previous['VersionId']} (identical).")
        else:
            print(f"{key}: Versions differ, keeping both.")

except Exception as e:
    print(f"Error: {e}")