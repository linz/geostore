"""
Example S3 service test.
"""

import boto3

def test_s3_bucket_creation():
    """Test S3 bucket creation."""
    s3 = boto3.resource('s3')
    assert len(list(s3.buckets.all())) == 0
    bucket = s3.Bucket('foobar')
    bucket.create()
    assert len(list(s3.buckets.all())) == 1
