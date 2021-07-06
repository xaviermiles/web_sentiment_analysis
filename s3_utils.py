import botocore, boto3


# Querying
def is_s3_key_valid(bucket, key, profile):
    """
    Return boolean indicating whether given s3 key is valid for the given s3 
    bucket. A "valid s3 key" means that there is a file saved with that
    key in the given bucket
    """
    s3_client = boto3.Session(profile_name=profile).client('s3') 
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
            return False  # object does not exist
        else:
            raise e
            

def list_all_objects_s3(bucket, prefix, profile):
    """
    Necessary since the list_objects_v2() function only lists the first 1000 
    objects, and requires a continuation token to get the next 1000 objects.
    """
    s3 = boto3.Session(profile_name=profile).client('s3')
    keys = []
    truncated = True
    continuation_token = ""

    while truncated:
        list_kwargs = dict(Bucket=bucket, Prefix=prefix)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        resp = s3.list_objects_v2(**list_kwargs)
        keys += [x['Key'] for x in resp['Contents']]

        truncated = resp['IsTruncated']
        if truncated:
            continuation_token = resp['NextContinuationToken']
            
    return keys
            

# Reading/writing
def write_to_file_s3(txt_list, bucket, key, profile):
    """
    txtlist: list[str], where each element corresponds to a line in the txt
        file.
    """
    s3_client = boto3.Session(profile_name=profile).client('s3')
    txt_str = '\n'.join(txt_list) + '\n'
    s3_client.put_object(Body=txt_str, Bucket=bucket, Key=key)
    
            
def write_to_csv_s3(csv_list, bucket, key, profile):
    """
    csv_list: list[list[?]], where each inner list will correspond to a row in 
        the CSV file. (The elements of the inner lists will joined by commas 
        and the inner lists will then be joined by newlines.)
    """
    # double-quotes are used to enclose fields, so any double-quotes are 
    # changed to single-quotes
    csvlist_str_elements = [
        ['"' + str(x).replace('"', "'") + '"' for x in row]
        for row in csv_list
    ]
    
    csv_lines = [','.join(row) for row in csvlist_str_elements]
    write_to_txt_s3(csv_rows, bucket, key, csv_lines)
    
    
def read_file_from_s3(bucket, key, profile):
    """
    Returns a list[str] which contains 
    """
    s3_client = boto3.Session(profile_name=profile).client('s3')
    resp = s3_client.get_object(Bucket=bucket, Key=key)
    # Python 3.8/3.9 can't download files over 2GB via HTTP, so file is 
    # streamed in chunks just in case
    content = ''.join([
        chunk.decode() for chunk in resp['Body'].iter_chunks()
    ])
    
    return content