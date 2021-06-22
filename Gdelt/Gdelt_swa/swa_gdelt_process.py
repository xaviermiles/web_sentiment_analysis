import itertools
from operator import itemgetter
import requests, zipfile, io, csv
# from pathos.pools import ProcessPool
import random
import os.path
import concurrent.futures
from functools import partial
import boto3
import botocore

io.DEFAULT_BUFFER_SIZE = 8192*4

master_list = 'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt'
r = requests.get(master_list, stream=True)#, proxies=proxies)
with open('gdelt2_master.txt', mode='wb') as localfile:
    for line in r.iter_lines():
        if b'gkg.csv.zip' in line:
            localfile.write(line + b'\n')


# classification = ['c4.' + str(i) for i in range(1, 29)]
# classification.sort()
codes = ['c3.1', 'c3.2']#, 'c41.1', 'c41.2', 'c41.3',                                        # removed all features just to reduce the data size for processing
#          'c6.4', 'c6.5', 'c6.6',
#          'c7.1', 'c7.2', 'c8.1', 'c8.2', 'c8.3', 'c8.4', 'c8.5', 'c8.6', 'c8.7',
#          'v10.1', 'v10.2', 'v11.1']
# codes[2:2] = classification
codes_l = len(codes)

session = boto3.Session(profile_name='kandavar_processing')
s3 = session.client('s3')
bucket_name = 'statsnz-covid-kandavar'
    

def process_gkg(file_name):

    processed = []
    csv_file = file_name.split('/')[-1][:-4]
    date = file_name.split('/')[-1][:4]
    Prefix='G_from_2015/au/'                                   # change the country accordingly 
    
    if is_s3_key_valid(bucket_name, Prefix+csv_file): return

    r = requests.get(file_name, stream=True)
    print(csv_file)


    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            with zf.open(csv_file, 'r') as infile:
                for raw in io.TextIOWrapper(infile, encoding='latin-1'):


                    line = raw.split('\t')
                    if len(line) < 10: continue

                # Keep stories which reference NZ as a location
                    locs = line[9].split(';')
                    inc_nz = False
                    if locs == ['']: continue
                    for loc in locs:
                        if loc.split('#')[3] == 'AS':             # add country of choice eg: for Australia it is 'AS'
                            inc_nz = True
                            break
                    if not inc_nz: continue

                # Extract the relevant codes
                    gcam = line[17].split(',')
                    code_i = 0
                    code_v = codes[code_i]
                    code_l = len(code_v)
                    out = []
                    for el in gcam[1:]:

                        gcode, val = el.split(':')


                        while code_v < gcode:
                            out.append('')
                            code_i += 1
                            if (code_i == codes_l - 1): break
                            code_v = codes[code_i]

                        if code_v == gcode:
                            out.append(val)
                            code_i += 1
                            if (code_i == codes_l - 1): break
                            code_v = codes[code_i]

                    if (len(codes) == len(out) + 1): out.append('')


                # Extract the important information
                    out[0:0] = line[15].split(',')
                    out[0:0] = itemgetter(0, 1, 2, 3, 4, 7, 9, 11, 13)(line)
                    processed.append(out)
                if len(processed) > 0:
                     write_to_csv_s3(processed, "statsnz-covid-kandavar", csv_file)

                return(True)

    except: return(False)
    

def is_s3_key_valid(bucket, key):
    """
    Return boolean indicating whether given s3 key is valid for the given s3
    bucket. A "valid s3 key" means that there is a file saved with that
    key in the given bucket
    """
    s3_client = boto3.Session(profile_name="kandavar_processing").client('s3')
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
            return False  # object does not exist
        else:
            raise e    
    
    
    
    
def write_to_csv_s3(csv_list, bucket, csv_file):
    """
    outlist: list[list[?]], where each inner list will correspond to a row in the
        CSV file. (The elements of the inner lists will joined by commas and 
        the inner lists will then be joined by newlines.)
    """

    csvlist_str_elements = [
        ['"' + str(x).replace('"', "'") + '"' for x in row]
        for row in csv_list
    ]
    
    csv_rows = [','.join(row) for row in csvlist_str_elements]
    csv_str = '\n'.join(csv_rows) + '\n'
    key = 'G_from_2015/au/'+csv_file                                            # change the country to the respective folder eg for canada it is 'ca'
    s3.put_object(Body=csv_str, Bucket=bucket, Key=key)


    

# Import the master list
gkg_files = []
with open('gdelt2_master.txt') as f:
    for line in f:
        gkg_files.append(line.split(' ')[2][:-1])


if __name__ == '__main__':

#     for f in gkg_files[200000:200019]: process_gkg(f)
    with concurrent.futures.ThreadPoolExecutor(max_workers=35) as executor:
        executor.map(process_gkg, gkg_files[::-1])

print('finished')
