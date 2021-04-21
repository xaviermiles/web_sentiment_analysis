from operator import itemgetter
import requests, zipfile, io, csv
# from pathos.pools import ProcessPool
import random
import os.path
import concurrent.futures

io.DEFAULT_BUFFER_SIZE = 12288*4

master_list = 'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt'
r = requests.get(master_list, stream=True)#, proxies=proxies)
with open('gdelt2_master.txt', mode='wb') as localfile:
    for line in r.iter_lines():
        if b'gkg.csv.zip' in line:
            localfile.write(line + b'\n')

# Read through the file line-by-line
header = ['gkg_id', 'date', 'source', 'source_name', 'doc_id', 'v1counts',
          'v2counts', 'v1themes', 'v2themes', 'v1locations', 'v2locations',
          'v1persons', 'v2persons', 'v1org', 'v2org', 'tone', 'mention_dates',
          'gcam', 'image1', 'image2', 'image3', 'video1', 'allnames',
          'amounts', 'translation', 'extra']

classification = ['c4.' + str(i) for i in range(1, 29)]
classification.sort()
codes = ['c3.1', 'c3.2', 'c41.1', 'c41.2', 'c41.3',
         'c6.4', 'c6.5', 'c6.6',
         'c7.1', 'c7.2', 
         'v10.1', 'v10.2', 'v11.1']
codes[2:2] = classification
codes_l = len(codes)

def process_gkg(file_name):
    processed = []
    csv_file = file_name.split('/')[-1][:-4]
    date = file_name.split('/')[-1][:4]
    print(date)
    if date == '2020':

        if os.path.exists('gdelt-2020-AU/' + csv_file): return
    
        print(csv_file)
        r = requests.get(file_name, stream=True)

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
                            if loc.split('#')[3] == 'AS':
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

                        # print(gcode, code_v, code_v == gcode)

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
                        with open('gdelt-2020-AU/' + csv_file, 'w+',
                                  encoding='latin-1') as f:
                            csvf = csv.writer(f, lineterminator = '\n')
                            csvf.writerows(processed)
                        
                    return(True)
        except: return(False)


# Import the master list
gkg_files = []
with open('gdelt2_master.txt') as f:
    for line in f:
        gkg_files.append(line.split(' ')[2][:-1])

# Randomly sort the list if needed
# gkg_files = gkg_files[::-1]
# random.shuffle(gkg_files)

if __name__ == '__main__':
    #for f in gkg_files[::-1]: process_gkg(f)
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        executor.map(process_gkg, gkg_files[::-1])

print('finished')
