hdfs dfs -copyToLocal /data/msd/tasteprofile/mismatches/sid_mismatches_manually_accepted.txt ./sid_mismatches_manually_accepted.txt

hdfs dfs -copyToLocal /data/msd/tasteprofile/mismatches/sid_mismatches.txt ./sid_mismatches.txt

hdfs dfs -copyToLocal /data/msd/audio/features/ .

#DATA PROCESSING
#Question 1
#Question 1a
#file structure
hadoop fs -lsr /data/msd | awk '{print $8}' | \
sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'

 # |---audio
 # |-----attributes
 # |-------msd-jmir-area-of-moments-all-v1.0.attributes.csv
 # |-------msd-jmir-lpc-all-v1.0.attributes.csv
 # |-------msd-jmir-methods-of-moments-all-v1.0.attributes.csv
 # |-------msd-jmir-mfcc-all-v1.0.attributes.csv
 # |-------msd-jmir-spectral-all-all-v1.0.attributes.csv
 # |-------msd-jmir-spectral-derivatives-all-all-v1.0.attributes.csv
 # |-------msd-marsyas-timbral-v1.0.attributes.csv
 # |-------msd-mvd-v1.0.attributes.csv
 # |-------msd-rh-v1.0.attributes.csv
 # |-------msd-rp-v1.0.attributes.csv
 # |-------msd-ssd-v1.0.attributes.csv
 # |-------msd-trh-v1.0.attributes.csv
 # |-------msd-tssd-v1.0.attributes.csv
 # |-----features
 # |-------msd-jmir-area-of-moments-all-v1.0.csv
 # |---------part-00000.csv.gz
 # |---------part-00001.csv.gz
 # |---------part-00002.csv.gz
 # |---------part-00003.csv.gz
 # |---------part-00004.csv.gz
 # |---------part-00005.csv.gz
 # |---------part-00006.csv.gz
 # |---------part-00007.csv.gz
 # |-------msd-jmir-lpc-all-v1.0.csv
 # |---------part-00000.csv.gz
 # |---------part-00001.csv.gz
 # |---------part-00002.csv.gz
 # |---------part-00003.csv.gz
 # |---------part-00004.csv.gz
 # |---------part-00005.csv.gz
 # |---------part-00006.csv.gz
 # |---------part-00007.csv.gz
 # |-------msd-jmir-methods-of-moments-all-v1.0.csv
 # |---------part-00000.csv.gz
 # |---------part-00001.csv.gz
 # |---------part-00002.csv.gz
 # |---------part-00003.csv.gz
 # |---------part-00004.csv.gz
 # |---------part-00005.csv.gz
 # |---------part-00006.csv.gz
 # |---------part-00007.csv.gz
 # |-------msd-jmir-mfcc-all-v1.0.csv
 # |---------part-00000.csv.gz
 # |---------part-00001.csv.gz
 # |---------part-00002.csv.gz
 # |---------part-00003.csv.gz
 # |---------part-00004.csv.gz
 # |---------part-00005.csv.gz
 # |---------part-00006.csv.gz
 # |---------part-00007.csv.gz
 # |-------msd-jmir-spectral-all-all-v1.0.csv
 # |---------part-00000.csv.gz
 # |---------part-00001.csv.gz
 # |---------part-00002.csv.gz
 # |---------part-00003.csv.gz
 # |---------part-00004.csv.gz
 # |---------part-00005.csv.gz
 # |---------part-00006.csv.gz
 # |---------part-00007.csv.gz
 # |-------msd-jmir-spectral-derivatives-all-all-v1.0.csv
 # |---------part-00000.csv.gz
 # |---------part-00001.csv.gz
 # |---------part-00002.csv.gz
 # |---------part-00003.csv.gz
 # |---------part-00004.csv.gz
 # |---------part-00005.csv.gz
 # |---------part-00006.csv.gz
 # |---------part-00007.csv.gz
 # |-------msd-marsyas-timbral-v1.0.csv
 # |---------part-00000.csv.gz
 # |---------part-00001.csv.gz
 # |---------part-00002.csv.gz
 # |---------part-00003.csv.gz
 # |---------part-00004.csv.gz
 # |---------part-00005.csv.gz
 # |---------part-00006.csv.gz
 # |---------part-00007.csv.gz
 # |-------msd-mvd-v1.0.csv
 # |---------part-00000.csv.gz
 # |---------part-00001.csv.gz
 # |---------part-00002.csv.gz
 # |---------part-00003.csv.gz
 # |---------part-00004.csv.gz
 # |---------part-00005.csv.gz
 # |---------part-00006.csv.gz
 # |---------part-00007.csv.gz
 # |-------msd-rh-v1.0.csv
 # |---------part-00000.csv.gz
 # |---------part-00001.csv.gz
 # |---------part-00002.csv.gz
 # |---------part-00003.csv.gz
 # |---------part-00004.csv.gz
 # |---------part-00005.csv.gz
 # |---------part-00006.csv.gz
 # |---------part-00007.csv.gz
 # |-------msd-rp-v1.0.csv
 # |---------part-00000.csv.gz
 # |---------part-00001.csv.gz
 # |---------part-00002.csv.gz
 # |---------part-00003.csv.gz
 # |---------part-00004.csv.gz
 # |---------part-00005.csv.gz
 # |---------part-00006.csv.gz
 # |---------part-00007.csv.gz
 # |-------msd-ssd-v1.0.csv
 # |---------part-00000.csv.gz
 # |---------part-00001.csv.gz
 # |---------part-00002.csv.gz
 # |---------part-00003.csv.gz
 # |---------part-00004.csv.gz
 # |---------part-00005.csv.gz
 # |---------part-00006.csv.gz
 # |---------part-00007.csv.gz
 # |-------msd-trh-v1.0.csv
 # |---------part-00000.csv.gz
 # |---------part-00001.csv.gz
 # |---------part-00002.csv.gz
 # |---------part-00003.csv.gz
 # |---------part-00004.csv.gz
 # |---------part-00005.csv.gz
 # |---------part-00006.csv.gz
 # |---------part-00007.csv.gz
 # |-------msd-tssd-v1.0.csv
 # |---------part-00000.csv.gz
 # |---------part-00001.csv.gz
 # |---------part-00002.csv.gz
 # |---------part-00003.csv.gz
 # |---------part-00004.csv.gz
 # |---------part-00005.csv.gz
 # |---------part-00006.csv.gz
 # |---------part-00007.csv.gz
 # |-----statistics
 # |-------sample_properties.csv.gz
 # |---genre
 # |-----msd-MAGD-genreAssignment.tsv
 # |-----msd-MASD-styleAssignment.tsv
 # |-----msd-topMAGD-genreAssignment.tsv
 # |---main
 # |-----summary
 # |-------analysis.csv.gz
 # |-------metadata.csv.gz
 # |---tasteprofile
 # |-----mismatches
 # |-------sid_matches_manually_accepted.txt
 # |-------sid_mismatches.txt
 # |-----triplets.tsv
 # |-------part-00000.tsv.gz
 # |-------part-00001.tsv.gz
 # |-------part-00002.tsv.gz
 # |-------part-00003.tsv.gz
 # |-------part-00004.tsv.gz
 # |-------part-00005.tsv.gz
 # |-------part-00006.tsv.gz
 # |-------part-00007.tsv.gz

#file sizes in machine readable form
hdfs dfs -du /data/msd
# 13167872421  52671489684  /data/msd/audio
# 31585889     126343556    /data/msd/genre
# 182869445    731477780    /data/msd/main
# 514256719    2057026876   /data/msd/tasteprofile


#file sizes in human readable form
hdfs dfs -du -h /data/msd
# 12.3 G   49.1 G   /data/msd/audio
# 30.1 M   120.5 M  /data/msd/genre
# 174.4 M  697.6 M  /data/msd/main
# 490.4 M  1.9 G    /data/msd/tasteprofile

#file types
hdfs dfs -ls /data/msd
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:10 /data/msd/audio
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:17 /data/msd/genre
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:21 /data/msd/main
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:23 /data/msd/tasteprofile

#file formats and sizes and file type
hdfs dfs -lsr -h /data/msd/
#OUTPUT
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:10 /data/msd/audio
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 13:52 /data/msd/audio/attributes
# -rwxr-xr-x   4 hadoop supergroup      1.0 K 2019-05-06 13:52 /data/msd/audio/attributes/msd-jmir-area-of-moments-all-v1.0.attributes.csv
# -rwxr-xr-x   4 hadoop supergroup        671 2019-05-06 13:52 /data/msd/audio/attributes/msd-jmir-lpc-all-v1.0.attributes.csv
# -rwxr-xr-x   4 hadoop supergroup        484 2019-05-06 13:52 /data/msd/audio/attributes/msd-jmir-methods-of-moments-all-v1.0.attributes.csv
# -rwxr-xr-x   4 hadoop supergroup        898 2019-05-06 13:52 /data/msd/audio/attributes/msd-jmir-mfcc-all-v1.0.attributes.csv
# -rwxr-xr-x   4 hadoop supergroup        777 2019-05-06 13:52 /data/msd/audio/attributes/msd-jmir-spectral-all-all-v1.0.attributes.csv
# -rwxr-xr-x   4 hadoop supergroup        777 2019-05-06 13:52 /data/msd/audio/attributes/msd-jmir-spectral-derivatives-all-all-v1.0.attributes.csv
# -rwxr-xr-x   4 hadoop supergroup     12.0 K 2019-05-06 13:52 /data/msd/audio/attributes/msd-marsyas-timbral-v1.0.attributes.csv
# -rwxr-xr-x   4 hadoop supergroup      9.8 K 2019-05-06 13:52 /data/msd/audio/attributes/msd-mvd-v1.0.attributes.csv
# -rwxr-xr-x   4 hadoop supergroup      1.4 K 2019-05-06 13:52 /data/msd/audio/attributes/msd-rh-v1.0.attributes.csv
# -rwxr-xr-x   4 hadoop supergroup     34.1 K 2019-05-06 13:52 /data/msd/audio/attributes/msd-rp-v1.0.attributes.csv
# -rwxr-xr-x   4 hadoop supergroup      3.8 K 2019-05-06 13:52 /data/msd/audio/attributes/msd-ssd-v1.0.attributes.csv
# -rwxr-xr-x   4 hadoop supergroup      9.8 K 2019-05-06 13:52 /data/msd/audio/attributes/msd-trh-v1.0.attributes.csv
# -rwxr-xr-x   4 hadoop supergroup     27.6 K 2019-05-06 13:52 /data/msd/audio/attributes/msd-tssd-v1.0.attributes.csv
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:04 /data/msd/audio/features
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 13:52 /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv
# -rwxr-xr-x   4 hadoop supergroup      8.2 M 2019-05-06 13:52 /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00000.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      8.2 M 2019-05-06 13:52 /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00001.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      8.2 M 2019-05-06 13:52 /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00002.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      8.2 M 2019-05-06 13:52 /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00003.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      8.2 M 2019-05-06 13:52 /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00004.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      8.2 M 2019-05-06 13:52 /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00005.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      8.2 M 2019-05-06 13:52 /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00006.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      7.9 M 2019-05-06 13:52 /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00007.csv.gz
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv
# -rwxr-xr-x   4 hadoop supergroup      6.7 M 2019-05-06 13:52 /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00000.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.7 M 2019-05-06 13:52 /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00001.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.7 M 2019-05-06 13:52 /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00002.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.7 M 2019-05-06 13:52 /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00003.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.7 M 2019-05-06 13:52 /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00004.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.7 M 2019-05-06 13:52 /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00005.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.7 M 2019-05-06 13:52 /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00006.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.4 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00007.csv.gz
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv
# -rwxr-xr-x   4 hadoop supergroup      4.5 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00000.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      4.5 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00001.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      4.5 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00002.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      4.5 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00003.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      4.5 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00004.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      4.5 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00005.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      4.5 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00006.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      4.3 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00007.csv.gz
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv
# -rwxr-xr-x   4 hadoop supergroup      8.9 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00000.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      8.9 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00001.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      8.9 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00002.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      8.9 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00003.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      8.9 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00004.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      8.9 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00005.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      8.9 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00006.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      8.5 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00007.csv.gz
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv
# -rwxr-xr-x   4 hadoop supergroup      6.4 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00000.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.4 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00001.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.4 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00002.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.4 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00003.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.4 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00004.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.4 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00005.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.4 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00006.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.1 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00007.csv.gz
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv
# -rwxr-xr-x   4 hadoop supergroup      6.4 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00000.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.4 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00001.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.4 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00002.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.4 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00003.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.4 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00004.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.4 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00005.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.4 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00006.csv.gz
# -rwxr-xr-x   4 hadoop supergroup      6.1 M 2019-05-06 13:53 /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00007.csv.gz
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 13:53 /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv
# -rwxr-xr-x   4 hadoop supergroup     51.8 M 2019-05-06 13:53 /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00000.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     51.8 M 2019-05-06 13:53 /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00001.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     51.8 M 2019-05-06 13:53 /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00002.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     51.8 M 2019-05-06 13:53 /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00003.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     51.8 M 2019-05-06 13:53 /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00004.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     51.8 M 2019-05-06 13:53 /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00005.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     51.8 M 2019-05-06 13:53 /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00006.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     49.7 M 2019-05-06 13:53 /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00007.csv.gz
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 13:55 /data/msd/audio/features/msd-mvd-v1.0.csv
# -rwxr-xr-x   4 hadoop supergroup    165.9 M 2019-05-06 13:54 /data/msd/audio/features/msd-mvd-v1.0.csv/part-00000.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    166.0 M 2019-05-06 13:54 /data/msd/audio/features/msd-mvd-v1.0.csv/part-00001.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    165.9 M 2019-05-06 13:54 /data/msd/audio/features/msd-mvd-v1.0.csv/part-00002.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    165.9 M 2019-05-06 13:54 /data/msd/audio/features/msd-mvd-v1.0.csv/part-00003.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    166.0 M 2019-05-06 13:55 /data/msd/audio/features/msd-mvd-v1.0.csv/part-00004.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    165.9 M 2019-05-06 13:55 /data/msd/audio/features/msd-mvd-v1.0.csv/part-00005.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    166.0 M 2019-05-06 13:55 /data/msd/audio/features/msd-mvd-v1.0.csv/part-00006.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    158.3 M 2019-05-06 13:55 /data/msd/audio/features/msd-mvd-v1.0.csv/part-00007.csv.gz
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 13:56 /data/msd/audio/features/msd-rh-v1.0.csv
# -rwxr-xr-x   4 hadoop supergroup     30.2 M 2019-05-06 13:55 /data/msd/audio/features/msd-rh-v1.0.csv/part-00000.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     30.2 M 2019-05-06 13:55 /data/msd/audio/features/msd-rh-v1.0.csv/part-00001.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     30.2 M 2019-05-06 13:55 /data/msd/audio/features/msd-rh-v1.0.csv/part-00002.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     30.2 M 2019-05-06 13:55 /data/msd/audio/features/msd-rh-v1.0.csv/part-00003.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     30.2 M 2019-05-06 13:56 /data/msd/audio/features/msd-rh-v1.0.csv/part-00004.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     30.2 M 2019-05-06 13:56 /data/msd/audio/features/msd-rh-v1.0.csv/part-00005.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     30.2 M 2019-05-06 13:56 /data/msd/audio/features/msd-rh-v1.0.csv/part-00006.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     28.8 M 2019-05-06 13:56 /data/msd/audio/features/msd-rh-v1.0.csv/part-00007.csv.gz
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:01 /data/msd/audio/features/msd-rp-v1.0.csv
# -rwxr-xr-x   4 hadoop supergroup    518.9 M 2019-05-06 13:56 /data/msd/audio/features/msd-rp-v1.0.csv/part-00000.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    519.2 M 2019-05-06 13:57 /data/msd/audio/features/msd-rp-v1.0.csv/part-00001.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    519.0 M 2019-05-06 13:58 /data/msd/audio/features/msd-rp-v1.0.csv/part-00002.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    519.2 M 2019-05-06 13:59 /data/msd/audio/features/msd-rp-v1.0.csv/part-00003.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    519.2 M 2019-05-06 13:59 /data/msd/audio/features/msd-rp-v1.0.csv/part-00004.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    519.1 M 2019-05-06 14:00 /data/msd/audio/features/msd-rp-v1.0.csv/part-00005.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    519.1 M 2019-05-06 14:01 /data/msd/audio/features/msd-rp-v1.0.csv/part-00006.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    495.0 M 2019-05-06 14:01 /data/msd/audio/features/msd-rp-v1.0.csv/part-00007.csv.gz
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:02 /data/msd/audio/features/msd-ssd-v1.0.csv
# -rwxr-xr-x   4 hadoop supergroup     80.5 M 2019-05-06 14:02 /data/msd/audio/features/msd-ssd-v1.0.csv/part-00000.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     80.6 M 2019-05-06 14:02 /data/msd/audio/features/msd-ssd-v1.0.csv/part-00001.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     80.5 M 2019-05-06 14:02 /data/msd/audio/features/msd-ssd-v1.0.csv/part-00002.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     80.5 M 2019-05-06 14:02 /data/msd/audio/features/msd-ssd-v1.0.csv/part-00003.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     80.5 M 2019-05-06 14:02 /data/msd/audio/features/msd-ssd-v1.0.csv/part-00004.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     80.6 M 2019-05-06 14:02 /data/msd/audio/features/msd-ssd-v1.0.csv/part-00005.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     80.5 M 2019-05-06 14:02 /data/msd/audio/features/msd-ssd-v1.0.csv/part-00006.csv.gz
# -rwxr-xr-x   4 hadoop supergroup     76.8 M 2019-05-06 14:02 /data/msd/audio/features/msd-ssd-v1.0.csv/part-00007.csv.gz
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:04 /data/msd/audio/features/msd-trh-v1.0.csv
# -rwxr-xr-x   4 hadoop supergroup    185.4 M 2019-05-06 14:03 /data/msd/audio/features/msd-trh-v1.0.csv/part-00000.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    185.5 M 2019-05-06 14:03 /data/msd/audio/features/msd-trh-v1.0.csv/part-00001.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    185.4 M 2019-05-06 14:03 /data/msd/audio/features/msd-trh-v1.0.csv/part-00002.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    185.4 M 2019-05-06 14:03 /data/msd/audio/features/msd-trh-v1.0.csv/part-00003.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    185.4 M 2019-05-06 14:04 /data/msd/audio/features/msd-trh-v1.0.csv/part-00004.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    185.5 M 2019-05-06 14:04 /data/msd/audio/features/msd-trh-v1.0.csv/part-00005.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    185.5 M 2019-05-06 14:04 /data/msd/audio/features/msd-trh-v1.0.csv/part-00006.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    176.8 M 2019-05-06 14:04 /data/msd/audio/features/msd-trh-v1.0.csv/part-00007.csv.gz
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:10 /data/msd/audio/features/msd-tssd-v1.0.csv
# -rwxr-xr-x   4 hadoop supergroup    499.4 M 2019-05-06 14:05 /data/msd/audio/features/msd-tssd-v1.0.csv/part-00000.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    499.7 M 2019-05-06 14:06 /data/msd/audio/features/msd-tssd-v1.0.csv/part-00001.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    499.6 M 2019-05-06 14:06 /data/msd/audio/features/msd-tssd-v1.0.csv/part-00002.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    499.4 M 2019-05-06 14:07 /data/msd/audio/features/msd-tssd-v1.0.csv/part-00003.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    499.5 M 2019-05-06 14:08 /data/msd/audio/features/msd-tssd-v1.0.csv/part-00004.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    499.7 M 2019-05-06 14:09 /data/msd/audio/features/msd-tssd-v1.0.csv/part-00005.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    499.7 M 2019-05-06 14:09 /data/msd/audio/features/msd-tssd-v1.0.csv/part-00006.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    476.4 M 2019-05-06 14:10 /data/msd/audio/features/msd-tssd-v1.0.csv/part-00007.csv.gz
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:10 /data/msd/audio/statistics
# -rwxr-xr-x   4 hadoop supergroup     40.3 M 2019-05-06 14:10 /data/msd/audio/statistics/sample_properties.csv.gz
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:17 /data/msd/genre
# -rwxr-xr-x   4 hadoop supergroup     11.1 M 2019-05-06 14:17 /data/msd/genre/msd-MAGD-genreAssignment.tsv
# -rwxr-xr-x   4 hadoop supergroup      8.4 M 2019-05-06 14:17 /data/msd/genre/msd-MASD-styleAssignment.tsv
# -rwxr-xr-x   4 hadoop supergroup     10.6 M 2019-05-06 14:17 /data/msd/genre/msd-topMAGD-genreAssignment.tsv
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:21 /data/msd/main
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:22 /data/msd/main/summary
# -rwxr-xr-x   4 hadoop supergroup     55.9 M 2019-05-06 14:21 /data/msd/main/summary/analysis.csv.gz
# -rwxr-xr-x   4 hadoop supergroup    118.5 M 2019-05-06 14:22 /data/msd/main/summary/metadata.csv.gz
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:23 /data/msd/tasteprofile
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:23 /data/msd/tasteprofile/mismatches
# -rwxr-xr-x   4 hadoop supergroup     89.2 K 2019-05-06 14:23 /data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt
# -rwxr-xr-x   4 hadoop supergroup      1.9 M 2019-05-06 14:23 /data/msd/tasteprofile/mismatches/sid_mismatches.txt
# drwxr-xr-x   - hadoop supergroup          0 2019-05-06 14:23 /data/msd/tasteprofile/triplets.tsv
# -rwxr-xr-x   4 hadoop supergroup     61.1 M 2019-05-06 14:23 /data/msd/tasteprofile/triplets.tsv/part-00000.tsv.gz
# -rwxr-xr-x   4 hadoop supergroup     61.1 M 2019-05-06 14:23 /data/msd/tasteprofile/triplets.tsv/part-00001.tsv.gz
# -rwxr-xr-x   4 hadoop supergroup     61.1 M 2019-05-06 14:23 /data/msd/tasteprofile/triplets.tsv/part-00002.tsv.gz
# -rwxr-xr-x   4 hadoop supergroup     61.1 M 2019-05-06 14:23 /data/msd/tasteprofile/triplets.tsv/part-00003.tsv.gz
# -rwxr-xr-x   4 hadoop supergroup     61.0 M 2019-05-06 14:23 /data/msd/tasteprofile/triplets.tsv/part-00004.tsv.gz
# -rwxr-xr-x   4 hadoop supergroup     61.1 M 2019-05-06 14:23 /data/msd/tasteprofile/triplets.tsv/part-00005.tsv.gz
# -rwxr-xr-x   4 hadoop supergroup     61.1 M 2019-05-06 14:23 /data/msd/tasteprofile/triplets.tsv/part-00006.tsv.gz
# -rwxr-xr-x   4 hadoop supergroup     60.8 M 2019-05-06 14:23 /data/msd/tasteprofile/triplets.tsv/part-00007.tsv.gz

