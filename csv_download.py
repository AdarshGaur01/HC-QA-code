import os
import time
# import sys
# import warnings
import time

from datetime import date, timedelta

from google.cloud import storage
from google.cloud.storage import transfer_manager



os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(os.getcwd(), 'hc-data-prod-scrape-bb2329b89ea9.json')
BQ_PROJECT_ID = 'hc-data-prod-scrape'

storage_client = storage.Client()


CSV_PROD_BUCKET = '939239264b-hc-csv-parsed-share-prod'


def download_many_blobs_with_transfer_manager(blob_file_pairs, blob_names) -> str:
    """Downloads a blob from the bucket to filename."""
    results = transfer_manager.download_many(
        blob_file_pairs=blob_file_pairs,
        max_workers=10
    )

    for result, tup in zip(results, blob_file_pairs):
        if not isinstance(result, Exception):
            print("Downloaded {} to {}.".format(tup[1].split('/')[-1], tup[1]))



def main():
    ...
    # get the date of last sunday
    today = date.today()
    last_sunday = today - timedelta(days=today.weekday()+1)
    last_sunday_month = last_sunday.strftime("%m")
    last_sunday_year = last_sunday.strftime("%Y")
    last_sunday_nodash = last_sunday.strftime("%Y%m%d")
    print(last_sunday)


    file_version = 'v20230217'
    book_types = ['ebook', 'print']
    regions = ['US', 'UK', 'CA', 'AU', 'DE', 'FR']
    isbn_owner = ['HCCP', 'HCCA', 'HCAU', 'HCUK', 'HCUS', 'HQN','COMP',]

    # create a folder for the last sunday in cwd
    print("Creating directories ...")
    current_dir = os.getcwd()
    last_sunday_dir = os.path.join(current_dir, last_sunday_nodash)
    if not os.path.exists(last_sunday_dir):
        os.mkdir(last_sunday_dir)

    # create a folder in last sunday folder for book types
    for book_type in book_types:
        if not os.path.exists(os.path.join(last_sunday_dir, book_type)):
            os.mkdir(os.path.join(last_sunday_dir, book_type))
    
    

    bucket = storage_client.bucket(CSV_PROD_BUCKET)

    for freq in ['daily',]:
        blob_names = []
        blob_file_pair = []
        for book_type in book_types:
            for region in regions:
                if region != 'UK' and region !='US' and book_type == 'print':
                    continue
                for owner in isbn_owner:
                    if owner == 'COMP' and region !='US' and region!='UK':
                        continue
                    if book_type == 'print' :
                        if region != 'US' and region != 'UK':
                            continue
                        if owner == 'HCCA' or owner == 'HCAU':
                            continue
                        if region == 'UK' and owner != 'HCUK':
                            continue
                    path = f"{last_sunday_year}/{last_sunday_month}/{freq}/{book_type}/"
                    file_name = f"{freq[0].capitalize()}-{book_type[0].capitalize()}-{owner}-{last_sunday_nodash}-Amazon{region}-{file_version}.csv"
                    destination_file_path = os.path.join(os.path.join(last_sunday_dir, book_type), file_name)
                    blob = bucket.blob(path+file_name)
                    blob_names.append(path+file_name)
                    blob_file_pair.append((blob, destination_file_path))
        download_many_blobs_with_transfer_manager(blob_names=blob_names, blob_file_pairs=blob_file_pair,)




if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"Total time taken: {end_time - start_time}s")

