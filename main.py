
import google.cloud.storage as gcs
import apache_beam as beam
if __name__ == '__main__':
    # bucket_name = "york-project-bucket"
    # folder = "nadhiya/2021-12-17-18-15"
    # file_open = open("final_output.csv", 'a')
    # string_append = "author,score"
    # file_open.write(string_append)
    # file_open.write('\n')
    # storage_client = gcs.Client()
    # bucket = storage_client.get_bucket(bucket_name)
    # blobs = list(bucket.list_blobs(prefix=folder))
    # print(blobs, len(blobs))
    # for blob in blobs:
    #     print(blob.name)
    #     if (not blob.name.endswith("/")):
    #         blob.download_to_filename("Output.csv")
    #         f1 = open("Output.csv", 'r')
    #         file_open.write(f1.read())
    with beam.Pipeline() as pipeline:
        lines = pipeline | beam.io.ReadFromText('gs:york-project-bucket/nadhiya/2021-12-17-18-15//part-*.csv')
        output = lines | beam.io.WriteToText("gs:nm_york_cdf-_start/results/pipelined_output.csv")
