-- Function: eaa_dev.download_file_from_s3(text, text, text, text, text)

-- DROP FUNCTION eaa_dev.download_file_from_s3(text, text, text, text, text);

CREATE OR REPLACE FUNCTION eaa_dev.download_file_from_s3(
    access_key_id text,
    secret_access_key text,
    bucket_name text,
    s3_key text,
    local_file_name text)
  RETURNS integer AS
$BODY$
  import boto
  from boto.s3.key import Key
  conn = boto.connect_s3(access_key_id, secret_access_key)
  bucket = conn.get_bucket(bucket_name)

  # Get the Key object of the given key, in the bucket
  k = Key(bucket, s3_key)

  # Get the contents of the key into a file
  k.get_contents_to_filename(local_file_name)

  # Change permission so that other users can access the file
  import os
  os.chmod(local_file_name, 0666)

  return 0
$BODY$
  LANGUAGE plpython2u VOLATILE
  COST 100;
ALTER FUNCTION eaa_dev.download_file_from_s3(text, text, text, text, text)
  OWNER TO postgres;
