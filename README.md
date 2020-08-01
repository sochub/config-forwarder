# A python script to run into a aws lambda and index config logs.

This is a python script to parser and send config records to the lambda.Thiss script uses a sns-topic to read new files related to config logs but you can edit the same to run it into your env. Also has a main function to running this into your computer for specifics objects like:


```sh
$ python3 config_fwd.py -b bucket_NAME -k config-log-data-.json -e https://myELKdomain.aws.com
```

To run this you need:

- CONFIG running into your AWS
- SAVE your config records into a S3 bucket
- A SNS topic for each new s3 object related to the lambda
- A LAMBDA FUNCTION with access to the s3 and elasticsearch domain. 

This script need some envs to work:

| Name | Details |
| ------ | ------ |
| elk_node | the aws elasticsearch domain url |