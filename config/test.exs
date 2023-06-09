import Config

config :ex_aws,
  access_key_id: [{:system, "AWS_ACCESS_KEY_ID"}, {:awscli, "default", 30}, :instance_role],
  secret_access_key: [{:system, "AWS_SECRET_ACCESS_KEY"}, {:awscli, "default", 30}, :instance_role],
  json_codec: Jason,
  region: "us-east-1"

config :ex_aws, :dynamo_db,
  scheme: "http://",
  host: "localhost",
  port: "4566",
  region: "us-east-1"

config :ex_aws, :kinesis,
  scheme: "http://",
  host: "localhost",
  port: "4566",
  region: "us-east-1"
