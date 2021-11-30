import Config

config :ex_aws,
  json_codec: Jason,
  access_key_id: [{:system, "AWS_ACCESS_KEY_ID"}, {:awscli, "uberbrodt", 30}],
  secret_access_key: [{:system, "AWS_SECRET_ACCESS_KEY"}, {:awscli, "uberbrodt", 30}]

config :ex_aws, :dynamo_db,
  scheme: "http://",
  host: "localhost",
  port: "4566",
  region: "us-east-1"

config :ex_aws, :kinesis, region: "us-east-1"

# config :ex_aws, :kinesis,
#   scheme: "http://",
#   host: "localhost",
#   port: "4566",
#   region: "us-east-1"

config :logger,
  level: :debug,
  backends: [:console]
