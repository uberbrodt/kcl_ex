Mox.defmock(KinesisClient.KinesisMock, for: KinesisClient.Kinesis.Adapter)
Mox.defmock(KinesisClient.Stream.AppStateMock, for: KinesisClient.Stream.AppState.Adapter)

Application.put_env(:ex_aws, :dynamodb,
  scheme: "http://",
  host: "localhost",
  port: "4569",
  region: "us-east-1"
)

Application.put_env(:ex_aws, :kinesis,
  scheme: "http://",
  host: "localhost",
  port: "4568",
  region: "us-east-1"
)

Logger.configure(level: :debug)

ExUnit.start()
