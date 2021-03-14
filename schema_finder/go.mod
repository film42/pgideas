module github.com/film42/pgideas/schema_finder

go 1.15

require (
	github.com/cockroachdb/cockroach v20.2.5+incompatible
	github.com/kr/pretty v0.2.1 // indirect
)

replace github.com/cockroachdb/cockroach => github.com/cockroachdb/cockroach-gen v20.2.5+incompatible

replace github.com/gogo/protobuf => github.com/cockroachdb/gogoproto v1.2.1-0.20210111172841-8b6737fea948

replace github.com/grpc-ecosystem/grpc-gateway => github.com/cockroachdb/grpc-gateway v1.14.6-0.20200519165156-52697fc4a249

replace github.com/olekukonko/tablewriter => github.com/cockroachdb/tablewriter v0.0.5-0.20200105123400-bd15540e8847

replace github.com/abourget/teamcity => github.com/cockroachdb/teamcity v0.0.0-20180905144921-8ca25c33eb11

replace vitess.io/vitess => github.com/cockroachdb/vitess v2.2.0-rc.1.0.20180830030426-1740ce8b3188+incompatible

replace gopkg.in/yaml.v2 => github.com/cockroachdb/yaml v0.0.0-20180705215940-0e2822948641

replace go.etcd.io/etcd => github.com/cockroachdb/etcd v0.4.7-0.20200615211340-a17df30d5955
