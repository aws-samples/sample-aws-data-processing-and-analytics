# AWS Data Processing and Analytics Samples

A collection of ready-to-deploy examples, blog companion code, and hands-on workshops demonstrating data processing and analytics patterns on AWS.

## What's Inside

This repository is organized into three categories:

| Folder | Description |
|--------|-------------|
| [`blogs/`](blogs/) | Code accompanying AWS blog posts — deploy the exact architecture discussed in each article |
| [`samples/`](samples/) | Standalone, self-contained examples you can launch independently |
| [`workshops/`](workshops/) | Step-by-step guided labs for deeper learning |

## Available Content

### Blogs

| Name | Services | Description |
|------|----------|-------------|
| [Medallion Architecture with DevOps Agent](blogs/medallion-architecture-devops-agent/) | AWS Glue, Amazon MWAA, Amazon Redshift, Amazon S3, EventBridge, Lambda | Bronze → Silver → Gold data pipeline with autonomous incident troubleshooting |

### Samples

_Coming soon_ — standalone examples covering common data processing patterns.

### Workshops

_Coming soon_ — guided, hands-on labs for building analytics solutions.

## AWS Services Covered

This repo provides examples across the AWS data and analytics stack, including:

- **Data Integration & ETL** — AWS Glue, AWS Lambda
- **Data Lakes & Storage** — Amazon S3, AWS Lake Formation
- **Data Warehousing** — Amazon Redshift
- **Streaming & Real-Time** — Amazon Kinesis, Amazon MSK
- **Query & Analytics** — Amazon Athena, Amazon EMR
- **Orchestration** — Amazon MWAA (Apache Airflow), AWS Step Functions
- **Visualization** — Amazon QuickSight
- **Operational AI** — AWS DevOps Agent (autonomous troubleshooting)

> Not every service is covered today — content is actively growing. Contributions are welcome!

## Getting Started

Each example is self-contained with its own README, prerequisites, and deployment instructions. Pick a folder and follow the guide inside.

**General prerequisites:**
- An AWS account
- AWS CLI v2 installed and configured
- Permissions to create the resources described in each example (IAM roles, VPCs, etc.)

## Contributing

We welcome contributions! Whether it's a new sample, a bug fix, or improved documentation — see [CONTRIBUTING](CONTRIBUTING.md) for guidelines.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.

