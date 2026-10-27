# AWS CDK (Python) deployment

This folder is an alternative to the CloudFormation template in [`../cloudformation/`](../cloudformation/). It deploys the same demo infrastructure for the AWS Big Data Blog post on integrating the Apache Spark Troubleshooting Agent for Amazon EMR with AWS DevOps Agent. The CloudFormation path described in the blog post and in the [parent README](../README.md) is unchanged and remains the reference.

Use this app if you prefer the AWS CDK. Everything that happens in the AWS DevOps Agent console (agent space, private connection, capability provider, investigation) is identical, so follow the blog post for those steps. The one difference is the IAM role you give the agent space, which this stack creates for you (see [Manual steps after deploy](#manual-steps-after-deploy)).

## What it deploys

The stack is named `spark-troubleshooting-demo`, the same as in the blog post, and creates the same resources plus one extra IAM role for the agent space:

- A dedicated VPC with two subnets, in Availability Zones discovered at deploy time
- An Interface VPC Endpoint for SageMaker Unified Studio MCP, with private DNS
- The `SparkTroubleshootingRole` IAM role trusted by AWS DevOps Agent, with its MCP, EMR, EMR Serverless, Glue and S3 read policies
- The `analytics-events-platform` EMR Serverless application and its execution role
- A demo S3 bucket
- A CloudWatch alarm named `<application-id>-FailedJobs`
- The `DevOpsAgentRole-AgentSpace-SparkTroubleshooting` IAM role, which you select when you create the agent space (not in the CloudFormation template)

It has the same parameters (`TroubleshootingRoleName`, `VpcCidr`), the same us-east-1 guardrail, and the same stack outputs as the template, such as `TroubleshootingRoleArn`, `MCPEndpointURL`, `DemoVpcId`, `DemoSubnetIds` and `SMUSVpcEndpointSecurityGroupId`, plus two new ones for the agent space role: `AgentSpaceRoleName` and `AgentSpaceRoleArn`.

## How it uses the files in this blog folder

The app consumes the existing [`scripts/`](../scripts/) and [`data/`](../data/) directly; nothing is copied.

| File | Used for |
|------|----------|
| `scripts/az_discovery.py` | Source of the AZ-discovery custom-resource Lambda |
| `scripts/bucket_cleanup.py` | Source of the Lambda that empties the demo bucket on stack delete |
| `scripts/customer_events_aggregator.py` | Uploaded to the root of the demo bucket during `cdk deploy` |
| `data/*.parquet` | Uploaded to `data/` in the demo bucket during `cdk deploy` |

Because the script and data are uploaded by the deployment, **you do not need the `aws s3 cp` commands** from the blog post. Running them anyway is harmless.

## Prerequisites

- Python 3.10 or later
- Node.js and the AWS CDK CLI (`npm install -g aws-cdk`)
- AWS credentials for an account with access to AWS DevOps Agent, configured for **us-east-1**
- The account and Region bootstrapped once with `cdk bootstrap` (the app uploads file assets)

## Deploy

```bash
cd blogs/devops-agent-spark-mcp-integration/cdk

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt

cdk deploy
```

Add `--profile <name>` to use a named AWS profile. The stack can only be deployed to us-east-1; a CloudFormation rule rejects other Regions.

Then continue with the blog post, starting at the step that creates the agent space, but use the role this stack creates for it (see [Manual steps after deploy](#manual-steps-after-deploy)). The values you need are in the stack outputs, which `cdk deploy` prints and which you can read again with:

```bash
aws cloudformation describe-stacks --stack-name spark-troubleshooting-demo --region us-east-1 --query "Stacks[0].Outputs" --output json
```

The `DemoSubmitJobCommand` output contains the complete `aws emr-serverless start-job-run` command for the failing job. To print it, or any single output, on its own line, filter by `OutputKey`:

```bash
aws cloudformation describe-stacks --stack-name spark-troubleshooting-demo --region us-east-1 --query "Stacks[0].Outputs[?OutputKey=='DemoSubmitJobCommand'].OutputValue" --output text
```

## Manual steps after deploy

`cdk deploy` creates the AWS infrastructure only. This app does not create the AWS DevOps Agent resources, so after the deployment finishes you still configure them yourself in the AWS DevOps Agent console, then run the failing job and start the investigation. The [blog post](https://aws.amazon.com/blogs/big-data/accelerate-apache-spark-debugging-on-amazon-emr-with-aws-devops-agent/) gives the field-by-field values for each step. This table only shows which stack output feeds which step:

| Manual step | Stack output to use |
|-------------|---------------------|
| Create the agent space | `AgentSpaceRoleName`, as the existing role for primary account access ([steps below](#create-the-agent-space-with-the-stacks-role)) |
| Create the private connection | `DemoVpcId`, `DemoSubnetIds`, `SMUSVpcEndpointSecurityGroupId` |
| Register the MCP server as a capability provider | `MCPEndpointURL`, plus `TroubleshootingRoleArn` and `MCPServiceName` for its SigV4 authorization |
| Add the MCP server to the agent space | None |
| Submit the failing job | `DemoSubmitJobCommand` |
| Confirm the alarm, then start the investigation | `FailedJobsAlarmName` |

### Create the agent space with the stack's role

The stack creates an IAM role for the agent space, `DevOpsAgentRole-AgentSpace-SparkTroubleshooting` (the `AgentSpaceRoleName` output). Use it instead of letting the console create one. The agent's own AWS calls during an investigation run as the agent space role, and the role the console auto-creates cannot read the demo bucket's logs (`s3:ListBucket`, `s3:GetObject`) or EMR Serverless job runs (`emr-serverless:GetJobRun`), so the agent reports AccessDenied on them. The stack's role has that read-only access.

1. In the AWS DevOps Agent console, choose **Create Agent Space** and fill in the name, description and other details as in the blog post.
2. In **Give this Agent Space AWS resource access**, select **Assign an existing role**, not **Auto-create a new AWS DevOps Agent role**.
3. From the dropdown, choose `DevOpsAgentRole-AgentSpace-SparkTroubleshooting`.
4. In the Web App section, keep **Auto-create a new AWS DevOps Agent role**.
5. Choose **Create**, then continue with the private connection.

If you choose Auto-create for step 2, the investigation still works, because the MCP tool reads the logs with the stack's `SparkTroubleshootingRole`. The agent just can't corroborate its findings from the raw logs itself.

### Things to watch for

- Do these steps only after `cdk deploy` has finished. The private connection needs the VPC and subnets to exist.
- `DemoSubnetIds` is one comma-separated string. In the console, select the two subnets individually.
- In the capability provider's **Authorization Configuration** step (SigV4), enter the `TroubleshootingRoleArn` output as the IAM role and the `MCPServiceName` output (`sagemaker-unified-studio-mcp`) as the service name.
- When you add the MCP server to the agent space, **Select MCP server tools** lists one tool, `analyze_spark_workload`, where the blog post's screenshot shows two (it also lists `analyze_spark_history_server_endpoint`). One tool is expected: the [Amazon EMR documentation](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/spark-troubleshooting-using-troubleshooting-agent.html) lists only `analyze_spark_workload` for this endpoint (`/spark-troubleshooting/mcp`). Select it and choose **Save**. The investigation in the blog post uses only this tool.
- When you clean up, delete the private connection by the name you gave it. The blog post's cleanup step uses a different example name (`smus-spark-private`).
- The investigation prompt in the blog post doesn't include the alarm name. Add the `FailedJobsAlarmName` value so the agent targets that alarm.
- The alarm can take a few minutes to turn red after the job fails, and it stays in ALARM for about 5 minutes after the last failure. Start the investigation while it is still in ALARM.
- Everything is in us-east-1, including the console.

## Clean up

Order matters, exactly as in the blog post. The AWS DevOps Agent private connection creates network interfaces in this stack's subnets. Remove the MCP server from the agent space, delete the agent space (`cdk destroy` also deletes the role it uses), deregister the `spark-troubleshooting` capability provider, delete the private connection, and wait until it is gone from the console. Then:

```bash
cdk destroy
```

The bucket is emptied automatically, including object versions, so no manual `aws s3 rm` is needed.

## Differences from the CloudFormation template

| Area | CloudFormation template | CDK app |
|------|-------------------------|---------|
| Custom-resource Lambda code | Inline copy in `ZipFile`, kept in sync with `scripts/` by hand | `scripts/*.py` packaged directly |
| Lambda runtime | `python3.12` | `python3.14` |
| Script and data upload | Manual `aws s3 cp` | Done by the deployment |
| Lambda log groups | Created implicitly, never expire | Explicit, 1-week retention |
| Bucket policy | None | Denies non-TLS requests |
| Agent space role | Not created. The console auto-creates one | `DevOpsAgentRole-AgentSpace-SparkTroubleshooting` with the console role's trust policy and `AIDevOpsAgentAccessPolicy`, plus read access to the demo bucket and job runs. Outputs `AgentSpaceRoleName` and `AgentSpaceRoleArn` |
| `FailedJobs` alarm evaluation | 1 period of 60 s | 1 breaching period out of the last 5 (60 s each). EMR Serverless publishes metrics a few minutes late, and a single-period alarm can miss the datapoint |
| Logical IDs | Fixed names | Resources built with higher-level constructs carry a CDK hash suffix. Physical names and output keys are unchanged |
| Trust-policy statement IDs | `AllowDevOpsAgentAssumeRole`, `AllowSameAccountAssumeRole` | Same two statements, without `Sid` values |

## Tests and security checks

```bash
pytest
```

The tests assert the synthesized template and also check that:

- the stack has every output and parameter of the CloudFormation template, and only the two agent space role outputs on top,
- the Lambda and deployment assets are byte-identical to the files in `scripts/` and `data/`, and
- [cdk-nag](https://github.com/cdklabs/cdk-nag) (AWS Solutions rules) reports no unsuppressed findings.

`app.py` applies the same cdk-nag checks on every `cdk synth`. Each suppression in `spark_troubleshooting/stack.py` carries its justification, mirroring the ones in the CloudFormation template. The app uses cdk-nag 2.x because 3.x builds on an experimental CDK API.

## Security

See [CONTRIBUTING](https://github.com/aws-samples/sample-aws-data-processing-and-analytics/blob/main/CONTRIBUTING.md) for more information.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](https://github.com/aws-samples/sample-aws-data-processing-and-analytics/blob/main/LICENSE) file.
