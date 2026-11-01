# Agent evaluation

The `evaluations/` fixtures define a small, reviewable rubric for missing records, duplicate records,
and insufficient evidence. The evaluator scores only the structured operational report; it does not
request or inspect model chain-of-thought.

```bash
uv run python -m agentic_batch_data_reliability.agent.evaluation \
  --case evaluations/missing-records.json \
  --report path/to/deployed-investigation.json
```

Checks cover batch scope, evidence sources, failure identification, facts-versus-hypotheses
separation, approval state, recovery strategy, and uncertainty handling. This lexical rubric is a
regression signal, not proof that a root-cause conclusion is correct. A maintainer must review false
positives and update fixture terms when prompts or tool schemas change.

For an AWS evaluation run, record beside the result:

- model/inference-profile ID and Region;
- wall-clock investigation duration;
- Strands/Bedrock input and output token metrics when the selected model returns them;
- number and names of tool calls;
- estimated Bedrock, Athena, and Glue cost using the account's current pricing;
- tool errors and missing evidence.

The repository does not contain fabricated usage or cost numbers. Those metrics require a deployed
run and vary by model, Region, evidence volume, and current AWS pricing.
