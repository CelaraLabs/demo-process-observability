# Step Normalization

You are mapping free-form process step descriptions to structured step IDs from a predefined process definition.

## Process Definition

{{PROCESS_DEFINITION_YAML}}

## Input Instance

{{INPUT_JSON}}

## Task

Given the instance's current state description, map it to the structured process definition:

1. **Identify the process**: Based on `candidate_process` and the context, determine which process this belongs to (`recruiting` or `project-management`). Use the `important_terms` from the process definition to help.

2. **Identify the phase**: Determine which phase within that process best matches the current state.

3. **Identify the step**: Find the specific step within that phase that best matches `state.step` and `state.summary`.

4. **Determine status**: Map the instance status to one of: `pending`, `in_progress`, `completed`, `blocked`, `canceled`.

## Output

Return a JSON object with this exact structure:

```json
{
  "process_id": "recruiting | project-management | unknown",
  "phase_id": "<phase id from definition, or 'unknown'>",
  "step_id": "<step id from definition, or null if no specific step matches>",
  "step_status": "pending | in_progress | completed | blocked | canceled",
  "confidence": 0.0-1.0,
  "reasoning": "<brief explanation of the mapping>"
}
```

## Mapping Guidelines

### Process Identification
- `hiring`, `recruiting`, `talent`, `candidate`, `interview`, `JD`, `offer` → `recruiting`
- `project`, `delivery`, `sprint`, `milestone`, `status report` → `project-management`
- If unclear, use `unknown`

### Phase Mapping for Recruiting
- JD, role details, MSA, kickoff → `onboarding`
- Lever, LinkedIn, referral, sourcing → `sourcing`
- Candidate submission, interview, feedback → `candidate-presentation`
- Offer, SOW, background check, onboarding → `Offer and onboarding`
- Weekly report → `weekly-report`

### Phase Mapping for Project Management
- Scope, contract, initiation → `project-initiation`
- Status report, execution, monitoring → `project-execution`

### Status Mapping
- `in_progress` → `in_progress`
- `done`, `completed` → `completed`
- `blocked`, `waiting` → `blocked`
- `paused`, `canceled`, `stopped` → `canceled`
- `unknown` or unclear → `in_progress` (default assumption)

### Step Matching Examples
- "Offer and SOW preparation" → phase: `Offer and onboarding`, step: `offer-letter`
- "Interview scheduled" → phase: `candidate-presentation`, step: `interview-scheduling`
- "Client reviewing candidate profiles" → phase: `candidate-presentation`, step: `client-profile-feedback`
- "Awaiting client feedback after final interview" → phase: `candidate-presentation`, step: `client-interview-feedback`
- "Background check in progress" → phase: `Offer and onboarding`, step: `background-check`
- "Hiring paused" or "search canceled" → step_status: `canceled`, step_id: null
- "Weekly status report" → phase: `project-execution`, step: `status-report`

### Confidence Guidelines
- Exact match to step name: 0.95+
- Clear semantic match: 0.85-0.95
- Reasonable inference: 0.70-0.85
- Uncertain/guessing: 0.50-0.70
- Very uncertain: below 0.50

Return ONLY the JSON object, no additional text.
