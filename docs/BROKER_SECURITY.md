# Broker access policy examples

These examples close the missing-policy artifact gap; they are not deployed IAM
configuration or evidence of AWS authorization enforcement.

## Identity permissions

The policies target **existing queues and identities in the same AWS account**:

- [Producer](../deploy/iam/producer-policy.example.json): send requests to the input
  queue and resolve its URL. Remove `GetQueueUrl` if that producer already has the URL.
- [Runtime](../deploy/iam/runtime-policy.example.json): resolve all three queue URLs;
  receive, acknowledge, extend visibility and check readiness on input; send events
  and dead letters. It cannot provision queues or consume the events/DLQ queues.

Verified in repository code: `internal/adapter/sqs/client.go` resolves all URLs;
`internal/bootstrap/bootstrap.go` probes only the input queue. `consumer.go` uses
receive/delete/visibility on input and sends to DLQ; `publisher.go` sends events.
Therefore the runtime policy needs `GetQueueAttributes` only on input.
AWS documents these actions as queue-ARN-scoped permissions in its
[SQS permission reference](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-api-permissions-reference.html).

To use the examples in an authorized AWS deployment:

1. Replace example account `111122223333`, region `us-east-1`, and queue names in
   **every** ARN. Match the three `SQS_*` queue-name settings; these JSON files do
   not interpolate environment variables.
2. Have a separate provisioning identity create/configure queues. The runtime
   permissions deliberately exclude `CreateQueue` and `SetQueueAttributes`.
3. In IAM, select the intended producer user/role and embed the producer JSON as
   an inline policy; embed runtime JSON in the service role. Alternatively create
   customer-managed policies and attach each to its intended identity. See the
   [official attachment procedure](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_manage-attach-detach.html).
4. Review the identities' other permissions and existing queue policies: these
   additive Allow policies do not revoke privileges granted elsewhere. Cross-account
   producers also require an appropriate queue resource policy and identity-side
   authorization; these examples alone do not grant cross-account access. See
   [IAM policy evaluation](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_identity-vs-resource.html).

These examples contain only SQS permissions. If using SSE-KMS, supply key-scoped
KMS permissions and compatible key policies separately; see
[SQS key management](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-key-management.html).

## Provider binding and supported sender identities

Verified in `internal/adapter/sqs/sender_policy.go`: `SQS_SENDER_PROVIDERS` matches
**exact SenderId strings**, then checks the payload's provider ID. This is application
validation after receipt; IAM controls access to the queue, not that payload binding.
For example, `AIDAEXAMPLEPROVIDERA=provider-a;AIDAEXAMPLEPROVIDERB=provider-b` maps two
illustrative IAM user IDs to separate providers. Replace both IDs with real values.

AWS defines SenderId as the user ID for IAM users and shows role IDs with a session
suffix for assumed roles in [ReceiveMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html).
The current implementation supports stable IAM user IDs. It can match a known full
role-session SenderId, but rotating sessions require a deliberate mapping strategy
that is **not implemented** here. A role ARN, role name, or bare role ID does not
substitute for the exact received string. Sender matching has no prefix or wildcard
support; `*` on the provider side permits every provider for that exact sender.
Deleting/recreating a user changes its unique ID even if its name is reused; see
[IAM identifiers](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_identifiers.html).

## Verification boundary

Verified locally: both example files parse as JSON; permissions were compared with
actual runtime calls and official AWS documentation. Existing sender-policy tests
exercise application rejection, not IAM enforcement. LocalStack integration tests
and the local `000000000000=*` mapping do **not** establish broker IAM isolation.
No policy was attached, no AWS resource was changed, and no live AWS allow/deny
probe was executed. Broker enforcement and rotating role-session support remain
explicitly unverified/unsupported respectively.
