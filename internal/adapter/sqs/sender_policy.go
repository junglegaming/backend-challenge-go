package sqs

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// anyProvider lets a sender act for every provider (the internal service,
// or LocalStack, where every sender is the account id 000000000000).
const anyProvider = "*"

var (
	// ErrUnauthorizedSender reports a message whose broker identity is not
	// allowed to act for the providerId it carries.
	ErrUnauthorizedSender = errors.New("sender is not allowed to act for this provider")
	// ErrInvalidSenderPolicy reports a malformed SQS_SENDER_PROVIDERS value.
	ErrInvalidSenderPolicy = errors.New("invalid sender policy")
)

// SenderPolicy binds broker identities (the SQS SenderId system attribute:
// the IAM user or role id of whoever sent the message) to the providers they
// may act for, so the queue enforces the same provider binding as the
// token-derived providerId on the HTTP path.
type SenderPolicy map[string][]string

// ParseSenderPolicy parses "senderId=provider-a|provider-b;otherId=*".
func ParseSenderPolicy(raw string) (SenderPolicy, error) {
	policy := SenderPolicy{}
	for _, entry := range strings.Split(raw, ";") {
		sender, providers, ok := strings.Cut(strings.TrimSpace(entry), "=")
		list, valid := parseProviders(providers)
		if !ok || sender == "" || !valid {
			return nil, fmt.Errorf("%w: %q (expected senderId=provider|provider)", ErrInvalidSenderPolicy, entry)
		}
		policy[sender] = append(policy[sender], list...)
	}
	return policy, nil
}

// parseProviders splits "a|b", rejecting empty names and names with
// surrounding whitespace (they would never match exactly).
func parseProviders(raw string) ([]string, bool) {
	list := strings.Split(raw, "|")
	invalid := slices.ContainsFunc(list, func(p string) bool { return p == "" || strings.TrimSpace(p) != p })
	return list, !invalid
}

// Authorize checks that sender may submit operations for provider.
func (p SenderPolicy) Authorize(sender, provider string) error {
	allowed := p[sender]
	if slices.Contains(allowed, anyProvider) || slices.Contains(allowed, provider) {
		return nil
	}
	return fmt.Errorf("%w: sender %q, provider %q", ErrUnauthorizedSender, sender, provider)
}
