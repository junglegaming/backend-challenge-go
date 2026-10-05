package domain

// FailureCode é um resultado de negócio definitivo, persistido com a transação.
// Entradas corrigíveis (formato, campos obrigatórios) nunca recebem FailureCode: são 400 sem persistência.
type FailureCode string

const (
	FailureInsufficientFunds         FailureCode = "INSUFFICIENT_FUNDS"
	FailureReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	FailureCurrencyMismatch          FailureCode = "CURRENCY_MISMATCH"
	FailurePlayerWalletMismatch      FailureCode = "PLAYER_WALLET_MISMATCH"
	FailureBalanceLimitExceeded      FailureCode = "BALANCE_LIMIT_EXCEEDED"
	FailureReferenceNotFound         FailureCode = "REFERENCE_NOT_FOUND"
	FailureReferenceNotProcessed     FailureCode = "REFERENCE_NOT_PROCESSED"
	FailureReferenceMismatch         FailureCode = "REFERENCE_MISMATCH"
	FailureReferenceKindInvalid      FailureCode = "REFERENCE_KIND_INVALID"
	FailureReversalAmountMismatch    FailureCode = "REVERSAL_AMOUNT_MISMATCH"
	FailureAlreadyReversed           FailureCode = "ALREADY_REVERSED"
	FailureInternal                  FailureCode = "INTERNAL_FAILURE"
)
