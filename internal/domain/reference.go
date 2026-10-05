package domain

// ReferenceDecision é o resultado da avaliação de uma referência já encontrada.
type ReferenceDecision int

const (
	ReferenceAccepted ReferenceDecision = iota
	ReferenceWait
	ReferenceRejected
)

var allowedReferenceKinds = map[Kind]map[Kind]struct{}{
	KindWin:      {KindBet: {}},
	KindRefund:   {KindBet: {}},
	KindRollback: {KindBet: {}, KindWin: {}, KindRefund: {}},
}

// EvaluateReference aplica as regras de concordância entre a operação e sua referência.
// ALREADY_REVERSED é garantido pelo banco (índice único por referência revertida).
func EvaluateReference(op, ref *WagerTransaction) (ReferenceDecision, FailureCode) {
	switch ref.Status() {
	case StatusPending, StatusPendingReference:
		return ReferenceWait, ""
	case StatusRejected, StatusFailed:
		return ReferenceRejected, FailureReferenceNotProcessed
	}
	if _, ok := allowedReferenceKinds[op.Kind()][ref.Kind()]; !ok {
		return ReferenceRejected, FailureReferenceKindInvalid
	}
	o, r := op.Request(), ref.Request()
	if o.ProviderID != r.ProviderID || op.PlayerID() != ref.PlayerID() || op.WalletID() != ref.WalletID() ||
		op.Money().Currency() != ref.Money().Currency() || o.RoundID != r.RoundID {
		return ReferenceRejected, FailureReferenceMismatch
	}
	if op.Kind() != KindWin && !op.Money().Equal(ref.Money()) {
		return ReferenceRejected, FailureReversalAmountMismatch
	}
	return ReferenceAccepted, ""
}

// IsReversal indica operações que consomem a referência (no máximo uma por referência).
func IsReversal(k Kind) bool { return k == KindRefund || k == KindRollback }

// MovementDirection devolve a direção do lançamento; ok=false para LOSS.
// ROLLBACK inverte a direção do original: BET -> CREDIT, WIN/REFUND -> DEBIT.
func MovementDirection(op Kind, ref Kind) (Direction, bool) {
	switch op {
	case KindBet:
		return Debit, true
	case KindWin, KindRefund, KindOpening:
		return Credit, true
	case KindRollback:
		if ref == KindBet {
			return Credit, true
		}
		return Debit, true
	}
	return "", false
}
