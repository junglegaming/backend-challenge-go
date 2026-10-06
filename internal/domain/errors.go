package domain

import "errors"

var (
	ErrUninitialized     = errors.New("valor de domínio não inicializado")
	ErrInvalidAmount     = errors.New("valor monetário inválido")
	ErrInvalidCurrency   = errors.New("moeda inválida")
	ErrCurrencyMismatch  = errors.New("moedas incompatíveis")
	ErrAmountOverflow    = errors.New("valor monetário excede o limite representável")
	ErrInvalidArgument   = errors.New("argumento inválido")
	ErrInsufficientFunds = errors.New("saldo insuficiente")
	ErrInvalidTransition = errors.New("transição de estado inválida")
)
