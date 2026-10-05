package httpapi

import (
	"fmt"
	"net/http"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
	"github.com/google/uuid"
)

func (s *Server) openWallet(w http.ResponseWriter, r *http.Request) error {
	var req openWalletRequest
	if err := decodeJSON(w, r, &req); err != nil {
		return err
	}
	if req.PlayerID == uuid.Nil {
		return fmt.Errorf("%w: playerId is required", errInvalidRequest)
	}

	initial, err := req.InitialBalance.toDomain()
	if err != nil {
		return err
	}
	open, err := domain.OpenWallet(req.PlayerID, initial.Currency(), initial)
	if err != nil {
		return err
	}
	if err := s.wallets.OpenWallet(r.Context(), open); err != nil {
		return err
	}

	writeJSON(w, http.StatusCreated, walletView(open.Wallet))

	return nil
}

func (s *Server) getWallet(w http.ResponseWriter, r *http.Request) error {
	id, err := pathUUID(r, "walletId")
	if err != nil {
		return err
	}
	wallet, err := s.wallets.FindByID(r.Context(), id)
	if err != nil {
		return err
	}

	writeJSON(w, http.StatusOK, walletView(wallet))
	return nil
}

// Explicação

// - openWallet monta a abertura no domínio (domain.OpenWallet) e entrega para o repositório. O handler não decide se há OPENING: isso é regra de domínio.
// - A moeda da carteira vem do próprio initialBalance. Não há moeda separada no corpo, então não há como divergir.
// - getWallet não exige provedor. O schema de carteira não tem coluna de provedor, então essa leitura não é isolada por provedor. Isso fica registrado como limitação no ARCHITECTURE.md.
