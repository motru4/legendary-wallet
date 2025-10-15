package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"wallet-service/internal/models"
	"wallet-service/internal/repositories/postgresrepo"
	"wallet-service/internal/services"

	_ "wallet-service/docs"

	"github.com/go-playground/validator"
	httpSwagger "github.com/swaggo/http-swagger"
)

type Wallet struct {
	walletService *services.WalletService
	validate      *validator.Validate
}

func NewWallet(mux *http.ServeMux, walletService *services.WalletService) *Wallet {
	h := &Wallet{
		walletService: walletService,
		validate:      validator.New(),
	}

	mux.HandleFunc("POST /api/v1/wallets", h.createWallet)
	mux.HandleFunc("GET /api/v1/wallets/{walletId}", h.getWallet)
	mux.HandleFunc("POST /api/v1/wallet", h.createOperation)
	mux.HandleFunc("GET /api/v1/wallets/{walletId}/operations/{operationId}", h.getOperation)

	mux.Handle("/swagger/", httpSwagger.WrapHandler)

	return h
}

// @Summary Get wallet balance
// @Description Retrieves the current balance of a wallet by its ID
// @Tags wallets
// @Accept json
// @Produce json
// @Param walletId path string true "Wallet ID (UUIDv4)"
// @Success 200 {object} models.WalletBalanceResponse
// @Failure 400 {object} map[string]interface{}
// @Failure 404 {object} map[string]interface{}
// @Failure 500 {object} map[string]interface{}
// @Router /wallets/{walletId} [get]
func (h *Wallet) getWallet(w http.ResponseWriter, r *http.Request) {
	walletID := r.PathValue("walletId")

	// Валидация UUID
	if err := h.validate.Var(walletID, "required,uuid4"); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid wallet ID format")
		return
	}

	ctx := r.Context()
	balanceResponse, err := h.walletService.GetWalletBalance(ctx, walletID)
	if err != nil {
		if errors.Is(err, postgresrepo.ErrWalletNotFound) {
			h.writeError(w, http.StatusNotFound, "Wallet not found")
			return
		}
		h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to get wallet balance: %v", err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(balanceResponse)
}

// @Summary Create a new wallet
// @Description Creates a new wallet with an initial balance of 0
// @Tags wallets
// @Accept json
// @Produce json
// @Success 201 {object} models.WalletCreateResponse
// @Failure 500 {object} map[string]interface{}
// @Router /wallets [post]
func (h *Wallet) createWallet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Создаем кошелек с автоматической генерацией ID
	wallet, err := h.walletService.CreateWallet(ctx)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create wallet: %v", err))
		return
	}

	response := models.WalletCreateResponse{
		WalletID: wallet.WalletID,
		Balance:  wallet.Balance,
		Status:   "created",
		Message:  models.MessageWalletCreated,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)

}

// @Summary Get operation status
// @Description Retrieves the status of a specific operation for a wallet
// @Tags operations
// @Accept json
// @Produce json
// @Param walletId path string true "Wallet ID (UUIDv4)"
// @Param operationId path string true "Operation ID (UUIDv4)"
// @Success 200 {object} models.OperationStatusResponse
// @Failure 400 {object} map[string]interface{}
// @Failure 404 {object} map[string]interface{}
// @Failure 500 {object} map[string]interface{}
// @Router /wallets/{walletId}/operations/{operationId} [get]
func (h *Wallet) getOperation(w http.ResponseWriter, r *http.Request) {
	walletID := r.PathValue("walletId")
	operationID := r.PathValue("operationId")

	// Валидация UUID
	if err := h.validate.Var(walletID, "required,uuid4"); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid wallet ID format")
		return
	}
	if err := h.validate.Var(operationID, "required,uuid4"); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid operation ID format")
		return
	}

	ctx := r.Context()
	operationStatus, err := h.walletService.GetOperation(ctx, walletID, operationID)
	if err != nil {
		if errors.Is(err, postgresrepo.ErrOperationNotFound) {
			h.writeError(w, http.StatusNotFound, "Operation not found")
			return
		}
		if errors.Is(err, postgresrepo.ErrWalletNotFound) {
			h.writeError(w, http.StatusNotFound, "Wallet not found")
			return
		}
		h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to get operation status: %v", err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(operationStatus)
}

// @Summary Create a wallet operation (deposit/withdraw)
// @Description Creates a new deposit or withdraw operation for a wallet
// @Tags operations
// @Accept json
// @Produce json
// @Param operation body models.WalletOperationRequest true "Operation Request"
// @Success 202 {object} models.OperationCreateResponse
// @Failure 400 {object} map[string]interface{}
// @Failure 404 {object} map[string]interface{}
// @Failure 500 {object} map[string]interface{}
// @Router /wallet [post]
func (h *Wallet) createOperation(w http.ResponseWriter, r *http.Request) {
	var req models.WalletOperationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid JSON format")
		return
	}

	// Валидация входных данных
	if err := h.validate.Struct(req); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Sprintf("Validation error: %v", err))
		return
	}

	// Проверка допустимых типов операций
	if req.OperationType != models.OperationTypeDeposit && req.OperationType != models.OperationTypeWithdraw {
		h.writeError(w, http.StatusBadRequest, "OperationType must be DEPOSIT or WITHDRAW")
		return
	}

	// Проверка положительной суммы
	if req.Amount <= 0 {
		h.writeError(w, http.StatusBadRequest, "Amount must be positive")
		return
	}

	ctx := r.Context()
	operationID, err := h.walletService.CreateOperation(ctx, req)
	if err != nil {
		if errors.Is(err, postgresrepo.ErrWalletNotFound) {
			h.writeError(w, http.StatusNotFound, "Wallet not found")
			return
		}
		h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create operation: %v", err))
		return
	}

	response := models.OperationCreateResponse{
		OperationID: operationID,
		Status:      models.OperationStatusAccepted,
		Message:     models.MessageOperationQueued,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(response)
}

// Вспомогательная функция для отправки ошибок
func (h *Wallet) writeError(w http.ResponseWriter, statusCode int, message string) {
	errorResponse := map[string]interface{}{
		"error":   http.StatusText(statusCode),
		"message": message,
		"code":    statusCode,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(errorResponse)
}
