package db

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

// KeyProvider is an interface for retrieving encryption keys.
type KeyProvider interface {
	// GetKey returns the 32-byte AES-256 key.
	GetKey() ([]byte, error)
}

// LocalFileKeyProvider stores and retrieves the AES-256 key from a local file.
type LocalFileKeyProvider struct {
	path string // Path to the key file, typically /data/librarr.key
}

// NewLocalFileKeyProvider creates a new key provider. If the key file does not exist,
// it generates a new random 256-bit key and saves it.
func NewLocalFileKeyProvider(path string) (*LocalFileKeyProvider, error) {
	// Ensure the directory exists.
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create key directory: %w", err)
	}

	kp := &LocalFileKeyProvider{path: path}

	// Check if the key file exists.
	if _, err := os.Stat(path); err == nil {
		// File exists; it will be loaded on GetKey().
		return kp, nil
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("stat key file: %w", err)
	}

	// File does not exist — generate a new key.
	key := make([]byte, 32) // 256-bit key
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate random key: %w", err)
	}

	// Write the key file (hex-encoded for readability/editability).
	keyHex := hex.EncodeToString(key)
	if err := os.WriteFile(path, []byte(keyHex), 0600); err != nil {
		return nil, fmt.Errorf("write key file: %w", err)
	}

	slog.Info("generated new encryption key", "path", path)
	return kp, nil
}

// GetKey returns the AES-256 key from the key file.
func (kp *LocalFileKeyProvider) GetKey() ([]byte, error) {
	data, err := os.ReadFile(kp.path)
	if err != nil {
		return nil, fmt.Errorf("read key file: %w", err)
	}

	// Decode hex-encoded key.
	key, err := hex.DecodeString(string(data))
	if err != nil {
		return nil, fmt.Errorf("decode key hex: %w", err)
	}

	if len(key) != 32 {
		return nil, fmt.Errorf("invalid key length: expected 32 bytes, got %d", len(key))
	}

	return key, nil
}

// Encryptor provides AES-256-GCM encryption and decryption.
type Encryptor struct {
	keyProvider KeyProvider
}

// NewEncryptor creates a new encryptor with the given key provider.
func NewEncryptor(keyProvider KeyProvider) *Encryptor {
	return &Encryptor{keyProvider: keyProvider}
}

// Encrypt encrypts plaintext using AES-256-GCM. Returns the nonce (12 bytes) + ciphertext.
func (e *Encryptor) Encrypt(plaintext string) ([]byte, error) {
	key, err := e.keyProvider.GetKey()
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	// Generate a random 12-byte nonce.
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}

	// Encrypt: nonce + ciphertext.
	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return ciphertext, nil
}

// Decrypt decrypts a value encrypted with Encrypt (expecting nonce + ciphertext format).
func (e *Encryptor) Decrypt(ciphertext []byte) (string, error) {
	key, err := e.keyProvider.GetKey()
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create GCM: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return "", fmt.Errorf("ciphertext too short")
	}

	nonce, encrypted := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, encrypted, nil)
	if err != nil {
		return "", fmt.Errorf("decrypt: %w", err)
	}

	return string(plaintext), nil
}
