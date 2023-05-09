package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/golang/glog"
)

func pkcs7Unpad(data []byte) ([]byte, error) {
	length := len(data)
	unpadding := int(data[length-1])
	if unpadding > length {
		return nil, fmt.Errorf("file-decrypt: invalid padding")
	}
	return data[:(length - unpadding)], nil
}

// To be removed
func DecryptFile(inputFile, outputFile string, privateKey *rsa.PrivateKey, encryptedKeyB64 string) (err error) {

	encryptedKey, err := base64.StdEncoding.DecodeString(encryptedKeyB64)
	if err != nil {
		glog.Fatalf("Error decoding base64 encoded key: %v", err)
	}

	decryptedKey, err := rsa.DecryptOAEP(sha256.New(), rand.Reader, privateKey, encryptedKey, nil)
	if err != nil {
		glog.Fatalf("Error decrypting key: %v", err)
	}

	encryptedData, err := os.ReadFile(inputFile)
	if err != nil {
		glog.Fatalf("Error reading input file: %v", err)
	}

	iv := encryptedData[:aes.BlockSize]
	ciphertext := encryptedData[aes.BlockSize:]

	block, err := aes.NewCipher(decryptedKey)
	if err != nil {
		glog.Fatalf("Error creating AES cipher: %v", err)
	}
	plaintext := make([]byte, len(ciphertext))
	mode := cipher.NewCBCDecrypter(block, iv)
	mode.CryptBlocks(plaintext, ciphertext)

	data, err := pkcs7Unpad(plaintext)
	if err != nil {
		glog.Fatalf("Error unpadding data: %v", err)
	}

	err = os.WriteFile(outputFile, data, 0644)
	if err != nil {
		glog.Fatalf("Error writing decrypted data to output file: %v", err)
	}

	return err
}
