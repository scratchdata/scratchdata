package gorm

import "github.com/scratchdata/scratchdata/pkg/storage/database/models"

func (db *Gorm) Enqueue(messageType models.MessageType, message any) (*models.Message, error) {
	return nil, nil
}

func (db *Gorm) Dequeue(messageType models.MessageType, claimedBy string) (*models.Message, bool) {
	return nil, false
}

func (db *Gorm) Delete(id uint) error {
	return nil
}
