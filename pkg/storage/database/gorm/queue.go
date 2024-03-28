package gorm

import (
	"encoding/json"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (db *Gorm) Enqueue(messageType models.MessageType, m any) (*models.Message, error) {
	mStr, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	message := &models.Message{
		MessageType: messageType,
		Status:      models.New,
		Message:     string(mStr),
	}

	res := db.db.Create(message)
	return message, res.Error
}

func (db *Gorm) Dequeue(messageType models.MessageType, claimedBy string) (*models.Message, bool) {
	var message models.Message

	res := db.db.Transaction(func(tx *gorm.DB) error {

		// This locking does not work with SQLite. Should use UPDATE .. WHERE status = new LIMIT 1 RESULT
		findRes := tx.Clauses(clause.Locking{Strength: clause.LockingStrengthUpdate, Options: clause.LockingOptionsSkipLocked}).
			Where("status = ? AND message_type = ?", models.New, messageType).
			First(&message)

		if findRes.Error != nil {
			return findRes.Error
		}

		message.Status = models.Claimed
		message.ClaimedAt = time.Now()
		message.ClaimedBy = claimedBy

		saveRes := tx.Save(&message)
		if saveRes.Error != nil {
			return saveRes.Error
		}

		return nil

	})

	if res != nil {
		log.Error().Err(res).Any("message_type", messageType).Str("claimed_by", claimedBy).Msg("Unable to query for messages")
		return nil, false
	}

	return &message, true

}

func (db *Gorm) Delete(id uint) error {
	res := db.db.Unscoped().Delete(&models.Message{}, id)
	return res.Error
}
