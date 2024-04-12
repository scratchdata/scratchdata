package view

import (
	"context"
	"net/http"

	"github.com/gorilla/sessions"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
)

const gorillaSessionName = "gorilla_session"

func GetUser(ctx context.Context) (*models.User, bool) {
	userAny := ctx.Value("user")
	user, ok := userAny.(*models.User)
	return user, ok
}

type SessionService struct {
	sessionStore sessions.Store
}

func NewSession(sessionStore sessions.Store) *SessionService {
	return &SessionService{
		sessionStore: sessionStore,
	}
}

func (s *SessionService) NewFlash(w http.ResponseWriter, r *http.Request, f Flash) {
	// TODO breadchris how should these errors be handled?
	session, err := s.sessionStore.Get(r, gorillaSessionName)
	if err != nil {
		log.Err(err).Msg("failed to get session")
		return
	}
	session.AddFlash(f)
	err = session.Save(r, w)
	if err != nil {
		log.Err(err).Msg("failed to save session")
	}
}

func (s *SessionService) GetFlashes(w http.ResponseWriter, r *http.Request) ([]any, error) {
	session, err := s.sessionStore.Get(r, gorillaSessionName)
	if err != nil {
		return nil, err
	}
	flashes := session.Flashes()
	err = session.Save(r, w)
	if err != nil {
		return nil, err
	}
	return flashes, nil
}
