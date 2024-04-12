package view

import (
	"net/http"

	"github.com/gorilla/sessions"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
)

const gorillaSessionName = "gorilla_session"

type Service struct {
	sessionStore sessions.Store
}

func NewSession(sessionStore sessions.Store) *Service {
	return &Service{
		sessionStore: sessionStore,
	}
}

func (s *Service) GetUser(r *http.Request) (*models.User, bool) {
	userAny := r.Context().Value("user")
	user, ok := userAny.(*models.User)
	return user, ok
}

func (s *Service) NewFlash(w http.ResponseWriter, r *http.Request, f Flash) {
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

func (s *Service) GetFlashes(w http.ResponseWriter, r *http.Request) ([]any, error) {
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
