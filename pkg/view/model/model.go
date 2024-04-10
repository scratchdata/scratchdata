package model

import (
	"net/http"

	"github.com/gorilla/csrf"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/view"
	"github.com/scratchdata/scratchdata/pkg/view/session"
)

type ModelLoader struct {
	sessions *session.Service
}

func NewModelLoader(sessions *session.Service) *ModelLoader {
	return &ModelLoader{
		sessions: sessions,
	}
}

func (s *ModelLoader) Load(r *http.Request, w http.ResponseWriter, data ...map[string]any) view.Model {
	// TODO breadchris how should these errors be handled?
	flashes, err := s.sessions.GetFlashes(r)
	if err != nil {
		log.Err(err).Msg("failed to clear flashes")
	}

	var fls []view.Flash
	for _, flash := range flashes {
		f, ok := flash.(view.Flash)
		if !ok {
			continue
		}
		fls = append(fls, f)
	}

	m := view.Model{
		CSRFToken: csrf.TemplateField(r),
		Flashes:   fls,
	}

	user, ok := view.getUser(r)
	if !ok {
		return m
	}
	m.Email = user.Email

	if len(data) > 0 {
		m.Data = data[0]
	}

	return m
}
