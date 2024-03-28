package view

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"net/http"
)

func generateCSRFToken() (string, error) {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

const (
	CSRFTokenKey = "csrf_token"
)

func GetCSRFToken(r *http.Request) string {
	token, ok := r.Context().Value(CSRFTokenKey).(string)
	if !ok {
		return ""
	}
	return token
}

// CSRFMiddleware is a middleware that adds CSRF protection.
func CSRFMiddleware(secret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cookie, err := r.Cookie(CSRFTokenKey)
			var token string

			if err != nil || cookie.Value == "" {
				token, err = generateCSRFToken()
				if err != nil {
					http.Error(w, "Could not generate CSRF token", http.StatusInternalServerError)
					return
				}

				http.SetCookie(w, &http.Cookie{
					Name:     CSRFTokenKey,
					Value:    token,
					Path:     "/",
					HttpOnly: true,
				})
			} else {
				token = cookie.Value
			}

			ctx := context.WithValue(r.Context(), CSRFTokenKey, token)
			r = r.WithContext(ctx)

			if r.Method == http.MethodPost {
				postToken := r.FormValue("csrf_token")
				if postToken != token {
					http.Error(w, "Invalid CSRF token", http.StatusBadRequest)
					return
				}
			}

			next.ServeHTTP(w, r)
		})
	}
}
