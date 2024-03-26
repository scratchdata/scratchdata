package api

import (
	"context"
	"github.com/go-chi/jwtauth/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/gjson"
	"io"
	"net/http"
	"strconv"
	"time"
)

func (a *ScratchDataAPIStruct) AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.URL.Query().Get("api_key")

		hashedKey := a.storageServices.Database.Hash(apiKey)

		// If we have an admin api key, then get the database_id from a query param
		isAdmin := a.storageServices.Database.VerifyAdminAPIKey(r.Context(), hashedKey)
		if isAdmin {
			databaseId := r.URL.Query().Get("destination_id")
			dbInt, err := strconv.ParseInt(databaseId, 10, 64)
			if err != nil {
				dbInt = int64(-1)
			}
			ctx := context.WithValue(r.Context(), "databaseId", dbInt)
			next.ServeHTTP(w, r.WithContext(ctx))
		} else {
			// Otherwise, this API key is specific to a user
			keyDetails, err := a.storageServices.Database.GetAPIKeyDetails(r.Context(), hashedKey)

			if err != nil {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte("Unauthorized"))
				return
			}

			ctx := context.WithValue(r.Context(), "databaseId", keyDetails.DestinationID)
			next.ServeHTTP(w, r.WithContext(ctx))
		}
	})
}

func (a *ScratchDataAPIStruct) AuthGetDatabaseID(ctx context.Context) int64 {
	return ctx.Value("databaseId").(int64)
}

func (a *ScratchDataAPIStruct) Login(w http.ResponseWriter, r *http.Request) {
	url := a.googleOauthConfig.AuthCodeURL(uuid.New().String())
	http.Redirect(w, r, url, http.StatusTemporaryRedirect)
}

func (a *ScratchDataAPIStruct) Authenticator(ja *jwtauth.JWTAuth) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		hfn := func(w http.ResponseWriter, r *http.Request) {
			token, claims, err := jwtauth.FromContext(r.Context())

			if token == nil || err != nil {
				http.Redirect(w, r, "login", http.StatusTemporaryRedirect)
				return
			}

			userId, ok := claims["user_id"]

			if !ok {
				http.Redirect(w, r, "login", http.StatusTemporaryRedirect)
				// w.WriteHeader(http.StatusUnauthorized)
				// w.Write([]byte("Unauthorized"))
				return
			}

			user := a.storageServices.Database.GetUser(int64(userId.(float64)))
			if user.ID <= 0 {
				http.Redirect(w, r, "login", http.StatusTemporaryRedirect)
				// w.WriteHeader(http.StatusUnauthorized)
				// w.Write([]byte("Unauthorized"))
				return
			}

			ctx := context.WithValue(r.Context(), "userId", userId)
			// a.Authenticator(a.TokenAuth)(next).ServeHTTP(w, r.WithContext(ctx))
			// ctx :=
			// next.ServeHTTP(w, r)

			// if err != nil {
			// 	http.Error(w, err.Error(), http.StatusUnauthorized)
			// 	return
			// }

			// if token == nil || jwt.Validate(token, ja.validateOptions...) != nil {
			// 	http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			// 	return
			// }

			// Token is authenticated, pass it through
			next.ServeHTTP(w, r.WithContext(ctx))
		}
		return http.HandlerFunc(hfn)
	}
}

func (a *ScratchDataAPIStruct) Logout(w http.ResponseWriter, r *http.Request) {
	jwtCookie := &http.Cookie{Name: "jwt", Value: "", HttpOnly: true, Path: "/"}
	http.SetCookie(w, jwtCookie)
	http.Redirect(w, r, "login", http.StatusSeeOther)
}

func (a *ScratchDataAPIStruct) OAuthCallback(w http.ResponseWriter, r *http.Request) {
	// state := r.FormValue("state")
	code := r.FormValue("code")
	log.Print(code)

	token, err := a.googleOauthConfig.Exchange(r.Context(), code)
	if err != nil {
		log.Error().Err(err).Send()
		return
	}
	resp, err := a.googleOauthConfig.Client(r.Context(), token).Get("https://www.googleapis.com/oauth2/v3/userinfo")
	if err != nil {
		log.Error().Err(err).Send()
		return
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error().Err(err).Send()
		return
	}

	email := gjson.GetBytes(data, "email").String()
	user, err := a.storageServices.Database.CreateUser(email, "google", string(data))

	if err != nil {
		log.Error().Err(err).Send()
		return
	}

	claims := map[string]any{}
	claims["user_id"] = user.ID
	jwtauth.SetExpiryIn(claims, 7*24*time.Hour)
	_, tokenString, err := a.tokenAuth.Encode(claims)
	if err != nil {
		log.Error().Err(err).Msg("Unable to encode token")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte("Unauthorized"))
		return
	}

	jwtCookie := &http.Cookie{Name: "jwt", Value: tokenString, HttpOnly: true, Path: "/"}
	// jwtCookie := &http.Cookie{Name: "jwt", Value: tokenString, HttpOnly: false}

	http.SetCookie(w, jwtCookie)
	http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
}
