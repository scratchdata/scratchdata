package api

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/jwtauth/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
	"github.com/tidwall/gjson"
)

func UserFromContext(c context.Context) (*models.User, bool) {
	userAny := c.Value("user")
	user, ok := userAny.(*models.User)
	return user, ok
}

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
			ctx = context.WithValue(ctx, "teamId", keyDetails.Destination.TeamID)
			next.ServeHTTP(w, r.WithContext(ctx))
		}
	})
}

func (a *ScratchDataAPIStruct) AuthGetDatabaseID(ctx context.Context) int64 {
	dbId := ctx.Value("databaseId").(uint)
	return int64(dbId)
}

func (a *ScratchDataAPIStruct) AuthGetTeamID(ctx context.Context) uint {
	dbId := ctx.Value("teamId").(uint)
	return dbId
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
				http.Redirect(w, r, "/login", http.StatusTemporaryRedirect)
				return
			}

			userId, ok := claims["user_id"]
			if !ok {
				log.Error().Msg("User ID not found in claims")
				http.Redirect(w, r, "/login", http.StatusTemporaryRedirect)
				// w.WriteHeader(http.StatusUnauthorized)
				// w.Write([]byte("Unauthorized"))
				return
			}

			user := a.storageServices.Database.GetUser(uint(userId.(float64)))
			if user.ID <= 0 {
				log.Error().Msg("User not found")
				http.Redirect(w, r, "/login", http.StatusTemporaryRedirect)
				// w.WriteHeader(http.StatusUnauthorized)
				// w.Write([]byte("Unauthorized"))
				return
			}

			ctx := context.WithValue(r.Context(), "user", user)
			// a.Authenticator(a.tokenAuth)(next).ServeHTTP(w, r.WithContext(ctx))
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
	for _, cookie := range r.Cookies() {
		cookie.MaxAge = 0
		cookie.Value = ""
		http.SetCookie(w, cookie)
	}
	http.Redirect(w, r, "login", http.StatusSeeOther)
}

func (a *ScratchDataAPIStruct) OAuthCallback(w http.ResponseWriter, r *http.Request) {
	// state := r.FormValue("state")
	code := r.FormValue("code")
	log.Print(code)

	token, err := a.googleOauthConfig.Exchange(r.Context(), code)
	if err != nil {
		log.Error().Err(err).Msg("Unable to exchange code for token")
		return
	}
	resp, err := a.googleOauthConfig.Client(r.Context(), token).Get("https://www.googleapis.com/oauth2/v3/userinfo")
	if err != nil {
		log.Error().Err(err).Msg("Unable to get user info")
		return
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error().Err(err).Msg("Unable to read response body")
		return
	}

	email := gjson.GetBytes(data, "email").String()
	user, err := a.storageServices.Database.CreateUser(email, "google", string(data))

	if err != nil {
		log.Error().Err(err).Msg("Unable to create user")
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
