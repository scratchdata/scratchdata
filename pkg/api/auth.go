package api

import (
	"context"
	"net/http"
	"strconv"
)

func (a *ScratchDataAPIStruct) AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.URL.Query().Get("api_key")

		hashedKey := a.storageServices.Database.Hash(apiKey)

		// If we have an admin api key, then get the database_id from a query param
		isAdmin := a.storageServices.Database.VerifyAdminAPIKey(hashedKey)
		if isAdmin {
			databaseId := r.URL.Query().Get("database_id")
			dbInt, err := strconv.ParseInt(databaseId, 10, 64)
			if err != nil {
				dbInt = int64(-1)
			}
			ctx := context.WithValue(r.Context(), "databaseId", dbInt)
			next.ServeHTTP(w, r.WithContext(ctx))
		} else {
			// Otherwise, this API key is specific to a user
			keyDetails, err := a.storageServices.Database.GetAPIKeyDetails(hashedKey)

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
