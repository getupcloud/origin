package server

import (
	"fmt"
	"net/http"

	ctxu "github.com/docker/distribution/context"
	"github.com/docker/distribution/digest"
	"github.com/docker/distribution/registry/api/errcode"
	"github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/handlers"
	"github.com/docker/distribution/registry/storage"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	gorillahandlers "github.com/gorilla/handlers"
)

// BlobDispatcher takes the request context and builds the appropriate handler
// for handling blob requests.
func BlobDispatcher(ctx *handlers.Context, r *http.Request) http.Handler {
	reference := ctxu.GetStringValue(ctx, "vars.digest")
	dgst, _ := digest.ParseDigest(reference)

	blobHandler := &blobHandler{
		Context: ctx,
		Digest:  dgst,
	}

	return gorillahandlers.MethodHandler{
		"DELETE": http.HandlerFunc(blobHandler.Delete),
	}
}

// blobHandler handles http operations on blobs.
type blobHandler struct {
	*handlers.Context

	Digest digest.Digest
}

// Delete deletes the blob from the storage backend.
func (bh *blobHandler) Delete(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	if len(bh.Digest) == 0 {
		bh.Errors = append(bh.Errors, v2.ErrorCodeBlobUnknown)
		return
	}

	bd, err := storage.RegistryBlobDeleter(bh.Registry())
	if err != nil {
		bh.Errors = append(bh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}
	err = bd.Delete(bh, bh.Digest)
	if err != nil {
		// Ignore PathNotFoundError
		if _, ok := err.(storagedriver.PathNotFoundError); !ok {
			bh.Errors = append(bh.Errors, errcode.ErrorCodeUnknown.WithDetail(fmt.Errorf("error deleting blob %q: %v", bh.Digest, err)))
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// LayerDispatcher takes the request context and builds the appropriate handler
// for handling layer requests.
func LayerDispatcher(ctx *handlers.Context, r *http.Request) http.Handler {
	reference := ctxu.GetStringValue(ctx, "vars.digest")
	dgst, _ := digest.ParseDigest(reference)

	layerHandler := &layerHandler{
		Context: ctx,
		Digest:  dgst,
	}

	return gorillahandlers.MethodHandler{
		"DELETE": http.HandlerFunc(layerHandler.Delete),
	}
}

// layerHandler handles http operations on layers.
type layerHandler struct {
	*handlers.Context

	Digest digest.Digest
}

// Delete deletes the layer link from the repository from the storage backend.
func (lh *layerHandler) Delete(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	if len(lh.Digest) == 0 {
		lh.Errors = append(lh.Errors, v2.ErrorCodeBlobUnknown)
		return
	}

	err := lh.Repository.Blobs(lh).Delete(lh, lh.Digest)
	if err != nil {
		// Ignore PathNotFoundError
		if _, ok := err.(storagedriver.PathNotFoundError); !ok {
			lh.Errors = append(lh.Errors, errcode.ErrorCodeUnknown.WithDetail(fmt.Errorf("error unlinking layer %q from repo %q: %v", lh.Digest, lh.Repository.Name(), err)))
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// ManifestDispatcher takes the request context and builds the appropriate
// handler for handling manifest requests.
func ManifestDispatcher(ctx *handlers.Context, r *http.Request) http.Handler {
	reference := ctxu.GetStringValue(ctx, "vars.digest")
	dgst, _ := digest.ParseDigest(reference)

	manifestHandler := &manifestHandler{
		Context: ctx,
		Digest:  dgst,
	}

	return gorillahandlers.MethodHandler{
		"DELETE": http.HandlerFunc(manifestHandler.Delete),
	}
}

// manifestHandler handles http operations on mainfests.
type manifestHandler struct {
	*handlers.Context

	Digest digest.Digest
}

// Delete deletes the manifest information from the repository from the storage
// backend.
func (mh *manifestHandler) Delete(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	if len(mh.Digest) == 0 {
		mh.Errors = append(mh.Errors, v2.ErrorCodeManifestUnknown)
		return
	}

	manService, err := mh.Repository.Manifests(mh)
	if err != nil {
		mh.Errors = append(mh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
	}
	err = manService.Delete(mh.Digest)
	if err != nil {
		// Ignore PathNotFoundError
		if _, ok := err.(storagedriver.PathNotFoundError); !ok {
			mh.Errors = append(mh.Errors, errcode.ErrorCodeUnknown.WithDetail(fmt.Errorf("error deleting repo %q, manifest %q: %v", mh.Repository.Name(), mh.Digest, err)))
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}
