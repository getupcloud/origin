package server

import (
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/digest"
	"github.com/docker/distribution/registry/storage"
	memorycache "github.com/docker/distribution/registry/storage/cache/memory"
	"github.com/docker/distribution/registry/storage/driver/inmemory"
	kapi "k8s.io/kubernetes/pkg/api"
	kerrors "k8s.io/kubernetes/pkg/api/errors"
	ktestclient "k8s.io/kubernetes/pkg/client/unversioned/testclient"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/util/sets"

	"github.com/openshift/origin/pkg/client"
	"github.com/openshift/origin/pkg/client/testclient"
	imageapi "github.com/openshift/origin/pkg/image/api"
)

func testRegistryClient(imageStreams *imageapi.ImageStreamList, images *imageapi.ImageList) client.Interface {
	fake := &testclient.Fake{}

	nameToStream := make(map[string]*imageapi.ImageStream)
	for i := range imageStreams.Items {
		stream := &imageStreams.Items[i]
		nameToStream[stream.Name] = stream
	}

	fake.AddReactor("get", "imagestreams", func(action ktestclient.Action) (handled bool, ret runtime.Object, err error) {
		getAction, ok := action.(ktestclient.GetAction)
		if ok {
			if stream, exists := nameToStream[getAction.GetName()]; exists {
				return true, stream, nil
			}
		}
		return true, nil, kerrors.NewNotFound("ImageStream", getAction.GetName())
	})

	fake.AddReactor("list", "imagestreams", func(action ktestclient.Action) (handled bool, ret runtime.Object, err error) {
		return true, imageStreams, nil
	})

	fake.AddReactor("list", "images", func(action ktestclient.Action) (handled bool, ret runtime.Object, err error) {
		return true, images, nil
	})

	return fake
}

// checkReceivedDigests compares a list of received digests against a list of expected.
func checkReceivedDigests(t *testing.T, repo string, dgsts []digest.Digest, expected ...string) {
	expDgstSet := sets.NewString()
	for _, dgst := range expected {
		expDgstSet.Insert(dgst)
	}
	rcvDgstSet := sets.NewString()
	for _, dgst := range dgsts {
		if rcvDgstSet.Has(dgst.String()) {
			t.Errorf("received a duplicate digest %q for repository %q", dgst.String(), repo)
			continue
		}
		rcvDgstSet.Insert(dgst.String())
	}
	if len(rcvDgstSet) != len(expDgstSet) {
		t.Errorf("received unexpected number of digests (%d != %d) for repository %q", len(rcvDgstSet), len(expDgstSet), repo)
	}
	unexpected := rcvDgstSet.Difference(expDgstSet)
	for dgst := range unexpected {
		t.Errorf("received unexpected digest %q for repository %q", dgst, repo)
	}
	notReceived := expDgstSet.Difference(rcvDgstSet)
	for dgst := range notReceived {
		t.Errorf("expected digest %q not received for repository %q", dgst, repo)
	}
}

func TestEnumerateImages(t *testing.T) {
	streams := imageapi.ImageStreamList{
		Items: []imageapi.ImageStream{
			{ObjectMeta: kapi.ObjectMeta{Namespace: "ns", Name: "foo"}},
			{ObjectMeta: kapi.ObjectMeta{Namespace: "ns", Name: "bar"}},
			{ObjectMeta: kapi.ObjectMeta{Namespace: "ns", Name: "empty"}},
		},
	}
	images := imageapi.ImageList{
		Items: []imageapi.Image{
			// externally managed
			{
				ObjectMeta:           kapi.ObjectMeta{Name: "bb8bf2124de9cdb13e96087298d75538dddaacb93ccdd1c124c0a8889e670fdb"},
				DockerImageReference: "docker.io/openshift/openshift-base:latest",
			},
			// different namespace
			{
				ObjectMeta: kapi.ObjectMeta{
					Name:        "sha256:2143b74a119435ed7e868bd66cc7501d19fd5d360124453e91cee165b553c900",
					Annotations: map[string]string{imageapi.ManagedByOpenShiftAnnotation: "true"},
				},
				DockerImageReference: "172.30.177.237:5000/default/foo@sha256:2143b74a119435ed7e868bd66cc7501d19fd5d360124453e91cee165b553c900",
			},
			// missing annotation
			{
				ObjectMeta:           kapi.ObjectMeta{Name: "sha256:b0779bbd71414cc720f27a5d4d3f5c26878fc15f3811a0589de07960c2407c3f"},
				DockerImageReference: "172.30.177.237:5000/ns/bar@sha256:b0779bbd71414cc720f27a5d4d3f5c26878fc15f3811a0589de07960c2407c3f",
			},
			// name not a digest
			{
				ObjectMeta: kapi.ObjectMeta{
					Name:        "bdaffa30e8c12b53104b1c47a29c3292d1f5945ebf72c6c2cf53778cea2bbd72",
					Annotations: map[string]string{imageapi.ManagedByOpenShiftAnnotation: "true"},
				},
				DockerImageReference: "172.30.177.237:5000/ns/bar@sha256:bdaffa30e8c12b53104b1c47a29c3292d1f5945ebf72c6c2cf53778cea2bbd72",
			},
			// belongs to ns/foo image stream
			{
				ObjectMeta: kapi.ObjectMeta{
					Name:        "sha256:c3d990247510bcd3e1dbc3093a97bad5cfad753ea7bba9d74457515aa5d62406",
					Annotations: map[string]string{imageapi.ManagedByOpenShiftAnnotation: "true"},
				},
				DockerImageReference: "172.30.177.237:5000/ns/foo@sha256:c3d990247510bcd3e1dbc3093a97bad5cfad753ea7bba9d74457515aa5d62406",
			},
			// belongs to ns/bar image stream
			{
				ObjectMeta: kapi.ObjectMeta{
					Name:        "sha256:e00dfa7d5cd7e026a4ac19e1781451806c6784c0919ff87664eb126a1c03b6e8",
					Annotations: map[string]string{imageapi.ManagedByOpenShiftAnnotation: "true"},
				},
				DockerImageReference: "172.30.177.237:5000/ns/bar@sha256:e00dfa7d5cd7e026a4ac19e1781451806c6784c0919ff87664eb126a1c03b6e8",
			},
			// belongs to ns/bar image stream
			{
				ObjectMeta: kapi.ObjectMeta{
					Name:        "sha256:cb8815d8f7156545b189c32276f8d638c87ba913c126c66d79aac9f744d5a979",
					Annotations: map[string]string{imageapi.ManagedByOpenShiftAnnotation: "true"},
				},
				DockerImageReference: "172.30.177.237:5000/ns/bar@sha256:cb8815d8f7156545b189c32276f8d638c87ba913c126c66d79aac9f744d5a979",
			},
			// belongs to ns/bar image stream
			{
				ObjectMeta: kapi.ObjectMeta{
					Name:        "sha256:114ca2aa4e7deae983e19702015546a6be564f79aaabd1997c65ee8564323039",
					Annotations: map[string]string{imageapi.ManagedByOpenShiftAnnotation: "true"},
				},
				DockerImageReference: "172.30.177.237:5000/ns/bar@sha256:114ca2aa4e7deae983e19702015546a6be564f79aaabd1997c65ee8564323039",
			},
		},
	}

	ctx := kapi.WithNamespace(kapi.NewContext(), "ns")
	driver := inmemory.New()
	reg, err := storage.NewRegistry(ctx, driver, storage.BlobDescriptorCacheProvider(memorycache.NewInMemoryBlobDescriptorCacheProvider()))
	if err != nil {
		t.Fatalf("failed to create distribution.Namespace: %v", err)
	}
	client := testRegistryClient(&streams, &images)
	distRepo, err := reg.Repository(ctx, "foo")
	if err != nil {
		t.Fatalf("failed to create distribution.Repository: %v", err)
	}

	newTestRepository := func(isName string) *repository {
		return &repository{
			Repository:        distRepo,
			ctx:               ctx,
			registryInterface: client,
			namespace:         "ns",
			name:              isName,
		}
	}

	repo := newTestRepository("foo")
	dgsts, err := repo.Enumerate()
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	checkReceivedDigests(t, "ns/foo", dgsts,
		"sha256:c3d990247510bcd3e1dbc3093a97bad5cfad753ea7bba9d74457515aa5d62406",
	)

	repo = newTestRepository("bar")
	dgsts, err = repo.Enumerate()
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	checkReceivedDigests(t, "ns/bar", dgsts,
		"sha256:e00dfa7d5cd7e026a4ac19e1781451806c6784c0919ff87664eb126a1c03b6e8",
		"sha256:cb8815d8f7156545b189c32276f8d638c87ba913c126c66d79aac9f744d5a979",
		"sha256:114ca2aa4e7deae983e19702015546a6be564f79aaabd1997c65ee8564323039",
	)

	repo = newTestRepository("empty")
	dgsts, err = repo.Enumerate()
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	checkReceivedDigests(t, "ns/empty", dgsts)

	repo = newTestRepository("unknown")
	dgsts, err = repo.Enumerate()
	if err == nil {
		t.Fatalf("got unexpected non-error")
	}
	switch err.(type) {
	case distribution.ErrRepositoryUnknown:
		break
	default:
		expErr := distribution.ErrRepositoryUnknown{Name: "ns/unknown"}
		t.Errorf("got unexpected error: %v (%T) != %v (%T)", err, err, expErr, expErr)
	}
	if dgsts != nil {
		t.Errorf("got unexpected digests: %v", dgsts)
	}
}
