package analysis

import (
	"testing"

	osgraph "github.com/openshift/origin/pkg/api/graph"
	osgraphtest "github.com/openshift/origin/pkg/api/graph/test"
	buildedges "github.com/openshift/origin/pkg/build/graph"
	imageedges "github.com/openshift/origin/pkg/image/graph"
)

func TestUnpushableBuild(t *testing.T) {
	// Unconfigured internal registry
	g, _, err := osgraphtest.BuildGraph("../../../api/graph/test/unpushable-build.yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buildedges.AddAllInputOutputEdges(g)
	imageedges.AddAllImageStreamRefEdges(g)

	markers := FindUnpushableBuildConfigs(g)
	if e, a := 1, len(markers); e != a {
		t.Fatalf("expected %v, got %v", e, a)
	}

	if got, expected := markers[0].Key, MissingRequiredRegistryErr; got != expected {
		t.Fatalf("expected marker key %q, got %q", expected, got)
	}

	actualBC := osgraph.GetTopLevelContainerNode(g, markers[0].Node)
	expectedBC := g.Find(osgraph.UniqueName("BuildConfig|/ruby-hello-world"))
	if e, a := expectedBC.ID(), actualBC.ID(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	actualIST := markers[0].RelatedNodes[0]
	expectedIST := g.Find(osgraph.UniqueName("ImageStreamTag|/ruby-hello-world:latest"))
	if e, a := expectedIST.ID(), actualIST.ID(); e != a {
		t.Errorf("expected %v, got %v: \n%v", e, a, g)
	}

	// Missing image stream
	g, _, err = osgraphtest.BuildGraph("../../../api/graph/test/unpushable-build-2.yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	buildedges.AddAllInputOutputEdges(g)
	imageedges.AddAllImageStreamRefEdges(g)

	markers = FindUnpushableBuildConfigs(g)
	if e, a := 1, len(markers); e != a {
		t.Fatalf("expected %v, got %v", e, a)
	}

	if got, expected := markers[0].Key, MissingImageStreamErr; got != expected {
		t.Fatalf("expected marker key %q, got %q", expected, got)
	}
}

func TestPushableBuild(t *testing.T) {
	g, _, err := osgraphtest.BuildGraph("../../../api/graph/test/pushable-build.yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buildedges.AddAllInputOutputEdges(g)
	imageedges.AddAllImageStreamRefEdges(g)

	if e, a := 0, len(FindUnpushableBuildConfigs(g)); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
}

func TestBuildConfigNoOutput(t *testing.T) {
	g, _, err := osgraphtest.BuildGraph("../../../api/graph/test/bc-missing-output.yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// we were getting go panics with nil refs cause output destinations are not required for BuildConfigs
	buildedges.AddAllInputOutputEdges(g)
}

func TestCircularDeps(t *testing.T) {
	g, _, err := osgraphtest.BuildGraph("../../../api/graph/test/circular.yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	buildedges.AddAllInputOutputEdges(g)

	if len(FindCircularBuilds(g)) != 1 {
		t.Fatalf("expected having circular dependencies")
	}

	not, _, err := osgraphtest.BuildGraph("../../../api/graph/test/circular-not.yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	buildedges.AddAllInputOutputEdges(not)

	if len(FindCircularBuilds(not)) != 0 {
		t.Fatalf("expected not having circular dependencies")
	}

}
