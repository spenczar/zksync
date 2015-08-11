package zksync

import (
	"testing"

	"github.com/samuel/go-zookeeper/zk"
)

func TestDeleteIfExistsDoesDelete(t *testing.T) {
	defer cleanup(t)

	conn := setupZk(t)
	path := testPath("TestDeleteIfExists/exists")

	if err := createParentPath(path, conn, zk.WorldACL(zk.PermAll)); err != nil {
		t.Fatalf("failed creating parent path err=%q", err)
	}

	if err := deleteIfExists(path, conn); err != nil {
		t.Errorf("err deleting extant path err=%q", err)
	}

	exists, _, err := conn.Exists(path)
	if err != nil {
		t.Fatalf("failed checking existence err=%q", err)
	}
	if exists {
		t.Errorf("did not actually delete")
	}
}

func TestDeleteIfExistsDoesntErrorIfNoNode(t *testing.T) {
	defer cleanup(t)

	conn := setupZk(t)
	path := testPath("TestDeleteIfExists/doesntexist")

	if err := deleteIfExists(path, conn); err != nil {
		t.Errorf("err deleting extant path err=%q", err)
	}

	exists, _, err := conn.Exists(path)
	if err != nil {
		t.Fatalf("failed checking existence err=%q", err)
	}
	if exists {
		t.Errorf("path now exists?!")
	}
}

func TestCreateParentPath(t *testing.T) {
	defer cleanup(t)

	conn := setupZk(t)

	type testcase struct {
		path   string
		parent string
	}

	testcases := []testcase{
		{testPath("child"), testPath("")},
		{testPath("parent/child"), testPath("parent")},
		{testPath("grandparent/parent/child"), testPath("grandparent/parent")},
	}
	for _, tc := range testcases {
		err := createParentPath(tc.path, conn, zk.WorldACL(zk.PermAll))
		if err != nil {
			t.Errorf("create err=%q", err)
		}
		exists, _, err := conn.Exists(tc.parent)
		if err != nil {
			t.Errorf("exist check err=%q", err)
		}
		if !exists {
			t.Errorf("parent does not exist - path=%s  parent=%s", tc.path, tc.parent)
		}
	}
}
