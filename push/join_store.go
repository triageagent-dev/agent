package push

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// joinStore persists the agent join identity: the key, the certificate and
// the saved core CA, each under its own name. read returns nil for a name
// never written; write with nil data removes it.
type joinStore interface {
	read(name string) ([]byte, error)
	write(name string, data []byte) error
}

// dirJoinStore keeps each item as a file in a directory.
type dirJoinStore struct{ dir string }

func newDirJoinStore(dir string) (*dirJoinStore, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("join state dir: %w", err)
	}
	return &dirJoinStore{dir: dir}, nil
}

func (d *dirJoinStore) read(name string) ([]byte, error) {
	b, err := os.ReadFile(filepath.Join(d.dir, name))
	if os.IsNotExist(err) {
		return nil, nil
	}
	return b, err
}

func (d *dirJoinStore) write(name string, data []byte) error {
	path := filepath.Join(d.dir, name)
	if data == nil {
		err := os.Remove(path)
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

// secretJoinStore keeps each item as a key of one Secret in the agent's own
// namespace, so a new pod keeps the identity and needs no new approval. The
// chart creates the Secret empty and grants get and update on it alone.
type secretJoinStore struct {
	cs        kubernetes.Interface
	namespace string
	name      string
}

const joinSecretTimeout = 10 * time.Second

func (s *secretJoinStore) read(name string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), joinSecretTimeout)
	defer cancel()
	sec, err := s.cs.CoreV1().Secrets(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read join Secret %s/%s: %w", s.namespace, s.name, err)
	}
	return sec.Data[name], nil
}

func (s *secretJoinStore) write(name string, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), joinSecretTimeout)
	defer cancel()
	for range 5 {
		sec, err := s.cs.CoreV1().Secrets(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("read join Secret %s/%s: %w", s.namespace, s.name, err)
		}
		if sec.Data == nil {
			sec.Data = map[string][]byte{}
		}
		if data == nil {
			delete(sec.Data, name)
		} else {
			sec.Data[name] = data
		}
		_, err = s.cs.CoreV1().Secrets(s.namespace).Update(ctx, sec, metav1.UpdateOptions{})
		if apierrors.IsConflict(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("update join Secret %s/%s: %w", s.namespace, s.name, err)
		}
		return nil
	}
	return errors.New("update join Secret: too many conflicts")
}
