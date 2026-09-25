package benchmarks

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"path/filepath"
	"testing"
	"time"
	"wavicle/internal/auth"
	"wavicle/internal/cluster"
	"wavicle/internal/core"
	"wavicle/internal/protocol/resp3"
	"wavicle/internal/storage"
)

// Parallel hot-read contention: single-mutex vs 32-shard.
func BenchmarkGoat_ShardedVsSingle_Parallel(b *testing.B) {
	single := storage.NewFrontierCache()
	defer single.Close()
	sharded := storage.NewShardedFrontierCacheWithLimit(4 << 30)
	defer sharded.Close()
	for i := 0; i < 1000; i++ {
		p := fmt.Sprintf("k:%d", i)
		single.AppendAtom(core.NewEConst(core.VString("v")), p, nil, time.Time{})
		sharded.AppendAtom(core.NewEConst(core.VString("v")), p, nil, time.Time{})
	}
	b.Run("Single_RWMutex", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				single.GetCurrent(fmt.Sprintf("k:%d", i%1000))
				i++
			}
		})
	})
	b.Run("Sharded_32", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				sharded.GetCurrent(fmt.Sprintf("k:%d", i%1000))
				i++
			}
		})
	})
}

func BenchmarkGoat_ExtendedCmds(b *testing.B) {
	fc := storage.NewShardedFrontierCacheWithLimit(1 << 30)
	defer fc.Close()
	s := resp3.NewServer(fc, "", 1000)
	defer s.Close()
	b.Run("LPUSH", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = s.HandleCommand([]string{"LPUSH", fmt.Sprintf("l:%d", i%128), fmt.Sprint(i)})
		}
	})
	b.Run("ZADD", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = s.HandleCommand([]string{"ZADD", fmt.Sprintf("z:%d", i%128), fmt.Sprint(i), fmt.Sprintf("m%d", i)})
		}
	})
	b.Run("INCR", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = s.HandleCommand([]string{"INCR", "cnt"})
		}
	})
}

func BenchmarkGoat_ACLAuthorize(b *testing.B) {
	a := auth.NewDisabled()
	a.AddUser("app", "secret", []string{"GET", "SET"}, []string{"users:*"}, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = a.Authorize("app", "GET", "users:1:name")
	}
}

func BenchmarkGoat_Snapshot(b *testing.B) {
	fc := storage.NewShardedFrontierCacheWithLimit(1 << 30)
	defer fc.Close()
	for i := 0; i < 5000; i++ {
		fc.AppendAtom(core.NewEConst(core.VString("v")), fmt.Sprintf("k:%d", i), nil, time.Time{})
	}
	path := filepath.Join(b.TempDir(), "snap.jsonl")
	b.Run("Save5k", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = fc.SaveSnapshot(path)
		}
	})
}

func BenchmarkGoat_HashRing_Lookup(b *testing.B) {
	r := cluster.NewHashRing(128)
	r.Add("n1:6379")
	r.Add("n2:6379")
	r.Add("n3:6379")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Get(fmt.Sprintf("users:%d:name", i))
	}
}

// Real TLS handshake over net.Pipe with ephemeral self-signed cert.
func BenchmarkGoat_TLSHandshake(b *testing.B) {
	cert := selfSignedCert(b)
	srvCfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	cliCfg := &tls.Config{InsecureSkipVerify: true}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c1, c2 := net.Pipe()
		done := make(chan struct{})
		go func() {
			_ = tls.Server(c1, srvCfg).Handshake()
			c1.Close()
			close(done)
		}()
		_ = tls.Client(c2, cliCfg).Handshake()
		c2.Close()
		<-done
	}
}

func selfSignedCert(b *testing.B) tls.Certificate {
	b.Helper()
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "wavicle"}, NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour)}
	der, _ := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
