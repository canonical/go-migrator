package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/rancher/kine/pkg/client"
	kineep "github.com/rancher/kine/pkg/endpoint"
	"go.etcd.io/etcd/clientv3"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

var (
	mode          string
	endpoint      string
	etcd_direct   string
	dqlite_direct string
	db            string
)

func main() {
	app := cli.NewApp()
	app.Name = "migrator"
	app.Description = "Tool to migrate etcd to dqlite"
	app.UsageText = "Copy etcd data to kine\n" +
		"migrator --mode [backup-etcd|restore-etcd|backup-dqlite|restore-dqlite] --endpoint [etcd or kine endpoint] --db-dir [dir to store entries]\n" +
		"OR\n" +
		"migrator --mode direct --etcd-direct [etcd endpoint] --dqlite-direct [kine endpoint]"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name: "endpoint",
			// Value:       "http://127.0.0.1:12379",
			Value:       "unix:///var/snap/microk8s/current/var/kubernetes/backend/kine.sock",
			Destination: &endpoint,
		},
		cli.StringFlag{
			Name:        "etcd-direct",
			Value:       "http://127.0.0.1:12379",
			Destination: &etcd_direct,
		},
		cli.StringFlag{
			Name:        "dqlite-direct",
			Value:       "unix:///var/snap/microk8s/current/var/kubernetes/backend/kine.sock",
			Destination: &dqlite_direct,
		},
		cli.StringFlag{
			Name:  "mode",
			Value: "backup",
			//Value:       "restore",
			//Value:       "direct",
			Destination: &mode,
		},
		cli.StringFlag{
			Name:        "db-dir",
			Value:       "db",
			Destination: &db,
		},
		cli.BoolFlag{Name: "debug"},
	}
	app.Action = run

	if err := app.Run(os.Args); err != nil {
		logrus.Fatal(err)
	}
}

func getFileNamesForKey(index int) (string, string) {
	var keyfile = filepath.Join(db, fmt.Sprintf("%d.key", index))
	var datafile = filepath.Join(db, fmt.Sprintf("%d.data", index))
	return keyfile, datafile
}

func backup_etcd(ep string, dir string) error {
	ctx := context.Background()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	if err != nil {
		return fmt.Errorf("failed to create etcd client: %w", err)
	}
	defer cli.Close()

	resp, err := cli.Get(ctx, "", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		return fmt.Errorf("failed to get keys from etcd: %w", err)
	}

	err = os.Mkdir(db, 0700)
	if err != nil {
		return fmt.Errorf("couldn't create backup directory: %w", err)
	}

	for i, ev := range resp.Kvs {
		logrus.Debugf("%d) %s\n", i, ev.Key)
		keyfile, datafile := getFileNamesForKey(i)
		keyf, err := os.Create(keyfile)
		if err != nil {
			return fmt.Errorf("couldn't create keyfile %s: %w", keyfile, err)
		}
		defer keyf.Close()

		dataf, err := os.Create(datafile)
		if err != nil {
			return fmt.Errorf("couldn't create a datafile %s: %w", datafile, err)
		}
		defer dataf.Close()

		if _, err := keyf.Write(ev.Key); err != nil {
			return fmt.Errorf("couldn't write to keyfile %s: %w", keyfile, err)
		}
		if _, err := dataf.Write(ev.Value); err != nil {
			return fmt.Errorf("couldn't write to datafile %s: %w", datafile, err)
		}
	}
	return nil
}

func put_key(c client.Client, ctx context.Context, key string, databytes []byte) error {
	if err := c.Create(ctx, key, databytes); err != nil {
		if err.Error() == "key exists" {
			var attempts = 1
			var err error
			for attempts < 5 {
				if err = c.Put(ctx, key, databytes); err != nil {
					attempts++
					time.Sleep(50 * time.Millisecond)
					continue
				}
				return nil
			}
			return fmt.Errorf("couldn't put key %s: %w", key, err)
		} else {
			return fmt.Errorf("couldn't create key %s: %w", key, err)
		}
	}
	return nil
}

func restore_to_dqlite(ep string, dir string) error {
	ctx := context.Background()
	etcdcfg := kineep.ETCDConfig{
		Endpoints:   []string{ep},
		LeaderElect: false,
	}
	c, err := client.New(etcdcfg)
	if err != nil {
		return fmt.Errorf("failed to create etcd client: %w", err)
	}
	defer c.Close()

	var i = 0
	for {
		keyfile, datafile := getFileNamesForKey(i)
		_, err := os.Stat(keyfile)
		if os.IsNotExist(err) {
			fmt.Printf("Processed %d entries", i)
			break
		}

		keybytes, err := ioutil.ReadFile(keyfile)
		if err != nil {
			return fmt.Errorf("couldn't read keyfile %s: %w", keyfile, err)
		}
		key := string(keybytes)
		databytes, err := ioutil.ReadFile(datafile)
		if err != nil {
			return fmt.Errorf("couldn't read datafile %s: %w", datafile, err)
		}

		logrus.Debugf("%d) %s", i, key)
		if err := put_key(c, ctx, key, databytes); err != nil {
			logrus.Errorf("couldn't put key %s: %q", key, err)
		}
		i++
	}
	return nil
}

func backup_dqlite(ep string, dir string) error {
	ctx := context.Background()
	etcdcfg := kineep.ETCDConfig{
		Endpoints:   []string{ep},
		LeaderElect: false,
	}
	c, err := client.New(etcdcfg)
	if err != nil {
		return fmt.Errorf("couldn't create kine client: %w", err)
	}
	defer c.Close()

	resp, err := c.List(ctx, "/", 0)
	if err != nil {
		return fmt.Errorf("couldn't list key's values: %w", err)
	}
	err = os.Mkdir(db, 0700)
	if err != nil {
		return fmt.Errorf("couldn't create backup directory: %w", err)
	}
	for i, kv := range resp {
		logrus.Debugf("%d) %s\n", i, kv.Key)
		data, err := c.Get(ctx, string(kv.Key))
		if err != nil {
			return fmt.Errorf("couldn't get key %s: %w", kv.Key, err)
		}

		keyfile, datafile := getFileNamesForKey(i)
		keyf, err := os.Create(keyfile)
		if err != nil {
			return fmt.Errorf("couldn't create keyfile %s: %w", keyfile, err)
		}
		defer keyf.Close()

		dataf, err := os.Create(datafile)
		if err != nil {
			return fmt.Errorf("couldn't create datafile %s: %w", datafile, err)
		}
		defer dataf.Close()

		if _, err := keyf.Write(kv.Key); err != nil {
			logrus.Errorf("couldn't write to keyfile %s: %w", keyfile, err)
		}
		if _, err := dataf.Write(data.Data); err != nil {
			logrus.Errorf("couldn't write to datafile %s: %w", datafile, err)
		}
	}
	return nil
}

func restore_to_etcd(ep string, dir string) error {
	ctx := context.Background()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	if err != nil {
		return fmt.Errorf("failed to create etcd client: %w", err)
	}
	defer cli.Close()

	var i = 0
	for {
		keyfile, datafile := getFileNamesForKey(i)
		_, err := os.Stat(keyfile)
		if os.IsNotExist(err) {
			fmt.Printf("Processed %d entries", i)
			break
		}

		keybytes, err := ioutil.ReadFile(keyfile)
		if err != nil {
			return fmt.Errorf("couldn't read keyfile %s: %w", keyfile, err)
		}
		databytes, err := ioutil.ReadFile(datafile)
		if err != nil {
			return fmt.Errorf("couldn't read datafile %s: %w", datafile, err)
		}

		if _, err := cli.Put(ctx, string(keybytes), string(databytes)); err != nil {
			return fmt.Errorf("couldn't put key %s: %w", keybytes, err)
		}
		i++
	}
	return nil
}

func direct(etcd_direct string, dqlite_direct string) error {
	ctx_dqlite := context.Background()
	dqlite_cfg := kineep.ETCDConfig{
		Endpoints:   []string{dqlite_direct},
		LeaderElect: false,
	}
	dqlite, err := client.New(dqlite_cfg)
	if err != nil {
		return fmt.Errorf("failed to create kine client: %w", err)
	}
	defer dqlite.Close()

	ctx_etcd := context.Background()
	etcd, err := clientv3.New(clientv3.Config{
		Endpoints: []string{etcd_direct},
	})
	if err != nil {
		return fmt.Errorf("failed to create etcd client: %w", err)
	}
	defer etcd.Close()
	resp, err := etcd.Get(ctx_etcd, "", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		return fmt.Errorf("couldn't get keys from etcd: %w", err)
	}

	for i, ev := range resp.Kvs {
		logrus.Debugf("%d) %s", i, ev.Key)
		err = dqlite.Create(ctx_dqlite, string(ev.Key), ev.Value)
		if err != nil {
			if err.Error() == "key exists" {
				if err := dqlite.Put(ctx_dqlite, string(ev.Key), ev.Value); err != nil {
					return fmt.Errorf("couldn't put key to dqlite %s: %w", ev.Key, err)
				}
			} else {
				return fmt.Errorf("couldn't create dqlite key %s: %w", ev.Key, err)
			}
		}
		i++
	}
	return nil
}

func run(c *cli.Context) {
	if c.Bool("debug") {
		logrus.SetLevel(logrus.DebugLevel)
	}

	logrus.Infof("mode: %s, endpoint: %s, dir: %s", mode, endpoint, db)
	if mode == "backup" || mode == "backup-etcd" {
		if err := backup_etcd(endpoint, db); err != nil {
			logrus.Fatal(err)
		}
	} else if mode == "restore" || mode == "restore-to-dqlite" || mode == "restore-dqlite" {
		if err := restore_to_dqlite(endpoint, db); err != nil {
			logrus.Fatal(err)
		}
	} else if mode == "backup-dqlite" {
		if err := backup_dqlite(endpoint, db); err != nil {
			logrus.Fatal(err)
		}
	} else if mode == "restore-to-etcd" || mode == "restore-etcd" {
		if err := restore_to_etcd(endpoint, db); err != nil {
			logrus.Fatal(err)
		}
	} else if mode == "direct" {
		if err := direct(etcd_direct, dqlite_direct); err != nil {
			logrus.Fatal(err)
		}
	} else {
		logrus.Fatal("Unknown mode")
		return
	}

}
