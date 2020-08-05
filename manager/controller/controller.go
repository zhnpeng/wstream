package controller

import (
	"context"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/sirupsen/logrus"
)

// LeaderKeyPrefix, use this key prefix to get controller endpoint
var LeaderKeyPrefix = "/controller/"

type Controller struct {
	etcdClient *clientv3.Client
	endpoint   string
	active     chan struct{}
}

func NewControoler() *Controller {
	return &Controller{
		active: make(chan struct{}, 1),
	}
}

func (c *Controller) Run() {
	go c.compaign(c.etcdClient, LeaderKeyPrefix, c.endpoint)
	<-c.active
}

func (c *Controller) compaign(cli *clientv3.Client, pfx string, val string) {
	for {
		s, err := concurrency.NewSession(cli, concurrency.WithTTL(5))
		if err != nil {
			logrus.Errorf("retry compaign because of err: %v", err)
			continue
		}
		elect := concurrency.NewElection(s, pfx)
		if err = elect.Campaign(context.TODO(), val); err != nil {
			logrus.Errorf("retry compaign because of err: %v", err)
			continue
		}
		// elect successed
		logrus.Infof("%s is contorller", c.endpoint)
		c.active <- struct{}{}
		return
	}
}
