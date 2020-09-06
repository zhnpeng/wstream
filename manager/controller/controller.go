package controller

import (
	"context"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"github.com/sirupsen/logrus"
)

// LeaderKeyPrefix, use this key prefix to get controller endpoint
var LeaderKeyPrefix = "/controller/"

type Controller struct {
	endpoint string

	elect  *concurrency.Election
	active chan struct{}
}

func NewControoler() *Controller {
	return &Controller{
		active: make(chan struct{}, 1),
	}
}

func (c *Controller) Run() {
	go c.Compaign()
	<-c.active
}

func (c *Controller) Compaign() {
	for {
		if err := c.elect.Campaign(context.TODO(), c.endpoint); err != nil {
			logrus.Errorf("retry compaign because of err: %v", err)
			continue
		}
		// elect successed
		logrus.Infof("%s is contorller", c.endpoint)
		c.active <- struct{}{}
		return
	}
}

func NewElect(cli *clientv3.Client, pfx string, ttl int) (*concurrency.Election, error) {
	s, err := concurrency.NewSession(cli, concurrency.WithTTL(ttl))
	if err != nil {
		return nil, err
	}
	return concurrency.NewElection(s, pfx), nil
}
