package azurefilebroker

import (
	"encoding/json"
	"errors"
	"os"

	"github.com/pivotal-cf/brokerapi"

	"code.cloudfoundry.org/goshims/ioutilshim"
	"code.cloudfoundry.org/lager"
)

type fileStore struct {
	fileName     string
	ioutil       ioutilshim.Ioutil
	dynamicState *DynamicState
}

type DynamicState struct {
	InstanceMap map[string]ServiceInstance
	ShareMap    map[string]brokerapi.BindDetails
}

func NewFileStore(
	fileName string,
	ioutil ioutilshim.Ioutil,
) Store {
	return &fileStore{
		fileName: fileName,
		ioutil:   ioutil,
		dynamicState: &DynamicState{
			InstanceMap: make(map[string]ServiceInstance),
			ShareMap:    make(map[string]brokerapi.BindDetails),
		},
	}
}

func (s *fileStore) Restore(logger lager.Logger) error {
	logger = logger.Session("restore-state")
	logger.Info("start")
	defer logger.Info("end")

	serviceData, err := s.ioutil.ReadFile(s.fileName)
	if err != nil {
		logger.Error("failed-to-read-state-file", err, lager.Data{"fileName": s.fileName})
		return err
	}

	err = json.Unmarshal(serviceData, s.dynamicState)
	if err != nil {
		logger.Error("failed-to-unmarshall-state from state-file", err, lager.Data{"fileName": s.fileName})
		return err
	}
	logger.Info("state-restored", lager.Data{"fileName": s.fileName})

	return err
}

func (s *fileStore) Save(logger lager.Logger) error {
	logger = logger.Session("serialize-state")
	logger.Info("start")
	defer logger.Info("end")

	stateData, err := json.Marshal(s.dynamicState)
	if err != nil {
		logger.Error("failed-to-marshall-state", err)
		return err
	}

	err = s.ioutil.WriteFile(s.fileName, stateData, os.ModePerm)
	if err != nil {
		logger.Error("failed-to-write-state-file", err, lager.Data{"fileName": s.fileName})
		return err
	}

	logger.Info("state-saved", lager.Data{"state-file": s.fileName})
	return nil
}

func (s *fileStore) Cleanup() error {
	return nil
}

func (s *fileStore) RetrieveServiceInstance(id string) (ServiceInstance, error) {
	requestedServiceInstance, found := s.dynamicState.InstanceMap[id]
	if !found {
		return ServiceInstance{}, errors.New(id + " Not Found.")
	}
	return requestedServiceInstance, nil
}

func (s *fileStore) RetrieveBindingDetails(id string) (brokerapi.BindDetails, error) {
	requestedBindingInstance, found := s.dynamicState.ShareMap[id]
	if !found {
		return brokerapi.BindDetails{}, errors.New(id + " Not Found.")
	}
	return requestedBindingInstance, nil
}

func (s *fileStore) CreateServiceInstance(id string, instance ServiceInstance) error {
	s.dynamicState.InstanceMap[id] = instance
	return nil
}

func (s *fileStore) CreateBindingDetails(id string, details brokerapi.BindDetails) error {
	s.dynamicState.ShareMap[id] = details
	return nil
}

func (s *fileStore) UpdateServiceInstance(id string, instance ServiceInstance) error {
	_, found := s.dynamicState.InstanceMap[id]
	if !found {
		return errors.New(id + " Not Found.")
	}
	s.dynamicState.InstanceMap[id] = instance
	return nil
}

func (s *fileStore) DeleteServiceInstance(id string) error {
	_, found := s.dynamicState.InstanceMap[id]
	if !found {
		return errors.New(id + " Not Found.")
	}

	delete(s.dynamicState.InstanceMap, id)
	return nil
}

func (s *fileStore) DeleteBindingDetails(id string) error {
	_, found := s.dynamicState.ShareMap[id]
	if !found {
		return errors.New(id + " Not Found.")
	}

	delete(s.dynamicState.ShareMap, id)
	return nil
}

func (s *fileStore) IsServiceInstanceConflict(id string, instance ServiceInstance) bool {
	return isServiceInstanceConflict(s, id, instance)
}

func (s *fileStore) IsBindingConflict(id string, details brokerapi.BindDetails) bool {
	return IsBindingConflict(s, id, details)
}
