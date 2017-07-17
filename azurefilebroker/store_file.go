package azurefilebroker

import (
	"encoding/json"
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
	InstanceMap    map[string]ServiceInstance
	BindDetailsMap map[string]brokerapi.BindDetails
	FileShareMap   map[string]FileShare
}

func NewFileStore(
	fileName string,
	ioutil ioutilshim.Ioutil,
) Store {
	return &fileStore{
		fileName: fileName,
		ioutil:   ioutil,
		dynamicState: &DynamicState{
			InstanceMap:    make(map[string]ServiceInstance),
			BindDetailsMap: make(map[string]brokerapi.BindDetails),
			FileShareMap:   make(map[string]FileShare),
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
		return ServiceInstance{}, brokerapi.ErrInstanceDoesNotExist
	}
	return requestedServiceInstance, nil
}

func (s *fileStore) RetrieveBindingDetails(id string) (brokerapi.BindDetails, error) {
	requestedBindingDetails, found := s.dynamicState.BindDetailsMap[id]
	if !found {
		return brokerapi.BindDetails{}, brokerapi.ErrInstanceDoesNotExist
	}
	return requestedBindingDetails, nil
}

func (s *fileStore) RetrieveFileShare(id string) (FileShare, error) {
	requestedFileShare, found := s.dynamicState.FileShareMap[id]
	if !found {
		return FileShare{}, brokerapi.ErrInstanceDoesNotExist
	}
	return requestedFileShare, nil
}

func (s *fileStore) CreateServiceInstance(id string, instance ServiceInstance) error {
	s.dynamicState.InstanceMap[id] = instance
	return nil
}

func (s *fileStore) CreateBindingDetails(id string, details brokerapi.BindDetails) error {
	s.dynamicState.BindDetailsMap[id] = details
	return nil
}

func (s *fileStore) CreateFileShare(id string, share FileShare) error {
	s.dynamicState.FileShareMap[id] = share
	return nil
}

func (s *fileStore) UpdateFileShare(id string, share FileShare) error {
	_, found := s.dynamicState.FileShareMap[id]
	if !found {
		return brokerapi.ErrInstanceDoesNotExist
	}
	s.dynamicState.FileShareMap[id] = share
	return nil
}

func (s *fileStore) DeleteServiceInstance(id string) error {
	_, found := s.dynamicState.InstanceMap[id]
	if !found {
		return brokerapi.ErrInstanceDoesNotExist
	}

	delete(s.dynamicState.InstanceMap, id)
	return nil
}

func (s *fileStore) DeleteBindingDetails(id string) error {
	_, found := s.dynamicState.BindDetailsMap[id]
	if !found {
		return brokerapi.ErrInstanceDoesNotExist
	}

	delete(s.dynamicState.BindDetailsMap, id)
	return nil
}

func (s *fileStore) DeleteFileShare(id string) error {
	_, found := s.dynamicState.FileShareMap[id]
	if !found {
		return brokerapi.ErrInstanceDoesNotExist
	}

	delete(s.dynamicState.FileShareMap, id)
	return nil
}

func (s *fileStore) GetLockForUpdate(_ string, _ int) error {
	return nil
}

func (s *fileStore) ReleaseLockForUpdate(_ string) error {
	return nil
}
