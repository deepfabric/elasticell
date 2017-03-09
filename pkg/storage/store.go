package storage

// Store is used for data persistent. It's contains many cells.
type Store struct {
	storeID int64
}

// GetCurrentClusterID get current cluster ID
// If is the first, return 0, nil
func (s *Store) GetCurrentClusterID() (int64, error) {
	return 0, nil
}

// GetStoreID get current storeID
func (s *Store) GetStoreID() int64 {
	return s.storeID
}

// DeleteLocalCell delete local cell
func (s *Store) DeleteLocalCell(id int64) error {
	return nil
}

// Start start the store
func (s *Store) Start() error {
	return nil
}

// Stop stop the store
func (s *Store) Stop() error {
	return nil
}
