package catalog

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
)

var (
	ErrPictureAlreadyExists = errors.New("picture already exists")
	ErrPictureNotFound      = errors.New("picture not found")
	ErrPathAlreadyExists    = errors.New("path already exists")
)

type Catalog struct {
	mu sync.RWMutex
	db *gorm.DB
}

func New(path string) (*Catalog, error) {
	var err error
	c := &Catalog{}
	defer func() {
		if err == nil {
			return
		}
		c.Close()
	}()

	err = os.MkdirAll(filepath.Dir(path), 0755)
	if err != nil {
		return nil, err
	}

	dbValues := make(url.Values)
	dbValues.Set("_auto_vacuum", "incremental")
	dbValues.Set("_busy_timeout", "60000")
	dbValues.Set("_journal_mode", "WAL")
	dbValues.Set("_locking_mode", "NORMAL")
	dbValues.Set("mode", "rwc")
	dbValues.Set("_mutex", "full")
	dbValues.Set("cache", "shared")
	dbValues.Set("_synchronous", "NORMAL")
	dbValues.Set("_loc", "UTC")
	dbValues.Set("_txlock", "exclusive")
	if idx := strings.Index(path, "?"); idx >= 0 {
		path = path[:idx]
	}
	dbConnectionString := fmt.Sprintf("file:%s?%s",
		path, dbValues.Encode())

	c.db, err = gorm.Open("sqlite3", dbConnectionString)
	if err != nil {
		return nil, err
	}

	c.db.LogMode(false)

	err = c.db.AutoMigrate(&modelPicture{}).Error
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Catalog) Close() {
	if c.db != nil {
		c.db.Close()
	}
}

func (c *Catalog) NewPicture(pic Picture) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	tx := c.db.Begin()
	if err := tx.Error; err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()

	mdl := &modelPicture{Picture: pic}
	if err := tx.Take(mdl).Error; !gorm.IsRecordNotFoundError(err) {
		if err == nil {
			return ErrPictureAlreadyExists
		}
		return err
	}

	pathMdl := &modelPicture{}
	if err := tx.Take(pathMdl, &modelPicture{Picture: Picture{Path: pic.Path}}).Error; !gorm.IsRecordNotFoundError(err) {
		if err == nil {
			return ErrPathAlreadyExists
		}
		return err
	}

	if err := tx.Create(mdl).Error; err != nil {
		return err
	}

	if err := tx.Commit().Error; err != nil {
		return err
	}
	return nil
}

func (c *Catalog) UpdatePicture(pic Picture) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	tx := c.db.Begin()
	if err := tx.Error; err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()

	pictureMdl := &modelPicture{Picture: pic}
	if err := tx.Take(pictureMdl).Error; err != nil {
		if gorm.IsRecordNotFoundError(err) {
			return ErrPictureNotFound
		}
		return err
	}

	pathMdl := &modelPicture{}
	if err := tx.Take(pathMdl, &modelPicture{Picture: Picture{Path: pic.Path}}).Error; !gorm.IsRecordNotFoundError(err) {
		if err == nil && !pictureMdl.IsSame(&pic) {
			return ErrPathAlreadyExists
		}
		return err
	}

	mdl := &modelPicture{Picture: pic}
	if err := tx.Save(mdl).Error; err != nil {
		return err
	}

	if err := tx.Commit().Error; err != nil {
		return err
	}
	return nil
}

func (c *Catalog) GetPicture(path string) (*Picture, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tx := c.db.Begin()
	if err := tx.Error; err != nil {
		return nil, err
	}
	defer tx.RollbackUnlessCommitted()

	mdl := &modelPicture{}
	if err := tx.Take(mdl, &modelPicture{Picture: Picture{Path: path}}).Error; err != nil {
		if gorm.IsRecordNotFoundError(err) {
			return nil, ErrPictureNotFound
		}
		return nil, err
	}
	return &mdl.Picture, nil
}

func (c *Catalog) DeletePicture(path string) (*Picture, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	tx := c.db.Begin()
	if err := tx.Error; err != nil {
		return nil, err
	}
	defer tx.RollbackUnlessCommitted()

	mdl := &modelPicture{}
	if err := tx.Take(mdl, &modelPicture{Picture: Picture{Path: path}}).Error; err != nil {
		if gorm.IsRecordNotFoundError(err) {
			return nil, ErrPictureNotFound
		}
		return nil, err
	}

	if err := tx.Unscoped().Delete(mdl).Error; err != nil {
		return nil, err
	}

	if err := tx.Commit().Error; err != nil {
		return nil, err
	}
	return &mdl.Picture, nil
}
