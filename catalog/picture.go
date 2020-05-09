package catalog

import (
	"strings"
	"time"
)

type Picture struct {
	SumMD5    string `gorm:"type:char(32);not null;primary_key"`
	SumSHA256 string `gorm:"type:char(64);not null;primary_key"`
	Size      int64  `gorm:"type:bigint;not null;primary_key"`
	Path      string `gorm:"type:varchar(4096);not null;unique_index"`
	TakenAt   *time.Time
}

func (p *Picture) IsSame(p2 *Picture) bool {
	return strings.EqualFold(p.SumMD5, p2.SumMD5) &&
		strings.EqualFold(p.SumSHA256, p2.SumSHA256) &&
		p.Size == p2.Size
}

type modelPicture struct {
	Picture
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (m *modelPicture) TableName() string {
	return "pictures"
}
