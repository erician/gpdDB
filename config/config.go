package config

import "github.com/erician/gpdDB/common/gpdtype"

type config struct {
	Appendsync int8
}

var appendSyncEnum = []gpdtype.Enum{
	{Name: "always", Value: OptionAppendSyncAlways},
	{Name: "no", Value: OptionAppendSyncNo},
	{Name: "everysec", Value: OptionAppendSyncEverysec},
}
